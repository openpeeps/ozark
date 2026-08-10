# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

## This module provides the main public API for Ozark with Postgres as the database driver.
## 
## It includes functions for initializing database connections and connection pools, as well
## as macros for executing SQL queries and mapping results to model instances. The module also
## defines the necessary types and exceptions for managing database interactions.

import std/[macros, macrocache, net, strutils, tables, locks, os]

import pkg/[openparser/sql, threading/once]
import pkg/db_connector/postgres {.all.}
import pkg/db_connector/db_postgres {.all.}
import pkg/db_connector/db_common {.all.}

import ./private/types
import ./dbmeta, ./psql_runtime
import ../model, ../query, ../collection

export SqlDriver, Port, strVal, `%`
export db_postgres, model, query, psql_runtime, collection

type
  DBConnectionPool* = ref object
    connections*: seq[DBConn]      # available
    busyConnections*: seq[DBConn]  # checked-out
    maxSize*: int
    lock: Lock

  DBCredentials* = ref object
    driver*: SqlDriver
    address*, name*, user*, password*: string
    port*: Port

  DBConnections* = OrderedTableRef[string, DBCredentials]
  
  Ozark = object
    dbs: DBConnections
    maindb: DBCredentials
    mainPool: DBConnectionPool

  OzarkConnectionError* = object of CatchableError

var
  DB: ptr Ozark
  o = createOnce()

const StaticDbTypes = CacheTable"StaticDbTypes"

proc getInstance*(): ptr Ozark =
  ## Get the singleton instance of the database manager
  once(o):
    DB = createShared(Ozark)
    DB[].dbs = newOrderedTable[string, DBCredentials]() # init map
  result = DB

proc initOzarkDatabase*(address, name, user, password: string,
                port: Port = Port(5432),
                driver: static SqlDriver = SqlDriver.pgsql) =
  ## Initialize the main database connection credentials
  ## 
  ## This should be called before using any database functionality.
  ## For more efficient connection management, consider using `initOzarkPool` after
  ## this to initialize a connection pool
  let db = getInstance()
  db[].maindb = DBCredentials(
    address: address,
    user: user,
    name: name,
    password: password,
    port: port,
    driver: driver
  )

proc `[]=`*(db: Ozark, id: string, dbCon: DBCredentials) =
  ## Add new database connection credentials
  db[id] = dbCon

proc add*(db: Ozark, id: string, dbCon: DBCredentials) {.inline.} =
  ## Add new database connection credentials
  db[id] = dbCon

macro withDB*(body: untyped) =
  ## Use the current database context to run database queries.
  ## 
  ## This macro will open a connection to the database,
  ## execute the body, and then close the connection.
  ## 
  ## For more efficient connection management, consider using `withDBPool` instead.
  result = newStmtList()
  add result, quote do:
    block:
      const sqlDriver {.inject.} = 1
      # This block ensures that multiple withDB calls can be nested without
      # interfering with each other's connections.
      let db = getInstance()
      assert db != nil, "Database manager not initialized. Call initDBManager first."
      let dbcon {.inject.} =
          case db[].maindb.driver
          of SqlDriver.pgsql:
            open(db[].maindb.address, db[].maindb.user,
                    db[].maindb.password, db[].maindb.name)
          of SqlDriver.sqlite:
            open(db[].maindb.address, "", "", "")
          else:
            raise newException(OzarkConnectionError,
              "Only PostgreSQL and SQLite drivers are currently implemented.")
      defer:
        dbcon.close()
      block:
        `body`

macro withDatabase*(id: static string, body: untyped) =
  ## Use the specified database context to run database queries.
  ## 
  ## This macro will open a connection to the database,
  ## execute the body, and then close the connection.
  ## 
  ## For more efficient connection management, consider using `withDBPool` instead.
  result = newStmtList()
  add result, quote do:
    block:
      # ensure multiple withDB calls can be nested without interfering with
      # each other's connections.
      let db = getInstance()
      assert db != nil, "Database manager not initialized. Call initDBManager first."
      assert db.hasKey(id), "Database connection with id `" & id & "` not found."
      let dbcon {.inject.} =
          open(db[id].address, db[$id].user,
                  db[id].password, db[$id].name)
      defer:
        dbcon.close()
      block:
        `body`

proc openConn(cfg: DBCredentials): DBConn =
  # Currently only supports PostgreSQL, but can be extended to
  # support other databases
  case cfg.driver
  of pgsql:
    open(cfg.address, cfg.user, cfg.password, cfg.name)
  else:
    raise newException(OzarkConnectionError,
      "Only PostgreSQL driver pool is currently implemented.")

proc initOzarkPool*(size: Positive = 10) =
  ## Initialize main DB connection pool.
  let db = getInstance()
  assert db[].maindb != nil,
      "Main DB credentials not initialized. Call `initOzarkDatabase` first."

  var pool = DBConnectionPool(
    maxSize: size.int,
    connections: @[],
    busyConnections: @[]
  )
  initLock(pool.lock)

  for _ in 0..<size.int:
    pool.connections.add(openConn(db[].maindb))
  db[].mainPool = pool

proc initOzarkDbWithPool*(address, name, user, password: string,
                port: Port = Port(5432),
                driver: static SqlDriver = SqlDriver.pgsql,
                poolSize: Positive = 10) =
  ## Initialize main database credentials and connection pool in
  ## one step for convenience.
  ## 
  ## This is equivalent to calling `initOzarkDatabase` followed by `initOzarkPool`
  initOzarkDatabase(address, name, user, password, port, driver)
  initOzarkPool(poolSize)

proc closeOzarkPool*() =
  ## Close all pooled connections.
  let db = getInstance()
  if db[].mainPool.isNil: return

  acquire(db[].mainPool.lock)
  defer: release(db[].mainPool.lock)

  for c in db[].mainPool.connections:
    c.close()
  for c in db[].mainPool.busyConnections:
    c.close()

  db[].mainPool.connections.setLen(0)
  db[].mainPool.busyConnections.setLen(0)

proc acquireConn*(pool: DBConnectionPool, timeoutMs: int = 5000): DBConn =
  ## Borrow one connection from pool, waiting up to timeoutMs.
  let stepMs = 10
  var waited = 0
  while waited <= timeoutMs:
    acquire(pool.lock)
    if pool.connections.len > 0:
      result = pool.connections.pop()
      pool.busyConnections.add(result)
      release(pool.lock)
      return

    sleep(stepMs)
    inc(waited, stepMs)

  raise newException(OzarkConnectionError,
    "Timed out waiting for a DB connection from pool.")

proc releaseConn*(pool: DBConnectionPool, conn: DBConn) =
  ## Return a connection to pool.
  acquire(pool.lock)
  defer: release(pool.lock)

  var idx = -1
  for i, c in pool.busyConnections:
    if c == conn:
      idx = i
      break

  if idx >= 0:
    pool.busyConnections.del(idx)
    pool.connections.add(conn)

macro withDBPool*(body: untyped) =
  ## Run queries using a pooled connection.
  result = newStmtList()
  body.add(
    newConstStmt(
      ident"dbdriver_internal",
      newLit"main"
    )
  )
  add result, quote do:
    const sqlDriver {.inject.} = 1
    block:
      let db = getInstance()
      assert db != nil, "Database manager not initialized. Call initOzarkDatabase first."
      assert db[].mainPool != nil,
        "DB pool not initialized. Call initOzarkPool first."
      let dbcon {.inject.} = acquireConn(db[].mainPool)
      defer:
        releaseConn(db[].mainPool, dbcon)
      `body`

#
# SQL Query Validator
#
proc parseSqlQuery(sql: NimNode, getRowProcName: string,
            args: NimNode = newEmptyNode()): NimNode {.compileTime.} =
  # Compile-time procedure to validate the SQL query and 
  # generate the appropriate runtime code to execute it and
  # map the results to model instances. This procedure is called by the `get` and `getAll` macros.
  try:
    let sqlDriver = SqlDriver(sql[1][^2][1][1][1].intVal)
    let parsedSql = parseSQL(sql[1][^1][1].strVal, sqlDriver = SqlDriver.pgsql)
    let modelSym = sql[1][^2][1][1][0]
    var colNames: seq[string]
    let
      top = parsedSql.sons[0]
      selectNode = top
      selectList = selectNode.sons[0]
    for c in selectList.sons:
      if c.kind == nkIdent and c.strVal == "*":
        colNames.add("*")
      else:
        if c.len > 0: colNames.add(c[0].strVal)
        else: colNames.add(c.strVal)

    # generate code to assign columns to model instance fields
    var idx = 0
    var assigns = newStmtList()
    for cn in colNames:
      if cn != "*":
        # assigns.add("inst." & cn & " = row[" & $idx & "]")
        assigns.add(
          nnkAsgn.newTree(
            nnkDotExpr.newTree(ident("inst"), ident(cn)),
            nnkBracketExpr.newTree(ident("row"), newLit(idx))
          )
        )
      else:
        # assign all columns to fields with matching names
        let modelFields = getTypeImpl(modelSym)[0].getTypeImpl[1]
        for field in getImpl(modelSym)[2][0][2]:
          # assigns.add("inst." & $(field[0][1]) & " = row[" & $idx & "]")
          assigns.add(
            nnkAsgn.newTree(
              nnkDotExpr.newTree(ident("inst"), field[0][1]),
              nnkBracketExpr.newTree(ident("row"), newLit(idx))
            )
          )
      inc idx
    
    # validate the number of SQL arguments and generate
    # the appropriate runtime code to execute the query and
    # map results to model instances.
    let nParams = countSqlArgs(args)
    if getRowProcName == "getRow":
      result = newCall(
        nnkBracketExpr.newTree(
          bindSym"getRowToModel",
          ident($(modelSym.getImpl[0][1]))
        ),
        ident"dbcon",
        newCall(bindSym"SqlQuery", newLit($parsedSql)),
        newLit(nParams),
      )
      appendSqlArgs(result, args)
      result.add(
        nnkLambda.newTree(
          newEmptyNode(),
          newEmptyNode(),
          newEmptyNode(),
          nnkFormalParams.newTree(
            newEmptyNode(),
            nnkIdentDefs.newTree(
              ident("inst"),
              ident($(modelSym.getImpl[0][1])),
              newEmptyNode()
            ),
            nnkIdentDefs.newTree(
              ident("row"),
              nnkBracketExpr.newTree(
                ident("seq"),
                ident("string")
              ),
              newEmptyNode()
            )
          ),
          newEmptyNode(),
          newEmptyNode(),
          assigns
        )
      )
    else:
      result = newCall(
        nnkBracketExpr.newTree(
          bindSym"instantRowsToModels",
          ident($(modelSym.getImpl[0][1]))
        ),
        ident"dbcon",
        newCall(bindSym"SqlQuery", newLit($parsedSql)),
        newLit(colNames),
        newLit(nParams)
      )
      appendSqlArgs(result, args)
  except SqlParseError as e:
    error("SQL parsing error: " & e.msg, sql[1][^1][1])

macro getAll*(sql: untyped): untyped =
  ## Finalize and get all results of the SQL statement.
  ## This macro produce the final SQL string and wraps it in a runtime call
  ## to execute it and return all rows via `instantRows`
  if sql.kind != nnkBlockExpr or sql[1][^1][0].strVal notin [
        "ozarkWhereResult", "ozarkRawSQLResult",
        "ozarkLimitResult", "ozarkOrderByResult",
        "ozarkSelectResult"
    ]:
    error("The argument to `getAll` must be the result of a `where` macro.", sql)
  if sql[1][^1][0].strVal == "ozarkSelectResult":
    result = sql.parseSqlQuery("instantRows")
  else:
    result = sql.parseSqlQuery("instantRows", sql[1][^1][2][1])

macro get*(sql: untyped): untyped =
  ## Finalize SQL statement. This macro produces the final SQL
  ## string and emits runtime code that maps selected columns into a new instance of `m`
  var runtimeCode: NimNode
  let calledMacro = sql[1][^1][0].strVal
  if sql.kind != nnkBlockExpr or
          calledMacro notin ["ozarkWhereResult", "ozarkRawSQLResult", "ozarkWhereInResult", "ozarkLimitResult"]:
    error("The argument to `get` must be the result of a `where` macro. Got " & calledMacro, sql)
  if calledMacro == "ozarkWhereInResult":
    result = sql.parseSqlQuery("getRow", sql[1][^1][2][1])
  else:
    result = sql.parseSqlQuery("getRow", sql[1][^1][2][1])

proc validateSqlNodes(nodes: seq[SqlNode], colNames: var seq[string]) {.compileTime.} =
  for sqlNode in nodes:
    case sqlNode.kind
    of nkSelect:
      let selectColumns = sqlNode.sons[0]
      let fromClause = sqlNode.sons[1]
      let tableName =
        if fromClause.sons.len > 0 and fromClause.sons[0].sons.len > 0:
          getTableName(fromClause.sons[0].sons[0].strVal)
        else:
          ""

      for colPair in selectColumns.sons:
        let col = colPair.sons[0]
        let hasAlias = colPair.sons.len > 1 and colPair.sons[1].kind == nkIdent
        let aliasName = if hasAlias: colPair.sons[1].strVal else: ""

        case col.kind
        of nkIdent:
          if col.strVal == "*" and tableName.len > 0:
            let typeDef = StaticSchemas[tableName][0][0]
            for field in typeDef[2][0][2]:
              colNames.add($(field[0][1]))
          elif tableName.len > 0:
            let typeDef = StaticSchemas[tableName][0][0]
            withColumn(typeDef, col.strVal):
              colNames.add(col.strVal)
          elif hasAlias:
            colNames.add(aliasName)

        of nkDot:
          let tbl = col.sons[0].strVal
          let field = col.sons[1].strVal
          if field == "*":
            let typeDef = StaticSchemas[getTableName(tbl)][0][0]
            for f in typeDef[2][0][2]:
              colNames.add($(f[0][1]))
          else:
            let typeDef = StaticSchemas[getTableName(tbl)][0][0]
            withColumn(typeDef, field):
              colNames.add(field)

        of nkPrGroup, nkSelect:
          # Scalar subquery in SELECT list: output column is the alias.
          # Still recurse for validation, but do not use inner selected names as output columns.
          var dummyCols: seq[string]
          validateSqlNodes(@[col], dummyCols)
          if hasAlias:
            colNames.add(aliasName)

        else:
          # Computed expression / function call with alias.
          if hasAlias:
            colNames.add(aliasName)

      for fromItem in fromClause.sons:
        for sub in fromItem.sons:
          if sub.kind in {nkSelect, nkPrGroup}:
            var dummyCols: seq[string]
            validateSqlNodes(@[sub], dummyCols)

    else:
      for child in sqlNode.sons:
        if child.kind in {nkSelect, nkPrGroup}:
          var dummyCols: seq[string]
          validateSqlNodes(@[child], dummyCols)

macro getWith*(sql: untyped, toModelIdent: untyped): untyped =
  ## Finalize a RAW SQL statement. This macro produces the final SQL
  ## string and emits runtime code that maps selected columns into
  ## a new instance of the specified model type.
  ## 
  ## This is used in conjunction with the `rawSQL` macro. For getting the
  ## raw results when using `rawSQL`, use the `getRaw` macro instead.
  var runtimeCode: NimNode
  let calledMacro = sql[1][1][0].strVal
  # if calledMacro != "ozarkRawSQLResult":
  #   error("The first argument to `getWith` must be the result of a `rawSQL` macro. Got " & calledMacro, sql)

  try:
    let parsedSql = parseSQL(sql[1][1][1].strVal, sqlDriver = SqlDriver.pgsql)
    var colNames: seq[string]
    validateSqlNodes(parsedSql.sons, colNames)

    # generate the runtime code that fetches the row and applies
    # the generated assignments
    let args = sql[1][1][^1][1][1]
    let randId = genSym(nskVar, "id")
    let runtimeCode =
      staticRead("private" / "stubs" / "iteratorInstantRows.nim") % [
        $parsedSql, 
        toModelIdent.strVal,
        colNames.mapIt("\"" & it & "\"").join(","),
        "instantRows",
        (if args.len > 0: "," & args.mapIt(it.repr).join(",") else: ""),
        (if args.len > 0: $args.len else: "0"),
        randId.repr
      ]
    result = macros.parseStmt(runtimeCode)
  except SqlParseError as e:
    error("SQL parsing error: " & e.msg, sql)

macro getRaw*(sql: untyped): untyped =
  ## Finalize a RAW SQL statement. This macro produces the final SQL
  ## string and emits runtime code that returns the raw results as a sequence of sequences of strings.
  discard # TODO

macro exists*(tableName: untyped) =
  ## Search in the current table for a record matching
  ## the specified values. This is a placeholder for an `EXISTS` query.
  withTableCheck tableName:
    result = newCall(
      bindSym"ozarkRawSQLResult",
      newLit("SELECT EXISTS(SELECT 1 FROM " & getTableName($tableName[1]) & " WHERE $1)"),
    )

macro limit*(sql: untyped, count: untyped): untyped =
  ## Placeholder for a `LIMIT` clause in SQL queries.
  if sql.kind != nnkBlockExpr or sql[1][1][0].strVal notin [
      "ozarkWhereResult", "ozarkRawSQLResult", "ozarkOrderByResult", "ozarkSelectResult"]:
    error("The argument to `limit` must be the result of a `select`, `where`", sql)
  let len = sql[1][^1][2][1].len + 1
  # sql[1][^1][1].strVal = sql[1][^1][1].strVal & " AND " & col & " " & op & " $" & $(len)
  sql[1][^1][^1][1].add(count) # add to the current varargs list
  sql[1][1] = newCall(
    bindSym"ozarkLimitResult",
    newLit(sql[1][1][1].strVal & " LIMIT $" & $(len))
  )
  result = sql

type
  Order* = enum
    Desc, Asc

macro orderDescBy*(sql: untyped, cols: static openArray[string]): untyped =
  ## Placeholder for an `ORDER BY` clause in SQL queries.
  if sql[1][1].kind != nnkCall or sql[1][1][0].strVal notin ["ozarkWhereResult", "ozarkSelectResult"]:
    error("The argument to `orderDescBy` must be the result of a `where` macro.")
  withColumnsCheck(sql[1][0][1], cols):
    let blockIdent = genSym(nskLabel, "ozarkBlockOrderBy")
    var newCallNode = newCall(bindSym"ozarkOrderByResult")
    let totalParam =
      if sql[1][^1].len > 2 and sql[1][^1][2].kind == nnkHiddenStdConv:
        # if there are already parameters (e.g. from a WHERE IN clause), we need to calculate
        # the new parameter index based on the existing parameters
        sql[1][^1][2][1].len # the number of params inside a nnkBracket node
      else:
        0
    
    var idx: seq[int]
    var argsNode =
      if totalParam == 0:
        newEmptyNode()
      else:
        sql[1][^1][2]
    
    newCallNode.insert(1, newLit(sql[1][1][1].strVal &
      " ORDER BY " & cols.join(", ") & " DESC"))

    if argsNode.kind != nnkEmpty:
      newCallNode.add(argsNode)

    sql[1][1] = newCallNode
    result = sql

macro rawSQL*(models: ptr ModelsTable, sql: static string, values: varargs[untyped]): untyped =
  ## Allows raw SQL queries without losing safety of
  ## model checks (table name/column names) and SQL validation at compile time
  try:
    let sqlNode = parseSQL(sql, sqlDriver = SqlDriver.pgsql)
    case sqlNode.sons[0].kind
    of nkSelect:
      # checking the select statement for if the specified
      # table name exists in the models and if the specified column names are valid
      let fromNode = sqlNode.sons[0].sons[1]
      assert fromNode.kind == nkFrom
      for table in fromNode.sons:
        if not StaticSchemas.hasKey(getTableName(table[0].strVal)):
          raise newException(OzarkModelDefect, "Unknown model `" & $table[0].strVal & "`")
    else: discard
    let blockIdent = genSym(nskLabel, "ozarkBlockRawSQL")

    result = nnkBlockStmt.newTree(
      blockIdent,
      newStmtList(
        newCall(bindSym"ozarkHoldModel", nil),
        newCall(
          bindSym"ozarkRawSQLResult",
          newLit(sql),
          nnkPrefix.newTree(ident"@",
            if values.len > 0: values
            else: nnkBracket.newTree()
          )
        )
      )
    )
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

macro exec*(sql: untyped) =
  ## Finalize and execute an SQL statement that doesn't
  ## return results (e.g. INSERT, UPDATE, DELETE).
  var sql = sql
  if sql.kind != nnkCall or
      sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult",
                      "ozarkInsertResult", "ozarkCreateTableResult",
                      "ozarkRemoveResult"]:
    if sql.kind != nnkBlockExpr:
      error("The argument to `exec` must be the result of a `where`, `rawSQL`, or `insert`/`update` macro. Got " & $sql[1][^1][0], sql)
    else:
      sql = sql[1][^1] # if it's a block expression, we need to extract the last statement which should be the SQL result
  try:
    let sqlNode = parseSQL($sql[1], sqlDriver = SqlDriver.pgsql)
    case sqlNode.sons[0].kind
    of nkInsert, nkDelete:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      result = macros.parseStmt(stub % [
              $sql[1],
              (
                if sql[2][1].len > 0: 
                  ", " & 
                  $sql[2][1].mapIt("toDbValue(" & it.repr & ")").join(",")
                else: ""
              ),
              randId.repr,
              $(sql[2][1]).len
        ])
    of nkUpdate:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      result = macros.parseStmt(stub % [
              $sql[1],
              (
                if sql[2][1][1].len > 0: 
                  ", " & 
                  $sql[2][1][1].mapIt("toDbValue(" & it.repr & ")").join(",")
                else: ""
              ),
              randId.repr,
              $(sql[2][1][1]).len # bracket len
        ])
    of nkCreateTable, nkCreateTableIfNotExists:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      result = macros.parseStmt(stub % [
              $sql[1],
              "",
              randId.repr,
              "0"
        ])
    of nkDropTable, nkDropTableIfExists, nkDropIfExists:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      result = macros.parseStmt(stub % [
              $sql[1],
              "",
              randId.repr,
              "0"
        ])
    else: discard 
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

proc tryInsertID*(db: DbConn, query: SqlPrepared,
                  args: varargs[string, `$`]): int64 {.
                  tags: [WriteDbEffect].}=
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row or -1 in case of an error. For Postgre this adds
  ## `RETURNING id` to the query, so it only works if your primary key is
  ## named `id`.
  let res = setupQuery(db, query, args)
  var x = pqgetvalue(res, 0, 0)
  if not isNil(x):
    result = parseBiggestInt($x)
  else:
    result = -1
  pqclear(res)

macro execGet*(sql: untyped): untyped =
  ## Finalize and execute an SQL statement that returns
  ## results (e.g. SELECT, INSERT with RETURNING).
  ## 
  ## This macro produces the final SQL string and
  ## wraps it in a runtime call
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkInsertResult"]:
    error("The argument to `execGet` must be the result of an `insert` or `delete` macro.")
  try:
    let sqlNode = parseSQL($sql[1], sqlDriver = SqlDriver.pgsql)
    case sqlNode.sons[0].kind
    of nkInsert:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "tryInsertID.nim")
      result = macros.parseStmt(stub % [
              $sql[1],
              $sql[2][1].mapIt(it.repr).join(","),
              randId.repr,
              $(sql[2][1]).len
        ])
    of nkDelete:
      discard # todo
    else: discard 
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)
