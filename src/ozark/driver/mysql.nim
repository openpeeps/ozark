# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#

## This module provides the main public API for Ozark with MySQL as the database driver.

import std/[macros, macrocache, net, strutils, tables, locks, os]

import pkg/[openparser/sql, threading/once]
import pkg/db_connector/mysql {.all.}
import pkg/db_connector/db_mysql {.all.}
import pkg/db_connector/db_common {.all.}

import ./private/types
import ./dbmeta, ./mysql_runtime
import ../model, ../query, ../collection

export SqlDriver, Port, strVal, `%`
export db_mysql, model, query, mysql_runtime, collection

type
  DBConnectionPool* = ref object
    connections*: seq[DBConn]
    busyConnections*: seq[DBConn]
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
  once(o):
    DB = createShared(Ozark)
    DB[].dbs = newOrderedTable[string, DBCredentials]()
  result = DB

proc initOzarkDatabase*(address, name, user, password: string,
                port: Port = Port(3306),
                driver: static SqlDriver = SqlDriver.mysql) =
  let db = getInstance()
  db[].maindb = DBCredentials(
    address: address,
    user: user,
    name: name,
    password: password,
    port: port,
    driver: driver
  )

proc hasKey*(db: Ozark, id: string): bool =
  result = db.dbs.hasKey(id)

proc `[]`*(db: Ozark, id: string): DBCredentials =
  result = db.dbs[id]

proc `[]=`*(db: Ozark, id: string, dbCon: DBCredentials) =
  db.dbs[id] = dbCon

proc add*(db: Ozark, id: string, dbCon: DBCredentials) {.inline.} =
  db.dbs[id] = dbCon

macro withDB*(body: untyped) =
  result = newStmtList()
  add result, quote do:
    block:
      const sqlDriver {.inject.} = 2
      let db = getInstance()
      assert db != nil, "Database manager not initialized. Call initOzarkDatabase first."
      let dbcon {.inject.} =
          case db[].maindb.driver
          of SqlDriver.mysql:
            open((if db[].maindb.port.int > 0 and db[].maindb.port.int != 3306:
                     db[].maindb.address & ":" & $db[].maindb.port.int
                   else: db[].maindb.address),
                  db[].maindb.user, db[].maindb.password, db[].maindb.name)
          of SqlDriver.pgsql:
            open(db[].maindb.address, db[].maindb.user,
                    db[].maindb.password, db[].maindb.name)
          of SqlDriver.sqlite:
            open(db[].maindb.address, "", "", "")
          else:
            raise newException(OzarkConnectionError,
              "Driver not implemented.")
      defer:
        dbcon.close()
      block:
        `body`

macro withDatabase*(id: static string, body: untyped) =
  result = newStmtList()
  add result, quote do:
    block:
      const sqlDriver {.inject.} = 2
      let db = getInstance()
      assert db != nil, "Database manager not initialized. Call initDBManager first."
      assert db.hasKey(id), "Database connection with id `" & id & "` not found."
      let dbcon {.inject.} =
          open(db[id].address, db[id].user,
                  db[id].password, db[id].name)
      defer:
        dbcon.close()
      block:
        `body`

proc openConn(cfg: DBCredentials): DBConn =
  case cfg.driver
  of SqlDriver.mysql:
    let host = if cfg.port.int > 0: cfg.address & ":" & $cfg.port.int else: cfg.address
    open(host, cfg.user, cfg.password, cfg.name)
  of SqlDriver.pgsql:
    open(cfg.address, cfg.user, cfg.password, cfg.name)
  else:
    raise newException(OzarkConnectionError, "Only MySQL/PostgreSQL pool implemented.")

proc initOzarkPool*(size: Positive = 10) =
  let db = getInstance()
  assert db[].maindb != nil, "Main DB credentials not initialized. Call initOzarkDatabase first."
  var pool = DBConnectionPool(maxSize: size.int, connections: @[], busyConnections: @[])
  initLock(pool.lock)
  for _ in 0..<size.int:
    pool.connections.add(openConn(db[].maindb))
  db[].mainPool = pool

proc initOzarkDbWithPool*(address, name, user, password: string,
                port: Port = Port(3306),
                driver: static SqlDriver = SqlDriver.mysql,
                poolSize: Positive = 10) =
  initOzarkDatabase(address, name, user, password, port, driver)
  initOzarkPool(poolSize)

proc closeOzarkPool*() =
  let db = getInstance()
  if db[].mainPool.isNil: return
  acquire(db[].mainPool.lock)
  defer: release(db[].mainPool.lock)
  for c in db[].mainPool.connections: c.close()
  for c in db[].mainPool.busyConnections: c.close()
  db[].mainPool.connections.setLen(0)
  db[].mainPool.busyConnections.setLen(0)

proc acquireConn*(pool: DBConnectionPool, timeoutMs: int = 5000): DBConn =
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
  raise newException(OzarkConnectionError, "Timed out waiting for a DB connection from pool.")

proc releaseConn*(pool: DBConnectionPool, conn: DBConn) =
  acquire(pool.lock)
  defer: release(pool.lock)
  var idx = -1
  for i, c in pool.busyConnections:
    if cast[pointer](c) == cast[pointer](conn):
      idx = i
      break
  if idx >= 0:
    pool.busyConnections.del(idx)
    pool.connections.add(conn)

macro withDBPool*(body: untyped) =
  result = newStmtList()
  body.add(newConstStmt(ident"dbdriver_internal", newLit"main"))
  add result, quote do:
    block:
      const sqlDriver {.inject.} = 2
      let db = getInstance()
      assert db != nil, "Database manager not initialized. Call initOzarkDatabase first."
      assert db[].mainPool != nil, "DB pool not initialized. Call initOzarkPool first."
      let dbcon {.inject.} = acquireConn(db[].mainPool)
      defer:
        releaseConn(db[].mainPool, dbcon)
      `body`

# SQL Query Validator helpers
proc parseSqlQuery(sql: NimNode, getRowProcName: string, args: NimNode = newEmptyNode()): NimNode {.compileTime.} =
  try:
    let sqlDriver = SqlDriver(sql[1][^2][1][1][1].intVal)
    let parsedSql = parseSQL(normalizeInLists(sql[1][^1][1].strVal, sqlDriver), sqlDriver = sqlDriver)
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
    var idx = 0
    var assigns = newStmtList()
    for cn in colNames:
      if cn != "*":
        assigns.add(nnkAsgn.newTree(nnkDotExpr.newTree(ident("inst"), ident(cn)), nnkBracketExpr.newTree(ident("row"), newLit(idx))))
      else:
        let modelFields = getTypeImpl(modelSym)[0].getTypeImpl[1]
        for field in getImpl(modelSym)[2][0][2]:
          assigns.add(nnkAsgn.newTree(nnkDotExpr.newTree(ident("inst"), field[0][1]), nnkBracketExpr.newTree(ident("row"), newLit(idx))))
      inc idx
    let nParams = countSqlArgs(args)
    if getRowProcName == "getRow":
      result = newCall(nnkBracketExpr.newTree(bindSym"getFirstToModel", ident($(modelSym.getImpl[0][1]))), ident"dbcon", newCall(bindSym"SqlQuery", newLit(sql[1][^1][1].strVal)), newLit(colNames), newLit(nParams))
      appendSqlArgs(result, args)
    else:
      result = newCall(nnkBracketExpr.newTree(bindSym"instantRowsToModels", ident($(modelSym.getImpl[0][1]))), ident"dbcon", newCall(bindSym"SqlQuery", newLit(sql[1][^1][1].strVal)), newLit(colNames), newLit(nParams))
      appendSqlArgs(result, args)
  except SqlParseError as e:
    error("SQL parsing error: " & e.msg, sql[1][^1][1])

macro getAll*(sql: untyped): untyped =
  if sql.kind != nnkBlockExpr or resultKindOf(trailingCallOf(sql)) notin selectFamilyResults:
    error("The argument to `getAll` must be the result of a `where` macro.", sql)
  if sql[1][^1][0].strVal == "ozarkSelectResult":
    result = sql.parseSqlQuery("instantRows")
  else:
    result = sql.parseSqlQuery("instantRows", paramsBracketOf(trailingCallOf(sql)))

macro get*(sql: untyped): untyped =
  var runtimeCode: NimNode
  let calledMacro = sql[1][^1][0].strVal
  if sql.kind != nnkBlockExpr or calledMacro notin ["ozarkWhereResult", "ozarkRawSQLResult", "ozarkWhereInResult", "ozarkLimitResult", "ozarkOffsetResult", "ozarkGroupByResult", "ozarkHavingResult"]:
    error("The argument to `get` must be the result of a `where` macro. Got " & calledMacro, sql)
  if calledMacro == "ozarkWhereInResult":
    result = sql.parseSqlQuery("getRow", paramsBracketOf(trailingCallOf(sql)))
  else:
    result = sql.parseSqlQuery("getRow", paramsBracketOf(trailingCallOf(sql)))

# validateSqlNodes (same as psql) 
proc validateSqlNodes(nodes: seq[SqlNode], colNames: var seq[string]) {.compileTime.} =
  for sqlNode in nodes:
    case sqlNode.kind
    of nkSelect:
      let selectColumns = sqlNode.sons[0]
      let fromClause = sqlNode.sons[1]
      let tableName = if fromClause.sons.len > 0 and fromClause.sons[0].sons.len > 0: getTableName(fromClause.sons[0].sons[0].strVal) else: ""
      for colPair in selectColumns.sons:
        let col = colPair.sons[0]
        let hasAlias = colPair.sons.len > 1 and colPair.sons[1].kind == nkIdent
        let aliasName = if hasAlias: colPair.sons[1].strVal else: ""
        case col.kind
        of nkIdent:
          if col.strVal == "*" and tableName.len > 0:
            let typeDef = StaticSchemas[tableName][0][0]
            for field in typeDef[2][0][2]: colNames.add($(field[0][1]))
          elif tableName.len > 0:
            let typeDef = StaticSchemas[tableName][0][0]
            withColumn(typeDef, col.strVal): colNames.add(col.strVal)
          elif hasAlias: colNames.add(aliasName)
        of nkDot:
          let tbl = col.sons[0].strVal
          let field = col.sons[1].strVal
          if field == "*":
            let typeDef = StaticSchemas[getTableName(tbl)][0][0]
            for f in typeDef[2][0][2]: colNames.add($(f[0][1]))
          else:
            let typeDef = StaticSchemas[getTableName(tbl)][0][0]
            withColumn(typeDef, field): colNames.add(field)
        of nkPrGroup, nkSelect:
          var dummyCols: seq[string]
          validateSqlNodes(@[col], dummyCols)
          if hasAlias: colNames.add(aliasName)
        else:
          if hasAlias: colNames.add(aliasName)
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
  var runtimeCode: NimNode
  let calledMacro = sql[1][1][0].strVal
  try:
    let parsedSql = parseSQL(sql[1][1][1].strVal, sqlDriver = SqlDriver.mysql)
    var colNames: seq[string]
    validateSqlNodes(parsedSql.sons, colNames)
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
  ensureStage(sql, selectFamilyResults, "getRaw")
  let trailing = trailingCallOf(sql)
  var bracket = paramsBracketOf(trailing)
  var call = newCall(bindSym"execRows", ident"dbcon", newCall(bindSym"SqlQuery", newLit(trailing[1].strVal)), newLit(bracket.len))
  for v in bracket:
    call.add(v)
  result = call

macro rawSQL*(models: ptr ModelsTable, sql: static string, values: varargs[untyped]): untyped =
  try:
    let sqlNode = parseSQL(sql, sqlDriver = SqlDriver.mysql)
    case sqlNode.sons[0].kind
    of nkSelect:
      let fromNode = sqlNode.sons[0].sons[1]
      assert fromNode.kind == nkFrom
      for table in fromNode.sons:
        if not StaticSchemas.hasKey(getTableName(table[0].strVal)):
          raise newException(OzarkModelDefect, "Unknown model `" & $table[0].strVal & "`")
    else: discard
    let blockIdent = genSym(nskLabel, "ozarkBlockRawSQL")
    result = nnkBlockStmt.newTree(blockIdent, newStmtList(
      newCall(bindSym"ozarkHoldModel", nil),
      newCall(bindSym"ozarkRawSQLResult", newLit(sql), nnkPrefix.newTree(ident"@", (if values.len > 0: values else: nnkBracket.newTree())))))
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

macro exec*(sql: untyped) =
  var sql = sql
  if sql.kind == nnkBlockExpr: sql = sql[1][^1]
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult", "ozarkInsertResult", "ozarkCreateTableResult", "ozarkRemoveResult", "ozarkUpsertResult"]:
    error("The argument to `exec` must be the result of a `where`, `rawSQL`, or `insert`/`update` macro. Got " & (if sql.kind == nnkCall: $sql[0] else: $sql.kind), sql)
  if sql.len == 2:
    sql.add(nnkPrefix.newTree(ident"@", nnkBracket.newTree()))
  try:
    if sql[0].strVal == "ozarkUpsertResult":
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      let argBracket = paramsBracketOf(sql)
      result = macros.parseStmt(stub % [sql[1].strVal, (if argBracket.len > 0: ", " & $argBracket.mapIt("toDbValue(" & it.repr & ")").join(",") else: ""), randId.repr, $(argBracket.len)])
      return
    let sqlNode = parseSQL(normalizeInLists($sql[1], SqlDriver.mysql), sqlDriver = SqlDriver.mysql)
    let argBracket = paramsBracketOf(sql)
    case sqlNode.sons[0].kind
    of nkInsert, nkDelete:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      result = macros.parseStmt(stub % [sql[1].strVal, (if argBracket.len > 0: ", " & $argBracket.mapIt("toDbValue(" & it.repr & ")").join(",") else: ""), randId.repr, $(argBracket.len)])
    of nkUpdate:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      result = macros.parseStmt(stub % [sql[1].strVal, (if argBracket.len > 0: ", " & $argBracket.mapIt("toDbValue(" & it.repr & ")").join(",") else: ""), randId.repr, $(argBracket.len)])
    of nkCreateTable, nkCreateTableIfNotExists:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      result = macros.parseStmt(stub % [$sql[1], "", randId.repr, "0"])
    of nkDropTable, nkDropTableIfExists, nkDropIfExists:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      result = macros.parseStmt(stub % [$sql[1], "", randId.repr, "0"])
    else: discard
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

macro execGet*(sql: untyped): untyped =
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkInsertResult"]:
    error("The argument to `execGet` must be the result of an `insert` or `delete` macro.")
  try:
    let argBracket = paramsBracketOf(sql)
    result = macros.parseStmt("block:\n  insertId(dbcon, SqlQuery(\"$1\"), $2)" % [sql[1].strVal, argBracket.mapIt(it.repr).join(",")])
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

import std/asyncdispatch

macro asyncGetAll*(sql: untyped): untyped =
  let tmp = genSym(nskLet, "ozarkAsyncResult")
  result = quote do:
    block:
      let `tmp` = `sql`.getAll()
      let fut = newFuture[typeof(`tmp`)]("ozarkAsyncGetAll")
      fut.complete(`tmp`)
      fut

macro asyncGet*(sql: untyped): untyped =
  let tmp = genSym(nskLet, "ozarkAsyncResult")
  result = quote do:
    block:
      let `tmp` = `sql`.get()
      let fut = newFuture[typeof(`tmp`)]("ozarkAsyncGet")
      fut.complete(`tmp`)
      fut

macro asyncExec*(sql: untyped): untyped =
  result = quote do:
    block:
      proc ozarkAsyncExecRun() =
        `sql`.exec()
      ozarkAsyncExecRun()
      let fut = newFuture[void]("ozarkAsyncExec")
      fut.complete()
      fut
