# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

import std/[macros, macrocache, strutils,
      sequtils, tables, os, random]

import pkg/db_connector/postgres {.all.}
import pkg/db_connector/db_postgres {.all.}
import pkg/db_connector/db_common {.all.}

import pkg/parsesql

import ./model, ./collection
import ./private/types

export SqlQuery, mapIt

type
  OzarkModelDefect* = object of CatchableError

randomize() # initialize random seed for generating unique statement names in `tryInsertID`

template table*(models: ptr ModelsTable, name): untyped = 
  ## Define SQL statement for a table
  # checkTableExists(name)
  bindSym($name)

template withTableCheck*(name: NimNode, body) =
  ## Check if a model with the given name exists in the Models table.
  if not StaticSchemas.hasKey(getTableName($name[1])):
    raise newException(OzarkModelDefect,
        "Unknown model `" & $name[1] & "`")
  body

template withColumnsCheck(model: NimNode, cols: openArray[string], body) =
  for col in cols:
    withColumnCheck(model, col):
      discard
  body

proc ozarkSelectResult(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereResult(sql: static[string], val: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereInResult(sql: static[string], vals: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkRawSQLResult(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkInsertResult(sql: static[string], values: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkLimitResult(sql: static[string], count: int): NimNode {.compileTime.} = newLit(sql)

proc ozarkHoldModel[T](t: T) {.compileTime.} =
  var x: T

template withColumnCheck(model: NimNode, col: string, body) =
  if col == "*":
    body # allow all columns, no need to check for existence
  elif not col.validIdentifier:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  else:
    let x = model[1].getImpl
    expectKind(x, nnkTypeDef)   # ensure it's a type definition
    expectKind(x[2], nnkRefTy)  # ensure it's a ref object
    expectKind(x[2][0], nnkObjectTy) # ensure it's an object type
    expectKind(x[2][0][1], nnkOfInherit)
    if x[2][0][1][0] != bindSym"Model":
      raise newException(OzarkModelDefect, "The first argument must be a model type.")
    var withColumnCheckPassed: bool
    for field in x[2][0][2]:
      if $(field[0][1]) == col:
        withColumnCheckPassed = true
        body; break
    if not withColumnCheckPassed:
      raise newException(OzarkModelDefect,
        "Column `" & col & "` does not exist in model `" & $model[1] & "`.")

macro select*(tableName: untyped, cols: static openArray[string]): untyped =
  ## Define `SELECT` clause
  withTableCheck tableName:
    withColumnsCheck tableName, cols:
      result = nnkBlockStmt.newTree(
        newEmptyNode(),
        newCall(bindSym"ozarkHoldModel", tableName),
        newCall(bindSym"ozarkSelectResult",
          newLit("SELECT " & cols.join() & " FROM " & getTableName($tableName[1]))
        )
      )

macro select*(tableName: untyped, col: static string): untyped =
  ## Define SELECT clause
  withTableCheck tableName:
    withColumnCheck tableName, col:
      result = nnkBlockStmt.newTree(
        newEmptyNode(),
        newStmtList(
          newCall(bindSym"ozarkHoldModel", tableName),
          newCall(bindSym"ozarkSelectResult",
            newLit("SELECT " & col & " FROM " & getTableName($tableName[1]))
          )
        )
      )

macro selectAll*(tableName: untyped): untyped =
  ## Define SELECT * clause
  withTableCheck tableName:
    result = nnkBlockStmt.newTree(
      newEmptyNode(),
      newStmtList(
        newCall(bindSym"ozarkHoldModel", tableName),
        newCall(bindSym"ozarkSelectResult",
          newLit("SELECT * FROM " & getTableName($tableName[1]))
        )
      )
    )

#
# WHERE clause macros
#

# - WHERE caluse Writers
proc writeWhereLikeStatements(op: static string, sql: NimNode,
                      infix: NimNode, col: string): NimNode {.compileTime.} =
  # Writer macro for both `whereLike` and `whereNotLike` to avoid code duplication.
  # This macro generates the SQL string for the WHERE LIKE/NOT LIKE clause and
  # also constructs the appropriate infix expression for the value with wildcards
  if sql.kind != nnkBlockExpr or sql[1][1][0].strVal != "ozarkSelectResult":
    error("The first argument to `where` statement must be the result of a `select` macro.")
  withColumnCheck(sql[1][0][1], col):
    let selectSql = sql[1][1][1].strVal
    sql[1][1][0] = bindSym"ozarkWhereResult"
    sql[1][1][1].strVal = sql[1][1][1].strVal & " WHERE " & col & " " & op & " $1"
    sql[1][1].add(infix)
    result = sql

proc writeWhereInWhereNotIn(op: static string,
      sql: NimNode, col: string, vals: NimNode): NimNode {.compileTime.} =
  # Writer macro for both `whereIn` and `whereNotIn` to avoid code duplication.
  # This macro generates the SQL string for the WHERE IN/NOT IN clause and
  # also adds the values as additional arguments to the macro result for later use in code generation
  if sql.kind != nnkBlockExpr or sql[1][1][0].strVal!= "ozarkSelectResult":
    error("The first argument to must be the result of a `select` macro.")
  withColumnCheck(sql[1][0][1], col):
    var placeholders = newSeq[string](vals.len)
    for i in 0..<vals.len:
      placeholders[i] = "$" & $(i + 1)
    let selectSql = sql[1][1][1].strVal
    sql[1][1][0] = bindSym"ozarkWhereInResult"
    sql[1][1][1].strVal = sql[1][1][1].strVal & " WHERE " & col & " " & op & " (" & placeholders.join(",") & ")"
    for i in 0..<vals.len:
      # add the values as additional arguments to the
      # macro result for later use in code generation
      sql[1][1].add(vals[i])
    result = sql

proc writeWhereStatement(op: static string, sql: NimNode,
      col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for simple WHERE clauses (e.g. `where`, `whereNot`) to avoid code duplication.
  if sql.kind != nnkBlockExpr or sql[1][1][0].strVal != "ozarkSelectResult":
    error("The first argument to `where` must be the result of a `select` macro.")
  withColumnCheck(sql[1][0][1], col):
    sql[1][1][0] = bindSym"ozarkWhereResult"
    sql[1][1][1].strVal = sql[1][1][1].strVal & " WHERE " & col & " " & op & " $1"
    sql[1][1].add(val)
    result = sql

proc writeOrWhereStatement(op: static string,
      sql: NimNode, col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for `orWhere` to avoid code duplication with `writeWhereStatement`.
  # This macro checks that the first argument is a valid `where` result and then
  # appends the new condition with an OR to the existing SQL string.
  if sql.kind != nnkBlockExpr or sql[1][1][0].strVal != "ozarkWhereResult":
    error("The first argument to `orWhere` must be the result of a `where` macro.")
  withColumnCheck(sql[1][0][1], col):
    let len = sql[1][1][2][1].len + 1 # calculate the new param index based on existing params
    sql[1][1][1].strVal = sql[1][1][1].strVal & " OR " & col & " " & op & " $" & $(len)
    sql[1][1][2][1].add(val)
    result = sql

# WHERE clause public macros
macro where*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause
  writeWhereStatement("=", sql, col, val)

macro whereNot*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with NOT
  writeWhereStatement("!=", sql, col, val)

macro orWhere*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define OR in WHERE clause
  writeOrWhereStatement("=", sql, col, val)

macro orWhereNot*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define OR with NOT in WHERE clause
  writeOrWhereStatement("!=", sql, col, val)

macro whereStartsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with LIKE for prefix matching
  writeWhereLikeStatements("LIKE", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereEndsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with LIKE for suffix matching
  writeWhereLikeStatements("LIKE", sql,
    nnkInfix.newTree(ident"&", newLit("%"), val), col)

macro whereLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with LIKE for any position
  writeWhereLikeStatements("LIKE", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereNotLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with NOT LIKE for any position
  writeWhereLikeStatements("NOT LIKE", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereNotStartsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with `NOT LIKE` for prefix matching
  writeWhereLikeStatements("NOT LIKE", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereNotEndsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with `NOT LIKE` for suffix matching
  writeWhereLikeStatements("NOT LIKE", sql,
    nnkInfix.newTree(ident"&", newLit("%"), val), col)

macro whereIn*(sql: untyped, col: static string, vals: openArray[untyped]): untyped =
  ## Define WHERE clause with IN operator
  writeWhereInWhereNotIn("IN", sql, col, vals)

macro whereNotIn*(sql: untyped, col: static string, vals: openArray[untyped]): untyped =
  ## Define WHERE clause with NOT IN operator
  writeWhereInWhereNotIn("NOT IN", sql, col, vals)

template parseSqlQuery(getRowProcName: string, args: seq[NimNode] = @[]) {.dirty.} =
  try:
    let parsedSql = parseSQL(sql[1][1][1].strVal)
    # extract selected column names from parsedSql AST
    var colNames: seq[string]
    let top = parsedSql.sons[0]
    let selectNode = top
    let selectList = selectNode.sons[0]
    for c in selectList.sons:
      if c.kind == nkIdent and c.strVal == "*":
        colNames.add("*")
      else:
        if c.len > 0:
          colNames.add(c[0].strVal)
        else:
          colNames.add(c.strVal)

    # generate code to assign columns to model instance fields
    var idx = 0
    var assigns: seq[string]
    for cn in colNames:
      if cn != "*":
        assigns.add("inst." & cn & " = row[" & $idx & "]")
      else:
        # assign all columns to fields with matching names
        let modelFields = getTypeImpl(m)[0].getTypeImpl[1]
        for field in getImpl(m)[2][0][2]:
          assigns.add("inst." & $(field[0][1]) & " = row[" & $idx & "]")
      inc idx
    
    # Create the runtime code that fetches the row and
    # applies the generated assignments
    var runtimeCode: string
    if getRowProcName == "getRow":
      let randId = genSym(nskVar, "id")
      runtimeCode =
        staticRead("private" / "stubs" / "iteratorGetRow.nim") % [
          $parsedSql, 
          $(m.getImpl[0][1]),
          assigns.join("\n    "),
          getRowProcName,
          (if args.len > 0: "," & args.mapIt(it.repr).join(",") else: ""),
          (if args.len > 0: $args.len else: "0"),
          randId.repr
        ]
    else:
      let randId = genSym(nskVar, "id")
      runtimeCode =
        staticRead("private" / "stubs" / "iteratorInstantRows.nim") % [
          $parsedSql,
          $(m.getImpl[0][1]),
          colNames.mapIt("\"" & it & "\"").join(","),
          getRowProcName,
          (if args.len > 0: "," & args.mapIt(it.repr).join(",") else: ""),
          (if args.len > 0: $args.len else: "0"),
          randId.repr
        ]
    result = macros.parseStmt(runtimeCode) # parse the generated code into a NimNode
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

macro getAll*(sql: untyped): untyped =
  ## Finalize and get all results of the SQL statement.
  ## This macro produce the final SQL string and wraps it in a runtime call
  ## to execute it and return all rows via `instantRows`
  if sql.kind != nnkBlockExpr or sql[1][1][0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult", "ozarkLimitResult"]:
    error("The argument to `getAll` must be the result of a `where` macro.")
  let m = sql[1][0][1][1] # extract the model type from the macro arguments for later use in code generation
  let v = sql[1][1][2]    # extract the additional arguments (e.g. for WHERE IN) from the macro arguments for later use in code generation
  parseSqlQuery("instantRows", @[v])

macro get*(sql: untyped): untyped =
  ## Finalize SQL statement. This macro produces the final SQL
  ## string and emits runtime code that maps selected columns into a new instance of `m`
  if sql.kind != nnkBlockExpr or sql[1][1][0].strVal notin ["ozarkWhereResult", "ozarkWhereInResult", "ozarkRawSQLResult", "ozarkLimitResult"]:
    error("The argument to `get` must be the result of a `where` macro.")
  let m = sql[1][0][1][1] # extract the model type from the macro arguments for later use in code generation
  if sql[1][1][0].strval == "ozarkWhereInResult":
    var vals: seq[NimNode]
    for n in sql[1][1][2][1]:
      vals.add(n)
    parseSqlQuery("getRow", vals)
  else:
    let v = sql[1][1][2]    # extract the additional arguments (e.g. for WHERE IN) from the macro arguments for later use in code generation
    parseSqlQuery("getRow", @[v])

macro insert*(tableName, data: untyped): untyped =
  ## Placeholder for INSERT queries
  withTableCheck tableName:
    expectKind(data, nnkTableConstr)
    var cols: seq[string]
    var values = newNimNode(nnkBracket)
    var valuesIds: seq[int]
    var idx = 1
    for kv in data:
      # var idx = genSym(nskVar, "v")
      let col = $kv[0]
      if col.validIdentifier:
        # todo check if column exists in model
        # todo check for NOT NULL columns without default values
        cols.add(col)
        values.add(kv[1])
        valuesIds.add(idx)
        inc idx
      else:
        raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
    result = newCall(
      bindSym"ozarkInsertResult",
      newLit("insert into " & getTableName($tableName[1]) & " (" & cols.join(",") & ") VALUES (" & valuesIds.mapIt("$" & $it).join(",") & ")"),
      nnkPrefix.newTree(ident"@", values)
    )

# macro exists*(tableName: static string) =
#   ## Search in the current table for a record matching
#   ## the specified values. This is a placeholder for an `EXISTS` query.
#   checkTableExists(tableName)

macro limit*(sql: untyped, count: untyped): untyped =
  ## Placeholder for a `LIMIT` clause in SQL queries.
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult"]:
    error("The argument to `get` must be the result of a `where` macro.")
  result = newCall(
      bindSym"ozarkLimitResult",
      newLit(sql[1].strVal & " LIMIT ?"),
      count
    )

# macro orderBy*(sql: untyped, col: static string, desc: bool = false): untyped =
#   ## Placeholder for an `ORDER BY` clause in SQL queries.
#   if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult"]:
#     error("The argument to `orderBy` must be the result of a `where` macro.")
#   if col.validIdentifier:
#     # todo check if column exists in model
#     discard
#   else:
#     raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
#   result = newCall(bindSym"ozarkLimitResult",
#       newLit(sql[1].strVal & " ORDER BY " & col & (if desc: " DESC" else: ""))
#     )

macro rawSQL*(models: ptr ModelsTable, sql: static string, values: varargs[untyped]): untyped =
  ## Allows raw SQL queries without losing safety of
  ## model checks (table name/column names) and SQL validation at compile time
  try:
    let sqlNode = parseSQL(sql)
    case sqlNode.sons[0].kind
    of nkSelect:
      # checking the select statement for if the specified
      # table name exists in the models and if the specified column names are valid
      let fromNode = sqlNode.sons[0].sons[1]
      assert fromNode.kind == nkFrom
      for table in fromNode.sons:
        withTableCheck ident(table[0].strVal):
          discard
    else: discard
    result = newCall(
      bindSym"ozarkRawSQLResult", newLit(sql)
    )
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

macro exec*(sql: untyped) =
  ## Finalize and execute an SQL statement that doesn't
  ## return results (e.g. INSERT, UPDATE, DELETE).
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult", "ozarkInsertResult"]:
    error("The argument to `exec` must be the result of a `where`, `rawSQL`, or `insert` macro.")
  try:
    let sqlNode = parseSQL(sql[1].strVal)
    result = newCall(
      ident"exec",
      ident"dbcon",
      newCall(ident"SqlQuery", newLit($sqlNode))
    )
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
    let sqlNode = parseSQL($sql[1])
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

proc `$`*(sql: SqlQuery): string =
  return string(sql)