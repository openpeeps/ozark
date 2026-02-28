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

template checkTableExists(name: string) =
  ## Check if a model with the given name exists in the Models table.
  if not StaticSchemas.hasKey(name):
    raise newException(OzarkModelDefect, "Unknown model `" & name & "`")

randomize() # initialize random seed for generating unique statement names in `tryInsertID`

macro table*(models: ptr ModelsTable, name: static string): untyped = 
  ## Define SQL statement for a table
  checkTableExists(name)
  result = newLit(name)

proc ozarkSelectResult(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereResult(sql: static[string], val: string): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereInResult(sql: static[string], vals: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkRawSQLResult(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkInsertResult(sql: static[string], values: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkLimitResult(sql: static[string], count: int): NimNode {.compileTime.} = newLit(sql)

macro select*(tableName: untyped, cols: static openArray[string]): untyped =
  ## Define SELECT clause
  checkTableExists($tableName)
  for col in cols:
    if col == "*" or col.validIdentifier:
      continue # todo check if column exists in model
    else:
      raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  result = newCall(bindSym"ozarkSelectResult",
      newLit("SELECT " & cols.join() & " FROM " & $tableName)
    )

macro select*(tableName: untyped, col: static string): untyped =
  ## Define SELECT clause
  checkTableExists($tableName)
  if col == "*" or col.validIdentifier:
    discard
  else:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  result = newCall(bindSym"ozarkSelectResult",
      newLit("SELECT " & col & " FROM " & $tableName)
    )

macro selectAll*(tableName: untyped): untyped =
  ## Define SELECT * clause
  checkTableExists($tableName)
  result = newCall(bindSym"ozarkSelectResult", newLit("SELECT * FROM " & $tableName))


#
# WHERE clause macros
#

# - WHERE caluse Writers
proc writeWhereLikeStatements(op: static string, sql: NimNode,
                      infix: NimNode, col: string): NimNode {.compileTime.} =
  # Writer macro for both `whereLike` and `whereNotLike` to avoid code duplication.
  # This macro generates the SQL string for the WHERE LIKE/NOT LIKE clause and
  # also constructs the appropriate infix expression for the value with wildcards
  if sql.kind != nnkCall or sql[0].strVal != "ozarkSelectResult":
    error("The first argument to `where` statement must be the result of a `select` macro.")
  if col.validIdentifier:
    # todo check if column exists in model
    discard
  else:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  let selectSql = sql[1].strVal
  result = newCall(bindSym"ozarkWhereResult",
    newLit(selectSql & " WHERE " & col & " " & op & " $1"),
    infix
  )

proc writeWhereInWhereNotIn(op: static string,
      sql: NimNode, col: string, vals: NimNode): NimNode {.compileTime.} =
  # Writer macro for both `whereIn` and `whereNotIn` to avoid code duplication.
  # This macro generates the SQL string for the WHERE IN/NOT IN clause and
  # also adds the values as additional arguments to the macro result for later use in code generation
  if sql.kind != nnkCall or sql[0].strVal != "ozarkSelectResult":
    error("The first argument to must be the result of a `select` macro.")
  if col.validIdentifier:
    # todo check if column exists in model
    discard
  else:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  let selectSql = sql[1].strVal
  var placeholders = newSeq[string](vals.len)
  for i in 0..<vals.len:
    placeholders[i] = "$" & $(i + 1)
  result = newCall(
    bindSym"ozarkWhereInResult",
    newLit(selectSql & " WHERE " & col & " " & op & " (" & placeholders.join(",") & ")"),
  )
  for i in 0..<vals.len:
    # add the values as additional arguments to the
    # macro result for later use in code generation
    result.add(vals[i])

proc writeWhereStatement(op: static string, sql: NimNode, col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for simple WHERE clauses (e.g. `where`, `whereNot`) to avoid code duplication.
  if sql.kind != nnkCall or sql[0].strVal != "ozarkSelectResult":
    error("The first argument to `where` must be the result of a `select` macro.")
  if col.validIdentifier:
    # todo check if column exists in model
    discard
  else:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  let selectSql = sql[1].strVal
  result = newCall(bindSym"ozarkWhereResult",
    newLit(selectSql & " WHERE " & col & " " & op & " $1"),
    val
  )

proc writeOrWhereStatement(op: static string, sql: NimNode, col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for `orWhere` to avoid code duplication with `writeWhereStatement`.
  # This macro checks that the first argument is a valid `where` result and then
  # appends the new condition with an OR to the existing SQL string.
  if sql.kind != nnkCall or sql[0].strVal != "ozarkWhereResult":
    error("The first argument to `orWhere` must be the result of a `where` macro.")
  if col.validIdentifier:
    # todo check if column exists in model
    discard
  else:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  let whereSql = sql[1].strVal
  result = newCall(bindSym"ozarkWhereResult",
    newLit(whereSql & " OR " & col & " " & op & " $1"),
    val
  )

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
    let parsedSql = parseSQL(sql[1].strVal)
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
        let modelFields = getTypeImpl(m)[1]
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
          $(getTypeImpl(m)[1]),
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
          $(getTypeImpl(m)[1]),
          colNames.mapIt("\"" & it & "\"").join(","),
          getRowProcName,
          (if args.len > 0: "," & args.mapIt(it.repr).join(",") else: ""),
          (if args.len > 0: $args.len else: "0"),
          randId.repr
        ]
    result = macros.parseStmt(runtimeCode) # parse the generated code into a NimNode
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

macro getAll*(sql: untyped, m: typedesc): untyped =
  ## Finalize and get all results of the SQL statement.
  ## This macro produce the final SQL string and wraps it in a runtime call
  ## to execute it and return all rows via `instantRows`
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult", "ozarkLimitResult"]:
    error("The argument to `get` must be the result of a `where` macro.")
  # if sql[0].strVal == "ozarkLimitResult":
  #   parseSqlQuery("instantRows", @[nnkPrefix.newTree(ident"$", sql[2])])
  # else:
  parseSqlQuery("instantRows", @[nnkPrefix.newTree(ident"$", sql[2])])

macro get*(sql: untyped, m: typedesc): untyped =
  ## Finalize SQL statement. This macro produces the final SQL
  ## string and emits runtime code that maps selected columns into a new instance of `m`
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkWhereInResult", "ozarkRawSQLResult"]:
    error("The argument to `get` must be the result of a `where` or `rawSQL` macro.")
  if sql[0].strval == "ozarkWhereInResult":
    parseSqlQuery("getRow", @[nnkPrefix.newTree(sql[2][1])])
  else:
    parseSqlQuery("getRow", @[nnkPrefix.newTree(newCall(ident"$", sql[2]))])

macro insert*(tableName: static string, data: untyped): untyped =
  ## Placeholder for INSERT queries
  checkTableExists(tableName)
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
    newLit("insert into " & $tableName & " (" & cols.join(",") & ") VALUES (" & valuesIds.mapIt("$" & $it).join(",") & ")"),
    nnkPrefix.newTree(ident"@", values)
  )

macro exists*(tableName: static string) =
  ## Search in the current table for a record matching
  ## the specified values. This is a placeholder for an `EXISTS` query.
  checkTableExists(tableName)

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
        checkTableExists(table[0].strVal)
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
      discard
    else: discard 
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

proc `$`*(sql: SqlQuery): string =
  return string(sql)