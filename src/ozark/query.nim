# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

import std/[macros, macrocache, strutils, sequtils, tables, os]

import pkg/db_connector/[db_postgres, db_common]
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

macro table*(models: ptr ModelsTable, name: static string): untyped = 
  ## Define SQL statement for a table
  checkTableExists(name)
  result = newLit(name)

proc ozarkSelectResult(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereResult(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
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

macro where*(sql: untyped, col: static string, val: static string): untyped =
  ## Define WHERE clause
  if sql.kind != nnkCall or sql[0].strVal != "ozarkSelectResult":
    error("The first argument to `where` must be the result of a `select` macro.")
  if col.validIdentifier:
    # todo check if column exists in model
    discard
  else:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  let selectSql = sql[1].strVal
  result = newCall(bindSym"ozarkWhereResult",
    newLit(selectSql & " WHERE " & col & " = '" & val & "'")
  )

macro whereNot*(sql: untyped, col: static string, val: static string): untyped =
  ## Define WHERE clause with NOT
  if sql.kind != nnkCall or sql[0].strVal != "ozarkSelectResult":
    error("The first argument to `whereNot` must be the result of a `select` macro.")
  if col.validIdentifier:
    # todo check if column exists in model
    discard
  else:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  let selectSql = sql[1].strVal
  result = newCall(bindSym"ozarkWhereResult",
    newLit(selectSql & " WHERE " & col & " != '" & val & "'")
  )

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
      runtimeCode =
        staticRead("private" / "stubs" / "iteratorGetRow.nim") % [
          $parsedSql, 
          $(getTypeImpl(m)[1]),
          assigns.join("\n    "), getRowProcName
        ]
    else:
      runtimeCode =
        staticRead("private" / "stubs" / "iteratorInstantRows.nim") % [
          $parsedSql,
          $(getTypeImpl(m)[1]),
          colNames.mapIt("\"" & it & "\"").join(","),
          getRowProcName,
            if args.len > 0: "," & args.mapIt(it.repr).join(",")
            else: ""
        ]
    result = macros.parseStmt(runtimeCode) # parse the generated code into a NimNode
    # echo result.repr
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

macro getAll*(sql: untyped, m: typedesc): untyped =
  ## Finalize and get all results of the SQL statement.
  ## This macro produce the final SQL string and wraps it in a runtime call
  ## to execute it and return all rows via `instantRows`
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult", "ozarkLimitResult"]:
    error("The argument to `get` must be the result of a `where` macro.")
  if sql[0].strVal == "ozarkLimitResult":
    parseSqlQuery("instantRows", @[nnkPrefix.newTree(ident"$", sql[2])])
  else:
    parseSqlQuery("instantRows")

macro get*(sql: untyped, m: typedesc): untyped =
  ## Finalize SQL statement. This macro produces the final SQL
  ## string and emits runtime code that maps selected columns into a new instance of `m`
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult"]:
    error("The argument to `get` must be the result of a `where` or `rawSQL` macro.")
  parseSqlQuery("getRow")

macro insert*(tableName: static string, data: untyped): untyped =
  ## Placeholder for INSERT queries
  checkTableExists(tableName)
  expectKind(data, nnkTableConstr)
  var cols: seq[string]
  var values = newNimNode(nnkBracket)
  for kv in data:
    let col = $kv[0]
    if col.validIdentifier:
      # todo check if column exists in model
      # todo check for NOT NULL columns without default values
      cols.add(col)
      values.add(kv[1])
    else:
      raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  result = newCall(
    bindSym"ozarkInsertResult",
    newLit("insert into " & $tableName & " (" & cols.join(",") & ") VALUES (" & values.mapIt("?").join(",") & ")"),
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
  if sql.kind != nnkStrLit:
    error("The argument to `exec` must be a string literal containing the SQL statement.")
  result = newCall(
    ident"exec",
    ident"dbcon",
    newCall(ident"SqlQuery", sql)
  )

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
      let stub = staticRead("private" / "stubs" / "tryInsertID.nim")
      result = macros.parseStmt(stub % [$sql[1], $sql[2][1].mapIt(it.repr).join(",")])
    of nkDelete:
      discard
    else: discard 
  except SqlParseError as e:
    raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)

proc `$`*(sql: SqlQuery): string =
  return string(sql)