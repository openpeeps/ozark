# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

import std/[macros, macrocache, strutils, sequtils, tables]
import pkg/db_connector/db_common
import pkg/parsesql

import ./model
import ./private/[ast, types]

export SqlQuery

type
  EnimsqlModelDefect* = object of CatchableError

template checkTableExists(name: string) =
  ## Check if a model with the given name exists in the Models table.
  if not StaticSchemas.hasKey(name):
    raise newException(EnimsqlModelDefect, "Unknown model `" & name & "`")

macro table*(models: ptr ModelsTable, name: static string): untyped = 
  ## Define SQL statement for a table
  checkTableExists(name)
  result = newLit(name)

proc ozarkSelectResult(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereResult(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkRawSQLResult(sql: static[string]): NimNode {.compileTime.} = newLit(sql)

macro select*(tableName: untyped, cols: static openArray[string]): untyped =
  ## Define SELECT clause
  checkTableExists($tableName)
  for col in cols:
    if col == "*" or col.validIdentifier:
      continue # todo check if column exists in model
    else:
      raise newException(EnimsqlModelDefect, "Invalid column name `" & col & "`")
  result = newCall(bindSym"ozarkSelectResult",
      newLit("SELECT " & cols.join() & " FROM " & $tableName)
    )

macro select*(tableName: untyped, col: static string): untyped =
  ## Define SELECT clause
  checkTableExists($tableName)
  if col == "*" or col.validIdentifier:
    # todo check if column exists in model
    discard
  else:
    raise newException(EnimsqlModelDefect, "Invalid column name `" & col & "`")
  result = newCall(bindSym"ozarkSelectResult",
      newLit("SELECT " & col & " FROM " & $tableName)
    )

macro where*(sql: untyped, col: static string, val: static string): untyped =
  ## Define WHERE clause
  if sql.kind != nnkCall or sql[0].strVal != "ozarkSelectResult":
    error("The first argument to `where` must be the result of a `select` macro.")
  if col.validIdentifier:
    # todo check if column exists in model
    discard
  else:
    raise newException(EnimsqlModelDefect, "Invalid column name `" & col & "`")
  let selectSql = sql[1].strVal
  result = newCall(bindSym"ozarkWhereResult",
    newLit(selectSql & " WHERE " & col & " = '" & val & "'")
  )

macro getAll*(sql: untyped): untyped =
  ## Finalize and get all results of the SQL statement.
  ## This macro produce the final SQL string and wraps it in a runtime call
  ## to execute it and return all rows via `getAllRows`
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult"]:
    error("The argument to `get` must be the result of a `where` macro.")
  try:
    let sqlNode = parseSql(sql[1].strVal)
    result = newCall(
      ident"getAllRows",
      ident"dbcon",
      newCall(
        ident"SqlQuery",
        newLit($sqlNode)
      )
    )
    # let parsedSql = parser.parseSQL(sql[1].strVal)
    # result = newCall(
    #   ident"getAllRows",
    #   ident"dbcon",
    #   newCall(
    #     ident"SqlQuery",
    #     newLit($parsedSql)
    #   )
    # )
    
  except SqlParseError as e:
    raise newException(EnimsqlModelDefect, "SQL Parsing Error: " & e.msg)

macro get*(sql: untyped): untyped =
  ## Finalize SQL statement. This macro produce
  ## the final SQL string and wraps it in a runtime call
  ## to execute it and return a single row via `getRow`
  if sql.kind != nnkCall or sql[0].strVal notin ["ozarkWhereResult", "ozarkRawSQLResult"]:
    error("The argument to `get` must be the result of a `where` macro.")
  try:
    let parsedSql = parseSQL(sql[1].strVal)
    result = newCall(
      ident"getRow",
      ident"dbcon",
      newCall(
        ident"SqlQuery",
        newLit($parsedSql)
      )
    )
  except SqlParseError as e:
    raise newException(EnimsqlModelDefect, "SQL Parsing Error: " & e.msg)

macro insert*(tableName: static string, data: untyped): untyped =
  ## Placeholder for INSERT queries
  checkTableExists(tableName)
  expectKind(data, nnkTableConstr)
  var cols, val: seq[string]
  for kv in data:
    let col = $kv[0]
    if col.validIdentifier:
      # todo check if column exists in model
      # todo check for NOT NULL columns without default values
      cols.add(col)
      case kv[1].kind
        of nnkIntLit:
          val.add($kv[1].intVal)
        of nnkFloatLit:
          val.add($kv[1].floatVal)
        of nnkStrLit:
          val.add(kv[1].strVal)
        else: discard
    else:
      raise newException(EnimsqlModelDefect, "Invalid column name `" & col & "`")
  try:
    let parsedSql = newLit("insert into " & $tableName & " (" & cols.join(", ") & ") VALUES (" & val.mapIt("?").join(", ") & ")")
    result = newLit($parsedSql)
  except SqlParseError as e:
    raise newException(EnimsqlModelDefect, "SQL Parsing Error: " & e.msg)

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
    raise newException(EnimsqlModelDefect, "SQL Parsing Error: " & e.msg)

macro exec*(sql: untyped) =
  ## Finalize and execute an SQL statement that doesn't
  ## return results (e.g. INSERT, UPDATE, DELETE).
  if sql.kind != nnkStrLit:
    error("The argument to `exec` must be a string literal containing the SQL statement.")
  result = newCall(
    ident"getRow",
    ident"dbcon",
    newCall(
      ident"SqlQuery",
      sql
    )
  )

macro execGet*(sql: untyped) =
  ## Finalize and execute an SQL statement that returns
  ## results (e.g. SELECT, INSERT with RETURNING).
  ## 
  ## This macro produces the final SQL string and
  ## wraps it in a runtime call
  if sql.kind != nnkStrLit:
    error("The argument to `execGet` must be a string literal containing the SQL statement.")

proc `$`*(sql: SqlQuery): string =
  return string(sql)