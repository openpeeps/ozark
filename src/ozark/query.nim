# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

import std/[macros, macrocache, strutils, options,
      sequtils, tables, os, random, strformat]

import pkg/parsesql
import pkg/db_connector/postgres {.all.}
import pkg/db_connector/db_postgres {.all.}
import pkg/db_connector/db_common {.all.}

import ./model, ./collection
import ./private/types

include ./runtime_helpers

export SqlQuery, mapIt

type
  OzarkModelDefect* = object of CatchableError

const preparedQueryStatements = CacheTable"preparedQueryStatements"

randomize() # initialize random seed for generating unique statement names in `tryInsertID`

template table*(models: ptr ModelsTable, name): untyped = 
  ## Define SQL statement for a table
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

proc ozarkSelectResult*(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereResult*(sql: static[string], val: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereInResult*(sql: static[string], vals: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkRawSQLResult(sql: static[string], vals: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkInsertResult*(sql: static[string], values: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkUpdateResult*(sql: static[string], values: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkLimitResult*(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkOrderByResult*(sql: static[string], vals: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkCreateTableResult*(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkRemoveResult*(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkHoldModel*[T](t: T) {.compileTime.} =
  var x: T

proc ozarkHoldModel*[T: typedesc](t: T) {.compileTime.} =
  var x: T

macro extractSQL*(sql: NimNode): untyped =
  ## Extracts the SQL `NimNode` to string for use in code generation
  result = newLit(sql.repr)

macro fromSQL*(sql: untyped): untyped =
  ## Macro to parse a resumed SQL string back into a NimNode for further manipulation
  ## This is used in the `where` macros to allow chaining multiple clauses based on runtime computations.
  let sqlStrNode = sql.getImpl()
  var parseStmtNode = parseStmt(sqlStrNode[^1].strVal)
  parseStmtNode[0][1][0][1].insert(0, newEmptyNode())
  result = parseStmtNode[0]

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
      error("The first argument must be a model type.", x[2][0][1][0])
    var checkPassed: bool
    for field in x[2][0][2]:
      if $(field[0][1]) == col:
        checkPassed = true
        body; break
    if not checkPassed:
      error("Column `" & col & "` does not exist in model `" & $model[1] & "`.")

template withColumn(x: NimNode, col: string, body) =
  if col == "*":
    body # allow all columns, no need to check for existence
  elif not col.validIdentifier:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  else:
    expectKind(x, nnkTypeDef)   # ensure it's a type definition
    expectKind(x[2], nnkRefTy)  # ensure it's a ref object
    expectKind(x[2][0], nnkObjectTy) # ensure it's an object type
    # expectKind(x[2][0][1], nnkOfInherit)
    # if x[2][0][1][0] != bindSym"Model":
    #   error("The first argument must be a model type.", x[2][0][1][0])
    var checkPassed: bool
    for field in x[2][0][2]:
      if $(field[0][1]) == col:
        checkPassed = true
        body; break
    if not checkPassed:
      error("Column `" & col & "` does not exist in model `" & $x[0][1] & "`.")

macro prepareTable*(modelName): untyped =
  ## Compile-time macro to prepare a model's table in the database.
  ## 
  ## This macro generates the SQL string for creating the table based
  ## on the model definition and executes it at compile time to ensure
  ## the table exists before any queries are made against it.
  withTableCheck(modelName):
    let tableName = getTableName($modelName[1])
    let schema = SqlSchemas[tableName]
    let id = genSym(nskType, "ozarkModel" & tableName)
    var types: seq[SqlNode]
    for k, v in StaticSchemas[tableName]:
      if v.kind == nnkTypeSection:
        for f in v[0][2][0][2]:
          let fieldName = f[0][1].strVal
          types.add(schema[fieldName])

    var compositePkCols: seq[string] # to hold column names for primary keys
    let columnDefs = types.map(proc(t: SqlNode): string =
      var colDef = t[0].strVal & " "
      if t[1].kind == nkIdent:
        colDef &= t[1].strVal
      elif t[1].kind == nkCall:
        colDef &= t[1][0].strVal & "(" & 
          t[1].sons[1..^1].mapIt($it.strVal).join(", ") & ")"
      if t.len > 2:
        for i in 2..<t.len:
          if t[i].kind == nkPrimaryKey:
            compositePkCols.add(t[0].strVal)
          else:
            colDef &= " " & $t[i]
      colDef
    )

    # fallback if no `{.pk.}` pragma is explicitly declared,
    # use `id` when present, as the primary key column by convention
    # otherwise the table will be created without a primary key, which
    # is not ideal but still functional for basic queries
    if compositePkCols.len == 0:
      for t in types:
        if t[0].strVal == "id":
          compositePkCols.add("id")
          break

    var sql = "CREATE TABLE IF NOT EXISTS " & tableName & " (" & columnDefs.join(", ")
    if compositePkCols.len > 0:
      sql &= ", PRIMARY KEY (" & compositePkCols.join(", ") & ")"
    sql &= ")"

    result = newCall(
      bindSym"ozarkCreateTableResult",
      newLit(sql)
    )

macro dropTable*(modelName: untyped, cascade: static bool = false): untyped =
  ## Compile-time macro to drop a model's table from the database.
  withTableCheck(modelName):
    let tableName = getTableName($modelName[1])
    result = newCall(
      bindSym"ozarkRawSQLResult",
      newLit("DROP TABLE IF EXISTS " & tableName & (
        if cascade: " CASCADE" else: ""
      )),
    )

#
# INSERT and UDATE clause macros 
#
macro insert*(tableName, data: untyped): untyped =
  ## Placeholder for an `INSERT` statement. This macro generates the SQL string for the
  ## INSERT statement. This macro performs compile-time checks for the existence
  ## of the specified model and the validity of the column names.
  withTableCheck tableName:
    expectKind(data, nnkTableConstr)
    var cols: seq[string]
    var values = newNimNode(nnkBracket)
    var valuesIds: seq[int]
    var idx = 1
    for kv in data:
      let col = $kv[0]
      withColumnCheck(tableName, col):
        cols.add(col)
        values.add(kv[1])
        valuesIds.add(idx)
        inc idx
    result = newCall(
      bindSym"ozarkInsertResult",
      newLit("insert into " & getTableName($tableName[1]) &
            " (" & cols.join(",") & ") VALUES (" & valuesIds.mapIt("$" & $it).join(",") & ")"),
      nnkPrefix.newTree(ident"@", values)
    )

macro removeRow*(tableName: untyped): untyped =
  ## Placeholder for a `DELETE` statement. This macro generates the SQL string for the
  ## DELETE statement. This macro performs compile-time checks for the existence
  ## of the specified model.
  let dbTable = getTableName($tableName[1])
  let blockIdent = genSym(nskLabel, "ozarkBlock" & dbTable)
  withTableCheck tableName:
    result = nnkBlockStmt.newTree(
      blockIdent,
      newStmtList(
        newCall(bindSym"ozarkHoldModel", tableName),
        newCall(
          bindSym"ozarkRemoveResult",
          newLit("DELETE FROM " & getTableName($tableName[1]))
        )
      )
    )

macro update*(tableName, data: untyped): untyped =
  ## Placeholder for an `UPDATE` statement. This macro generates the SQL string for the
  ## UPDATE statement. This macro performs compile-time checks for the existence
  ## of the specified model and the validity of the column names.
  withTableCheck tableName:
    expectKind(data, nnkTableConstr)
    var setClauses: seq[string]
    var values = newNimNode(nnkBracket)
    var valuesIds: seq[int]
    var idx = 1
    for kv in data:
      let col = $kv[0]
      withColumnCheck(tableName, col):
        setClauses.add(col & " = $" & $idx)
        values.add(kv[1])
        valuesIds.add(idx)
        inc idx
    let dbTable = getTableName($tableName[1])
    let blockIdent = genSym(nskLabel, "ozarkBlock" & dbTable)
    result = nnkBlockStmt.newTree(
      blockIdent,
      newStmtList(
        newCall(bindSym"ozarkHoldModel", tableName),
        newCall(
          bindSym"ozarkUpdateResult",
          newLit("update " & getTableName($tableName[1]) &
                " set " & setClauses.join(", ")),
          nnkPrefix.newTree(ident"@", values)
        )
      )
    )

#
# SELECT clause macros
#
macro select*(tableName: untyped, cols: static openArray[string]): untyped =
  ## Define `SELECT` clause with specific columns.
  withTableCheck tableName:
    withColumnsCheck tableName, cols:
      let dbTable = getTableName($tableName[1])
      let blockIdent = genSym(nskLabel, "ozarkBlock" & dbTable)
      result = nnkBlockStmt.newTree(
        blockIdent,
        newStmtList(
          newCall(bindSym"ozarkHoldModel", tableName),
          newCall(bindSym"ozarkSelectResult",
            newLit("SELECT " & cols.join(",") & " FROM " & dbTable)
          )
        )
      )


macro select*(tableName: untyped, col: static string): untyped =
  ## Define SELECT clause
  withTableCheck tableName:
    withColumnCheck tableName, col:
      let dbTable = getTableName($tableName[1])
      let blockIdent = genSym(nskLabel, "ozarkBlock" & "_" & dbTable)
      result = nnkBlockStmt.newTree(
        blockIdent,
        newStmtList(
          newCall(bindSym"ozarkHoldModel", tableName),
          newCall(bindSym"ozarkSelectResult",
            newLit("SELECT " & col & " FROM " & dbTable)
          )
        )
      )

macro selectAll*(tableName: untyped): untyped =
  ## Define SELECT * clause
  withTableCheck tableName:
    let dbTable = getTableName($tableName[1])
    let blockIdent = genSym(nskLabel, "ozarkBlock" & dbTable)
    result = nnkBlockStmt.newTree(
      blockIdent,
      newStmtList(
        newCall(bindSym"ozarkHoldModel", tableName),
        newCall(bindSym"ozarkSelectResult",
          newLit("SELECT * FROM " & dbTable)
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
  if sql.kind != nnkBlockExpr or sql[1][^1][0].strVal != "ozarkSelectResult":
    error("The first argument to `where` statement must be the result of a `select` macro. Got " & $sql[1][^1][0])
  withColumnCheck(sql[1][0][1], col):
    let selectSql = sql[1][^1][1].strVal
    sql[1][^1][0] = bindSym"ozarkWhereResult"
    sql[1][^1][1].strVal = sql[1][^1][1].strVal & " WHERE " & col & " " & op & " $1"
    sql[1][^1].add(infix)
    result = sql

proc writeWhereInWhereNotIn(op: static string,
      sql: NimNode, col: string, vals: NimNode): NimNode {.compileTime.} =
  # Writer macro for both `whereIn` and `whereNotIn` to avoid code duplication.
  # This macro generates the SQL string for the WHERE IN/NOT IN clause and
  # also adds the values as additional arguments to the macro result for later use in code generation
  if sql.kind != nnkBlockExpr or sql[1][^1][0].strVal notin ["ozarkSelectResult", "ozarkWhereResult"]:
    error("The first argument to `WHERE` clause must be the result of a `select` macro.")
  withColumnCheck(sql[1][^2][1], col):
    let calledMacro = sql[1][^1][0].strVal
    var placeholders = newSeq[string](vals.len)
    let ozarkOrmVar = genSym(nskLet, "OzarkORMInValuesPlaceholder")
    let ozarkOrmCountArgs = genSym(nskLet, "OzarkORMInValuesCount")
    for i in 0..<placeholders.len:
      placeholders[i] = "$" & $(i + 1)
    sql[1][^1][0] = bindSym"ozarkWhereInResult"
    # update the SQL string to include a placeholder for th
    # list of values and also add the values as additional arguments
    # to the macro result for later use in code generation
    let initSql = sql[1][^1][1].strVal
    var len: int
    if sql[1][^1].len == 3 and sql[1][^1][2].kind == nnkHiddenStdConv:
      sql[1][^1][2][1].add(vals) # add the values as additional arguments to the macro
      len = sql[1][^1][2][1].len
    else:
      sql[1][^1].add(vals) # add the values as additional arguments to the macro
      len = 1
    sql[1][^1][1].strVal = initSql & (
        if calledMacro == "ozarkWhereResult":
          " AND " & col & " " & op & " ($" & $(len) & ")"
        else:
        " WHERE " & col & " " & op & " ($" & $(len) & ")"
      )
    result = sql

proc writeWhereStatement(op: static string, sql: NimNode,
      col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for simple WHERE clauses (e.g. `where`, `whereNot`) to avoid code duplication.
  if sql.kind != nnkBlockExpr or sql[1][^1][0].strVal notin ["ozarkSelectResult", "ozarkWhereResult", "ozarkUpdateResult", "ozarkRemoveResult"]:
    error("The first argument to `WHERE` must be the result of a `select` macro. Got " & $sql[1][^1][0], sql)
  if sql[1][^1][0].strVal in ["ozarkWhereResult"]:
    # if it's already a where result, we need to append to the existing
    # SQL string and add the new value as an additional argument
    withColumnCheck(sql[1][^2][1], col):
      sql[1][^1][0] = bindSym"ozarkWhereResult"
      let len = sql[1][^1][2][1].len + 1
      sql[1][^1][1].strVal = sql[1][^1][1].strVal & " AND " & col & " " & op & " $" & $(len)
      sql[1][^1][^1][1].add(val) # add to the current varargs list
  elif sql[1][^1][0].strVal == "ozarkUpdateResult":
    # if it's an update result, we need to append to the existing
    # SQL string and add the new value as an additional argument
    withColumnCheck(sql[1][^2][1], col):
      sql[1][^1][0] = bindSym"ozarkWhereResult"
      let len = sql[1][^1][2][1].len + 1
      sql[1][^1][1].strVal = sql[1][^1][1].strVal & " WHERE " & col & " " & op & " $" & $(len)
      sql[1][^1][2][1].add(val) # add to the current varargs list
  else:
    withColumnCheck(sql[1][^2][1], col):
      sql[1][^1][0] = bindSym"ozarkWhereResult"
      sql[1][^1][1].strVal = sql[1][^1][1].strVal & " WHERE " & col & " " & op & " $1"
      sql[1][^1].add(val)
  result = sql

proc writeOrWhereStatement(op: static string,
      sql: NimNode, col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for `orWhere` to avoid code duplication with `writeWhereStatement`.
  # This macro checks that the first argument is a valid `where` result and then
  # appends the new condition with an OR to the existing SQL string.
  if sql.kind != nnkBlockExpr or sql[1][^1][0].strVal != "ozarkWhereResult":
    error("The first argument to `orWhere` must be the result of a `where` macro.")
  withColumnCheck(sql[1][^2][1], col):
    let len = sql[1][^1][2][1].len + 1 # calculate the new param index based on existing params
    sql[1][^1][1].strVal = sql[1][^1][1].strVal & " OR " & col & " " & op & " $" & $(len)
    sql[1][^1][2][1].add(val)
    result = sql

template getSqlImpl() {.dirty.} = 
  var sql = sql
  if sql.kind == nnkSym:
    sql = sql.getImpl()[2] # handle the case where the macro is called with a symbol instead of a block (e.g. `where x = 5` instead of `where(select(...), x = 5)`)

# WHERE clause public macros
macro where*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `WHERE` clause
  writeWhereStatement("=", sql, col, val)

macro whereNot*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `WHERE` clause with `NOT`
  writeWhereStatement("!=", sql, col, val)

macro orWhere*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `OR` in `WHERE` clause
  writeOrWhereStatement("=", sql, col, val)

macro orWhereNot*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define a `OR` condition with `NOT` in `WHERE` clause
  writeOrWhereStatement("!=", sql, col, val)

macro whereStartsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `WHERE` clause with `LIKE` for prefix matching
  writeWhereLikeStatements("LIKE", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereEndsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `WHERE` clause with `LIKE` for suffix matching
  writeWhereLikeStatements("LIKE", sql,
    nnkInfix.newTree(ident"&", newLit("%"), val), col)

macro whereLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with `LIKE` for any position
  writeWhereLikeStatements("LIKE", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereNotLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with `NOT LIKE` for any position
  writeWhereLikeStatements("NOT LIKE", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereNotStartsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `WHERE` clause with `NOT LIKE` for prefix matching
  writeWhereLikeStatements("NOT LIKE", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereNotEndsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `WHERE` clause with `NOT LIKE` for suffix matching
  writeWhereLikeStatements("NOT LIKE", sql,
    nnkInfix.newTree(ident"&", newLit("%"), val), col)

macro whereIn*(sql: untyped, col: static string, vals: untyped): untyped =
  ## Define `WHERE` clause with `IN` operator
  writeWhereInWhereNotIn("IN", sql, col, vals)

macro whereNotIn*(sql: untyped, col: static string, vals: untyped): untyped =
  ## Define `WHERE` clause with `NOT IN` operator
  writeWhereInWhereNotIn("NOT IN", sql, col, vals)

#
# SQL Query Validator
#
proc countSqlArgs(args: NimNode): int {.compileTime.} =
  case args.kind
  of nnkEmpty:
    0
  of nnkBracket:
    args.len
  of nnkHiddenStdConv:
    if args.len > 1 and args[1].kind == nnkBracket: args[1].len else: 1
  else:
    1

proc appendSqlArgs(callNode: var NimNode, args: NimNode) {.compileTime.} =
  case args.kind
  of nnkEmpty:
    discard
  of nnkBracket:
    for a in args:
      callNode.add(a)
  of nnkHiddenStdConv:
    if args.len > 1 and args[1].kind == nnkBracket:
      for a in args[1]:
        callNode.add(a)
    else:
      callNode.add(args)
  else:
    callNode.add(args)

proc parseSqlQuery(sql: NimNode, getRowProcName: string,
            args: NimNode = newEmptyNode()): NimNode {.compileTime.} =
  # Compile-time procedure to validate the SQL query and 
  # generate the appropriate runtime code to execute it and
  # map the results to model instances. This procedure is called by the `get` and `getAll` macros.
  try:
    let parsedSql = parseSQL(sql[1][^1][1].strVal)
    let m = sql[1][^2][1][1]
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
        let modelFields = getTypeImpl(m)[0].getTypeImpl[1]
        for field in getImpl(m)[2][0][2]:
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
          ident($(m.getImpl[0][1]))
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
              ident($(m.getImpl[0][1])),
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
          ident($(m.getImpl[0][1]))
        ),
        ident"dbcon",
        newCall(bindSym"SqlQuery", newLit($parsedSql)),
        newLit(colNames),
        newLit(nParams)
      )
      appendSqlArgs(result, args)
  except SqlParseError as e:
    error("SQL parsing error: " & e.msg, sql[1][^1][1])

#
# Public API for finalizing and 
# executing SQL queries
#
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
          calledMacro notin ["ozarkWhereResult", "ozarkRawSQLResult",
                                "ozarkWhereInResult", "ozarkLimitResult"]:
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
  if calledMacro != "ozarkRawSQLResult":
    error("The first argument to `getWith` must be the result of a `rawSQL` macro. Got " & calledMacro, sql)

  try:
    let parsedSql = parseSQL(sql[1][1][1].strVal)
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
    # echo result.repr
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
    let sqlNode = parseSQL(sql)
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
    let sqlNode = parseSQL($sql[1])
    case sqlNode.sons[0].kind
    of nkInsert, nkDelete:
      let randId = genSym(nskVar, "id")
      let stub = staticRead("private" / "stubs" / "execSql.nim")
      result = macros.parseStmt(stub % [
              $sql[1],
              (
                if sql[2][1].len > 0: 
                  ", " & 
                  $sql[2][1].mapIt(it.repr).join(",")
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
                  $sql[2][1][1].mapIt(it.repr).join(",")
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

proc `$`*(sql: SqlQuery): string = string(sql)