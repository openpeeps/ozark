# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

## This module implements macro-based SQL query generation for Ozark. It can be used to construct
## complex SQL queries in a type-safe manner, with compile-time checks for model and column existence.

import std/[macros, macrocache, strutils, options,
      sequtils, tables, os, random, strformat]

import pkg/openparser/sql
import pkg/db_connector/db_common

import ./model, ./collection, ./driver/dbmeta
import ./driver/private/types

export mapIt

type
  OzarkModelDefect* = object of CatchableError
    ## Exception type for errors related to model definitions,
    ## such as referencing an unknown model or column in a query

const preparedQueryStatements = CacheTable"preparedQueryStatements"

randomize() # initialize random seed for generating unique statement names in `tryInsertID`

template table*(models: ptr ModelsTable, name): untyped = 
  ## Define SQL statement for a table
  (bindSym($name), sqlDriver)

template withTableCheck*(modelTuple: NimNode, body) =
  ## Check if a model with the given name exists in the Models table.
  if not StaticSchemas.hasKey(getTableName($modelTuple[1][0])):
    raise newException(OzarkModelDefect,
        "Unknown model `" & $modelTuple[1][0][1] & "`")
  body

template withColumnsCheck*(model: NimNode, cols: openArray[string], body) =
  ## Check if the specified columns exist in the model definition
  for col in cols:
    withColumnCheck(model, col):
      discard
  body

proc ozarkSelectResult*(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereResult*(sql: static[string], val: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereInResult*(sql: static[string], vals: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkRawSQLResult*(sql: static[string], vals: varargs[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkInsertResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkUpdateResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
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

template withColumnCheck*(model: NimNode, col: string, body) =
  ## Check if the specified column exists in the model definition
  ## 
  ## This template is used by the query macros to perform compile-time checks
  ## for the existence of columns in the specified model. It ensures that any column
  ## referenced in a query actually exists in the model definition.
  if col == "*":
    body # allow all columns, no need to check for existence
  elif not col.validIdentifier:
    raise newException(OzarkModelDefect, "Invalid column name `" & col & "`")
  else:
    let x = model.getImpl
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
      error("Column `" & col & "` does not exist in model `" & $model & "`.")

template withColumn*(x: NimNode, col: string, body) =
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

proc getPlaceholder*(modelTuple: NimNode, idx: int = 1): string =
  ## Get the appropriate parameter placeholder for the specified SQL driver.
  ## This is used in the `insert` and `update` macros to generate the correct
  ## SQL syntax for parameter placeholders based on the configured driver.
  case modelTuple.getDriverType()
  of SqlDriver.pgsql:
    result = "$" & $idx
  else:
    result = "?"

macro prepareTable*(modelTuple): untyped =
  ## Compile-time macro to prepare a model's table in the database.
  ## 
  ## This macro generates the SQL string for creating the table based
  ## on the model definition and executes it at compile time to ensure
  ## the table exists before any queries are made against it.
  withTableCheck(modelTuple):
    let tableName = getTableName($modelTuple[1][0])
    let driverType = modelTuple.getDriverType()
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
        colDef &= (
          if t[1].strVal == "serial" and driverType == SqlDriver.sqlite:
            $(DataType.Integer)
          else:
            t[1].strVal
        )
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

macro dropTable*(modelTuple: untyped, cascade: static bool = false): untyped =
  ## Compile-time macro to drop a model's table from the database.
  ## 
  ## Cascade option is included for compatibility with databases that support it, but will be
  ## ignored for SQLite since it does not support CASCADE with DROP TABLE
  withTableCheck(modelTuple):
    let tableName = getTableName($modelTuple[1][0])
    result = newCall(
      bindSym"ozarkRawSQLResult",
      newLit(
        if modelTuple.getDriverType() == SqlDriver.sqlite:
          "DROP TABLE IF EXISTS " & tableName
        else:
          "DROP TABLE IF EXISTS " & tableName & (if cascade: " CASCADE" else: "")
      )
    )

#
# INSERT and UDATE clause macros 
#
macro insert*(modelTuple, data: untyped): untyped =
  ## Placeholder for an `INSERT` statement. This macro generates the SQL string for the
  ## INSERT statement. This macro performs compile-time checks for the existence
  ## of the specified model and the validity of the column names.
  withTableCheck modelTuple:
    expectKind(data, nnkTableConstr)
    var cols: seq[string]
    var values = newNimNode(nnkBracket)
    var valuesIds: seq[int]
    var idx = 1

    for kv in data:
      let col = $kv[0]
      withColumnCheck(modelTuple[1][0], col):
        cols.add(col)
        values.add(kv[1])
        valuesIds.add(idx)
        inc idx
    result =
      newCall(
        bindSym"ozarkInsertResult",
        newLit("insert into " & getTableName($modelTuple[1][0]) &
                " (" & cols.join(",") & ") VALUES (" & valuesIds.mapIt(getPlaceholder(modelTuple, it)).join(",") & ")"),
        nnkPrefix.newTree(ident"@", values),
    )

macro removeRow*(modelTuple: untyped): untyped =
  ## Placeholder for a `DELETE` statement. This macro generates the SQL string for the
  ## DELETE statement. This macro performs compile-time checks for the existence
  ## of the specified model.
  let tableName = getTableName($modelTuple[1][0])
  let blockIdent = genSym(nskLabel, "ozarkBlock" & tableName)
  withTableCheck modelTuple:
    result = nnkBlockStmt.newTree(
      blockIdent,
      newStmtList(
        newCall(bindSym"ozarkHoldModel", modelTuple),
        newCall(
          bindSym"ozarkRemoveResult",
          newLit("DELETE FROM " & tableName)
        )
      )
    )

macro update*(modelTuple, data: untyped): untyped =
  ## Placeholder for an `UPDATE` statement. This macro generates the SQL string for the
  ## UPDATE statement. This macro performs compile-time checks for the existence
  ## of the specified model and the validity of the column names.
  withTableCheck modelTuple:
    expectKind(data, nnkTableConstr)
    var idx = 1
    var setClauses: seq[string]
    var values = newNimNode(nnkBracket)
    # var valuesIds: seq[int]
    for kv in data:
      let col = $kv[0]
      withColumnCheck(modelTuple[1][0], col):
        setClauses.add(col & " = " & getPlaceholder(modelTuple, idx))
        values.add(kv[1])
        # valuesIds.add(idx)
        inc idx
    let dbTable = getTableName($modelTuple[1][0])
    let blockIdent = genSym(nskLabel, "ozarkBlock" & dbTable)
    result = nnkBlockStmt.newTree(
      blockIdent,
      newStmtList(
        newCall(bindSym"ozarkHoldModel", modelTuple),
        newCall(
          bindSym"ozarkUpdateResult",
          newLit("update " & dbTable & " set " & setClauses.join(", ")),
          nnkPrefix.newTree(ident"@", values)
        )
      )
    )

#
# SELECT clause macros
#
macro select*(modelTuple: untyped, cols: static openArray[string]): untyped =
  ## Define `SELECT` clause with specific columns.
  withTableCheck modelTuple:
    withColumnsCheck modelTuple[1][0], cols:
      let dbTable = getTableName($modelTuple[1][0])
      let blockIdent = genSym(nskLabel, "ozarkBlock" & dbTable)
      result = nnkBlockStmt.newTree(
        blockIdent,
        newStmtList(
          newCall(bindSym"ozarkHoldModel", modelTuple),
          newCall(bindSym"ozarkSelectResult",
            newLit("SELECT " & cols.join(",") & " FROM " & dbTable)
          )
        )
      )


macro select*(modelTuple: untyped, col: static string): untyped =
  ## Define SELECT clause
  withTableCheck modelTuple:
    withColumnCheck modelTuple[1][0], col:
      let dbTable = getTableName($modelTuple[1][0])
      let blockIdent = genSym(nskLabel, "ozarkBlock" & "_" & dbTable)
      result = nnkBlockStmt.newTree(
        blockIdent,
        newStmtList(
          newCall(bindSym"ozarkHoldModel", modelTuple),
          newCall(bindSym"ozarkSelectResult",
            newLit("SELECT " & col & " FROM " & dbTable)
          )
        )
      )

macro selectAll*(modelTuple: untyped): untyped =
  ## Define SELECT * clause
  withTableCheck modelTuple:
    let dbTable = getTableName($modelTuple[1][0])
    let blockIdent = genSym(nskLabel, "ozarkBlock" & dbTable)
    result = nnkBlockStmt.newTree(
      blockIdent,
      newStmtList(
        newCall(bindSym"ozarkHoldModel", modelTuple),
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
  let modelSym = sql[1][^2][1][1][0]
  let modelTuple = sql[1][^2][1]
  withColumnCheck(modelSym, col):
    let selectSql = sql[1][^1][1].strVal
    sql[1][^1][0] = bindSym"ozarkWhereResult"
    sql[1][^1][1].strVal = sql[1][^1][1].strVal & " WHERE " & col & " " & op & " " & getPlaceholder(modelTuple)
    sql[1][^1].add(infix)
    result = sql

proc writeWhereInWhereNotIn(op: static string,
                    sql: NimNode, col: string, vals: NimNode): NimNode {.compileTime.} =
  # Writer macro for both `whereIn` and `whereNotIn` to avoid code duplication.
  # This macro generates the SQL string for the WHERE IN/NOT IN clause and
  # also adds the values as additional arguments to the macro result for later use in code generation
  if sql.kind != nnkBlockExpr or sql[1][^1][0].strVal notin ["ozarkSelectResult", "ozarkWhereResult"]:
    error("The first argument to `WHERE` clause must be the result of a `select` macro.")
  let modelSym = sql[1][^2][1][1][0]
  let modelTuple = sql[1][^2][1]
  withColumnCheck(modelSym, col):
    let calledMacro = sql[1][^1][0].strVal
    var placeholders = newSeq[string](vals.len)
    let ozarkOrmVar = genSym(nskLet, "OzarkORMInValuesPlaceholder")
    let ozarkOrmCountArgs = genSym(nskLet, "OzarkORMInValuesCount")
    for i in 0..<placeholders.len:
      placeholders[i] = getPlaceholder(modelTuple, i + 1)
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
          " AND " & col & " " & op & " (" & placeholders.join(", ") & ")"
        else:
        " WHERE " & col & " " & op & " (" & placeholders.join(", ") & ")"
      )
    result = sql

proc writeWhereStatement(op: static string, sql: NimNode,
                        col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for simple WHERE clauses (e.g. `where`, `whereNot`) to avoid code duplication.
  if sql.kind != nnkBlockExpr or sql[1][^1][0].strVal notin ["ozarkSelectResult", "ozarkWhereResult", "ozarkUpdateResult", "ozarkRemoveResult"]:
    error("The first argument to `WHERE` must be the result of a `select` macro. Got " & $sql[1][^1][0], sql)
  let modelSym = sql[1][^2][1][1][0]
  let modelTuple = sql[1][^2][1]
  if sql[1][^1][0].strVal in ["ozarkWhereResult"]:
    # if it's already a where result, we need to append to the existing
    # SQL string and add the new value as an additional argument
    withColumnCheck(modelSym, col):
      sql[1][^1][0] = bindSym"ozarkWhereResult"
      let length = sql[1][^1][2][1].len + 1
      sql[1][^1][1].strVal = sql[1][^1][1].strVal & " AND " & col & " " & op & " " & getPlaceholder(modelTuple, length)
      sql[1][^1][^1][1].add(val) # add to the current varargs list
  elif sql[1][^1][0].strVal == "ozarkUpdateResult":
    # if it's an update result, we need to append to the existing
    # SQL string and add the new value as an additional argument
    withColumnCheck(modelSym, col):
      sql[1][^1][0] = bindSym"ozarkWhereResult"
      let length = sql[1][^1][2][1].len + 1
      sql[1][^1][1].strVal = sql[1][^1][1].strVal & " WHERE " & col & " " & op & " " & getPlaceholder(modelTuple, length)
      sql[1][^1][2][1].add(val) # add to the current varargs list
  else:
    withColumnCheck(modelSym, col):
      sql[1][^1][0] = bindSym"ozarkWhereResult"
      sql[1][^1][1].strVal = sql[1][^1][1].strVal & " WHERE " & col & " " & op & " " & getPlaceholder(modelTuple)
      sql[1][^1].add(val)
  result = sql

proc writeOrWhereStatement(op: static string,
      sql: NimNode, col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for `orWhere` to avoid code duplication with `writeWhereStatement`.
  # This macro checks that the first argument is a valid `where` result and then
  # appends the new condition with an OR to the existing SQL string.
  if sql.kind != nnkBlockExpr or sql[1][^1][0].strVal != "ozarkWhereResult":
    error("The first argument to `orWhere` must be the result of a `where` macro.")
  let modelSym = sql[1][^2][1][1][0]
  let modelTuple = sql[1][^2][1]
  withColumnCheck(modelSym, col):
    let length = sql[1][^1][2][1].len + 1 # calculate the new param index based on existing params
    sql[1][^1][1].strVal = sql[1][^1][1].strVal & " OR " & col & " " & op & " " & getPlaceholder(modelTuple, length)
    sql[1][^1][2][1].add(val)
    result = sql

template getSqlImpl() {.dirty.} = 
  var sql = sql
  if sql.kind == nnkSym:
    sql = sql.getImpl()[2] # handle the case where the macro is called with a symbol instead of a block (e.g. `where x = 5` instead of `where(select(...), x = 5)`)

# WHERE clause public macros
macro where*(sql: untyped, col: static string, vals: untyped): untyped =
  ## Define `WHERE` clause
  writeWhereStatement("=", sql, col, vals)

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

proc `$`*(sql: SqlQuery): string = string(sql)