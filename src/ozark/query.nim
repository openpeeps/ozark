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
proc ozarkWhereResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkWhereInResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkRawSQLResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkInsertResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkUpdateResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkOrderByResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkCreateTableResult*(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkRemoveResult*(sql: static[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkOffsetResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkGroupByResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkLimitResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkHavingResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)
proc ozarkUpsertResult*(sql: static[string], vals: seq[string]): NimNode {.compileTime.} = newLit(sql)

type
  Order* = enum
    ## Sort direction used by `orderBy` and friends
    Asc, Desc

#
# Query stage helpers
#
# Clause macros chain by rewriting the trailing `ozark*Result` call node of a
# block expression. These helpers centralize how that node is located, how its
# accumulated bound-params bracket is accessed, and which callees may precede
# a given clause.
#

const selectFamilyResults* = [
  "ozarkSelectResult", "ozarkWhereResult", "ozarkWhereInResult",
  "ozarkRawSQLResult", "ozarkLimitResult", "ozarkOrderByResult",
  "ozarkOffsetResult", "ozarkGroupByResult", "ozarkHavingResult"
]

proc trailingCallOf*(sql: NimNode): NimNode =
  ## Normalize a chained query expression (block stmt/expr or bare call)
  ## to its trailing `ozark*Result` call node.
  case sql.kind
  of nnkBlockExpr, nnkBlockStmt:
    result = sql[1][^1]
  of nnkCall:
    result = sql
  else:
    error("Invalid query expression.", sql)

proc resultKindOf*(callNode: NimNode): string =
  ## The callee name of a trailing result call (e.g. "ozarkWhereResult")
  if callNode.kind == nnkCall and callNode.len > 0 and
      callNode[0].kind in {nnkIdent, nnkSym}:
    result = callNode[0].strVal
  else:
    result = ""

proc ensureStage*(sql: NimNode, allowed: openArray[string], caller: string) =
  ## Compile-time guard: the query must currently be in one of `allowed` stages
  let kind = resultKindOf(trailingCallOf(sql))
  if kind notin allowed:
    error("`" & caller & "` cannot follow `" & kind &
      "`. Allowed predecessors: " & allowed.join(", "), sql)

proc modelTupleOf*(sql: NimNode): NimNode =
  ## Extract the `(bindSym(Model), sqlDriver)` tuple carried inside the block
  result = sql[1][^2][1]

proc modelSymOf*(sql: NimNode): NimNode =
  ## Extract the model type symbol from a chained query block
  result = modelTupleOf(sql)[1][0]

proc paramsBracketOf*(trailing: NimNode): NimNode =
  ## Return a mutable bracket node holding all bound params accumulated so far,
  ## regardless of the internal shape produced by earlier writers
  ## (`@[...]` prefix, compiler-folded varargs, bare single value, or none).
  if trailing.len >= 3:
    var carrier = trailing[2]
    while carrier.kind in {nnkHiddenStdConv, nnkHiddenSubConv} and carrier.len > 0:
      carrier = carrier[^1]
    case carrier.kind
    of nnkPrefix:
      if carrier.len > 1 and carrier[1].kind == nnkBracket:
        result = carrier[1]
      else:
        result = nnkBracket.newTree(carrier)
    of nnkBracket:
      result = carrier
    else:
      result = nnkBracket.newTree(trailing[2])
  else:
    result = nnkBracket.newTree()

proc rebuildResultCall*(kindSym: NimNode, sqlText: string, bracket: NimNode): NimNode =
  ## Standardized result call: (kindSym)(sqlText, @[params...])
  result = newCall(kindSym, newLit(sqlText))
  result.add(nnkPrefix.newTree(ident"@", bracket))

proc normalizeInLists*(sqlText: string): string =
  ## Collapse multi-placeholder groups — `(?, ?, ?)` or `($1, $2)` — into a
  ## single placeholder so the compile-time SQL validator (which only accepts
  ## one expression per parenthesized group) can validate statements with
  ## multi-value `IN (...)` lists. The returned text is used for validation
  ## and column extraction only; the original text is what gets executed.
  result = newStringOfCap(sqlText.len)
  var i = 0
  while i < sqlText.len:
    let c = sqlText[i]
    if c == '(' and i + 1 < sqlText.len and (sqlText[i + 1] == '?' or sqlText[i + 1] == '$'):
      var j = i + 1
      var phCount = 0
      var onlyPlaceholders = true
      while j < sqlText.len and sqlText[j] != ')':
        let d = sqlText[j]
        if d == '?':
          inc phCount
        elif d == '$':
          inc phCount
          while j + 1 < sqlText.len and sqlText[j + 1] in {'0'..'9'}:
            inc j
        elif d in {',', ' ', '\t', '\n', '\r'}:
          discard
        else:
          onlyPlaceholders = false
          break
        inc j
      if onlyPlaceholders and phCount >= 2 and j < sqlText.len:
        result.add("(?)")
        i = j + 1
        continue
    result.add(c)
    inc i

proc swapResultCall*(trailing: NimNode, kindSym: NimNode, newText: string,
                     bracket: NimNode) =
  ## In-place rewrite of an existing trailing call to a new stage while
  ## preserving every accumulated parameter
  trailing[0] = kindSym
  trailing[1] = newLit(newText)
  if trailing.len >= 3:
    trailing[2] = nnkPrefix.newTree(ident"@", bracket)
  else:
    trailing.add(nnkPrefix.newTree(ident"@", bracket))

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
          "DROP TABLE IF EXISTS " & tableName & (if cascade: " CASCADE" else: "")),
      nnkPrefix.newTree(ident"@", nnkBracket.newTree())
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

proc unbracket*(n: NimNode): NimNode =
  ## Normalizes an array-ish argument (`@["a"]`, `["a"]`, compiler-folded
  ## varargs, or a single value) to a plain bracket node
  case n.kind
  of nnkPrefix, nnkHiddenStdConv, nnkCommand:
    if n.len > 0 and n[^1].kind == nnkBracket:
      result = n[^1]
    else:
      result = nnkBracket.newTree(n)
  of nnkBracket:
    result = n
  else:
    result = nnkBracket.newTree(n)

# - WHERE caluse Writers
proc writeWhereLikeStatements(op: static string, connector: static string, sql: NimNode,
                      infix: NimNode, col: string): NimNode {.compileTime.} =
  # Writer macro for the LIKE/NOT LIKE family to avoid code duplication.
  # This generates the SQL string for the LIKE/NOT LIKE clause and
  # constructs the appropriate infix expression for the value with wildcards.
  let kind = resultKindOf(trailingCallOf(sql))
  if kind notin ["ozarkSelectResult", "ozarkWhereResult", "ozarkWhereInResult", "ozarkUpdateResult"]:
    error("The first argument to a `LIKE` clause must be the result of a `select`, `where` or `update` macro. Got " & kind, sql)
  let modelSym = modelSymOf(sql)
  let modelTuple = modelTupleOf(sql)
  let trailing = trailingCallOf(sql)
  withColumnCheck(modelSym, col):
    case kind
    of "ozarkSelectResult":
      var bracket = nnkBracket.newTree(infix)
      swapResultCall(trailing, bindSym"ozarkWhereResult",
        trailing[1].strVal & " WHERE " & col & " " & op & " " & getPlaceholder(modelTuple),
        bracket)
    else:
      var bracket = paramsBracketOf(trailing)
      bracket.add(infix)
      let sep =
        if kind == "ozarkUpdateResult": " WHERE "
        elif kind == "ozarkWhereInResult": " AND "
        else: " " & connector & " "
      swapResultCall(trailing, bindSym"ozarkWhereResult",
        trailing[1].strVal & sep & col & " " & op & " " & getPlaceholder(modelTuple, bracket.len),
        bracket)
    result = sql

proc writeWhereInWhereNotIn(op: static string, connector: static string,
                    sql: NimNode, col: string, vals: NimNode): NimNode {.compileTime.} =
  # Writer macro for the IN/NOT IN family to avoid code duplication.
  # Generates the SQL for the IN clause and flattens every value into the
  # accumulated params bracket so subsequent placeholders index correctly.
  let kind = resultKindOf(trailingCallOf(sql))
  if kind notin ["ozarkSelectResult", "ozarkWhereResult", "ozarkWhereInResult"]:
    error("The first argument to an `IN` clause must be the result of a `select` or `where` macro. Got " & kind, sql)
  let modelSym = modelSymOf(sql)
  let modelTuple = modelTupleOf(sql)
  let trailing = trailingCallOf(sql)
  withColumnCheck(modelSym, col):
    let items = unbracket(vals)
    var bracket = paramsBracketOf(trailing)
    let baseIdx = bracket.len
    var placeholders = newSeq[string](items.len)
    for i in 0..<items.len:
      placeholders[i] = getPlaceholder(modelTuple, baseIdx + i + 1)
      bracket.add(items[i])
    let sep =
      if kind == "ozarkSelectResult": " WHERE "
      elif kind == "ozarkWhereResult": " " & connector & " "
      else: " AND "
    swapResultCall(trailing, bindSym"ozarkWhereInResult",
      trailing[1].strVal & sep & col & " " & op & " (" & placeholders.join(", ") & ")",
      bracket)
    result = sql

proc writeWhereStatement(op: static string, sql: NimNode,
                        col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for simple WHERE clauses (e.g. `where`, `whereNot`) to avoid code duplication.
  if sql.kind != nnkBlockExpr or resultKindOf(trailingCallOf(sql)) notin ["ozarkSelectResult", "ozarkWhereResult", "ozarkUpdateResult", "ozarkRemoveResult"]:
    error("The first argument to `WHERE` must be the result of a `select` macro.", sql)
  let modelSym = modelSymOf(sql)
  let modelTuple = modelTupleOf(sql)
  let trailing = trailingCallOf(sql)
  withColumnCheck(modelSym, col):
    var bracket = paramsBracketOf(trailing)
    let sep =
      if resultKindOf(trailing) == "ozarkSelectResult":
        " WHERE "
      elif resultKindOf(trailing) == "ozarkWhereResult":
        " AND "
      else:
        # update/remove results start the WHERE section
        " WHERE "
    bracket.add(val)
    swapResultCall(trailing, bindSym"ozarkWhereResult",
      trailing[1].strVal & sep & col & " " & op & " " & getPlaceholder(modelTuple, bracket.len),
      bracket)
  result = sql

proc writeOrWhereStatement(op: static string,
      sql: NimNode, col: string, val: NimNode): NimNode {.compileTime.} =
  # Writer macro for `orWhere` to avoid code duplication with `writeWhereStatement`.
  # This macro checks that the first argument is a valid `where` result and then
  # appends the new condition with an OR to the existing SQL string.
  if sql.kind != nnkBlockExpr or resultKindOf(trailingCallOf(sql)) != "ozarkWhereResult":
    error("The first argument to `orWhere` must be the result of a `where` macro.")
  let modelSym = modelSymOf(sql)
  let modelTuple = modelTupleOf(sql)
  let trailing = trailingCallOf(sql)
  withColumnCheck(modelSym, col):
    var bracket = paramsBracketOf(trailing)
    bracket.add(val)
    swapResultCall(trailing, bindSym"ozarkWhereResult",
      trailing[1].strVal & " OR " & col & " " & op & " " & getPlaceholder(modelTuple, bracket.len),
      bracket)
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
  writeWhereLikeStatements("LIKE", "AND", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereEndsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `WHERE` clause with `LIKE` for suffix matching
  writeWhereLikeStatements("LIKE", "AND", sql,
    nnkInfix.newTree(ident"&", newLit("%"), val), col)

macro whereLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with `LIKE` for any position
  writeWhereLikeStatements("LIKE", "AND", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereNotLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define WHERE clause with `NOT LIKE` for any position
  writeWhereLikeStatements("NOT LIKE", "AND", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereNotStartsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `WHERE` clause with `NOT LIKE` for prefix matching
  writeWhereLikeStatements("NOT LIKE", "AND", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro whereNotEndsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `WHERE` clause with `NOT LIKE` for suffix matching
  writeWhereLikeStatements("NOT LIKE", "AND", sql,
    nnkInfix.newTree(ident"&", newLit("%"), val), col)

macro orWhereLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define an `OR LIKE` condition in `WHERE` clause
  writeWhereLikeStatements("LIKE", "OR", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro orWhereNotLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define an `OR NOT LIKE` condition in `WHERE` clause
  writeWhereLikeStatements("NOT LIKE", "OR", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro orWhereStartsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define an `OR` condition matching a prefix via `LIKE`
  writeWhereLikeStatements("LIKE", "OR", sql,
    nnkInfix.newTree(ident"&", val, newLit("%")), col)

macro orWhereEndsLike*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define an `OR` condition matching a suffix via `LIKE`
  writeWhereLikeStatements("LIKE", "OR", sql,
    nnkInfix.newTree(ident"&", newLit("%"), val), col)

macro whereIn*(sql: untyped, col: static string, vals: untyped): untyped =
  ## Define `WHERE` clause with `IN` operator
  writeWhereInWhereNotIn("IN", "AND", sql, col, vals)

macro whereNotIn*(sql: untyped, col: static string, vals: untyped): untyped =
  ## Define `WHERE` clause with `NOT IN` operator
  writeWhereInWhereNotIn("NOT IN", "AND", sql, col, vals)

macro orWhereIn*(sql: untyped, col: static string, vals: untyped): untyped =
  ## Define an `OR IN` condition in `WHERE` clause
  writeWhereInWhereNotIn("IN", "OR", sql, col, vals)

macro orWhereNotIn*(sql: untyped, col: static string, vals: untyped): untyped =
  ## Define an `OR NOT IN` condition in `WHERE` clause
  writeWhereInWhereNotIn("NOT IN", "OR", sql, col, vals)

proc `$`*(sql: SqlQuery): string = string(sql)

#
# Comparison WHERE clauses
#

template comparisonWhere(name: untyped, op: static string, writer: untyped) {.dirty.} =
  macro name*(sql: untyped, col: static string, val: untyped): untyped =
    ## Define a `WHERE` clause using an ordered comparison operator
    writer(op, sql, col, val)

comparisonWhere(whereGt, ">", writeWhereStatement)
comparisonWhere(whereGte, ">=", writeWhereStatement)
comparisonWhere(whereLt, "<", writeWhereStatement)
comparisonWhere(whereLte, "<=", writeWhereStatement)
comparisonWhere(orWhereGt, ">", writeOrWhereStatement)
comparisonWhere(orWhereGte, ">=", writeOrWhereStatement)
comparisonWhere(orWhereLt, "<", writeOrWhereStatement)
comparisonWhere(orWhereLte, "<=", writeOrWhereStatement)

#
# IS NULL / IS NOT NULL clauses
#

proc writeWhereNullStatement(nullSql: static string, sql: NimNode,
                             col: string): NimNode {.compileTime.} =
  # Writer for valueless NULL predicates. No bound parameters are added;
  # an empty params bracket is materialized so subsequent clauses keep
  # computing correct placeholder indexes.
  let kind = resultKindOf(trailingCallOf(sql))
  if kind notin ["ozarkSelectResult", "ozarkWhereResult", "ozarkWhereInResult", "ozarkUpdateResult"]:
    error("`IS NULL` clause cannot follow `" & kind & "`.", sql)
  let modelSym = modelSymOf(sql)
  let modelTuple = modelTupleOf(sql)
  let trailing = trailingCallOf(sql)
  withColumnCheck(modelSym, col):
    case kind
    of "ozarkSelectResult":
      swapResultCall(trailing, bindSym"ozarkWhereResult",
        trailing[1].strVal & " WHERE " & col & " " & nullSql,
        nnkBracket.newTree())
    else:
      var bracket = paramsBracketOf(trailing)
      let sep =
        if kind == "ozarkUpdateResult": " WHERE "
        elif kind == "ozarkWhereInResult": " AND "
        elif nullSql == "IS NULL": " AND "
        else: " AND "
      swapResultCall(trailing, bindSym"ozarkWhereResult",
        trailing[1].strVal & sep & col & " " & nullSql, bracket)
    result = sql

macro whereNull*(sql: untyped, col: static string): untyped =
  ## Define `WHERE col IS NULL`
  writeWhereNullStatement("IS NULL", sql, col)

macro whereNotNull*(sql: untyped, col: static string): untyped =
  ## Define `WHERE col IS NOT NULL`
  writeWhereNullStatement("IS NOT NULL", sql, col)

#
# BETWEEN clauses
#

proc writeWhereBetweenStatement(op: static string, connector: static string, sql: NimNode,
                                col: string, lo: NimNode, hi: NimNode): NimNode {.compileTime.} =
  let kind = resultKindOf(trailingCallOf(sql))
  if kind notin ["ozarkSelectResult", "ozarkWhereResult", "ozarkWhereInResult", "ozarkUpdateResult"]:
    error("`BETWEEN` clause cannot follow `" & kind & "`.", sql)
  let modelSym = modelSymOf(sql)
  let modelTuple = modelTupleOf(sql)
  let trailing = trailingCallOf(sql)
  withColumnCheck(modelSym, col):
    var bracket = paramsBracketOf(trailing)
    let baseIdx = bracket.len
    bracket.add(lo)
    bracket.add(hi)
    let phLo = getPlaceholder(modelTuple, baseIdx + 1)
    let phHi = getPlaceholder(modelTuple, baseIdx + 2)
    let sep =
      if kind == "ozarkSelectResult": " WHERE "
      elif kind == "ozarkUpdateResult": " WHERE "
      elif kind == "ozarkWhereInResult": " AND "
      else: " " & connector & " "
    swapResultCall(trailing, bindSym"ozarkWhereResult",
      trailing[1].strVal & sep & col & " " & op & " " & phLo & " AND " & phHi,
      bracket)
    result = sql

macro whereBetween*(sql: untyped, col: static string, lo: untyped, hi: untyped): untyped =
  ## Define `WHERE col BETWEEN lo AND hi`
  writeWhereBetweenStatement("BETWEEN", "AND", sql, col, lo, hi)

macro whereNotBetween*(sql: untyped, col: static string, lo: untyped, hi: untyped): untyped =
  ## Define `WHERE col NOT BETWEEN lo AND hi`
  writeWhereBetweenStatement("NOT BETWEEN", "AND", sql, col, lo, hi)

macro orWhereBetween*(sql: untyped, col: static string, lo: untyped, hi: untyped): untyped =
  ## Define an `OR BETWEEN` condition in `WHERE` clause
  writeWhereBetweenStatement("BETWEEN", "OR", sql, col, lo, hi)

#
# LIMIT / OFFSET / pagination
#
# Note: the compile-time SQL validator only accepts numeric literals in
# LIMIT/OFFSET positions, so these clauses take `static int` values that are
# inlined into the statement. For runtime-computed limits use `rawSQL`.
#

macro limit*(sql: untyped, count: static int): untyped =
  ## Define `LIMIT` clause with a constant row count.
  ## Must be called before `offset` (canonical SQL clause order).
  let kind = resultKindOf(trailingCallOf(sql))
  if kind notin selectFamilyResults or kind == "ozarkOffsetResult":
    error("`limit` cannot follow `" & kind & "`. Apply `limit` before `offset`.", sql)
  let trailing = trailingCallOf(sql)
  var bracket = paramsBracketOf(trailing)
  swapResultCall(trailing, bindSym"ozarkLimitResult",
    trailing[1].strVal & " LIMIT " & $count, bracket)
  result = sql

macro offset*(sql: untyped, count: static int): untyped =
  ## Define `OFFSET` clause with a constant row count.
  ensureStage(sql, selectFamilyResults, "offset")
  let trailing = trailingCallOf(sql)
  var bracket = paramsBracketOf(trailing)
  swapResultCall(trailing, bindSym"ozarkOffsetResult",
    trailing[1].strVal & " OFFSET " & $count, bracket)
  result = sql

macro paginate*(sql: untyped, page: static int, perPage: static int): untyped =
  ## Apply `LIMIT`/`OFFSET` pagination with constant values.
  ## `page` is 1-based.
  if page < 1:
    error("`paginate` requires a page number >= 1.", sql)
  ensureStage(sql, selectFamilyResults, "paginate")
  let trailing = trailingCallOf(sql)
  var bracket = paramsBracketOf(trailing)
  swapResultCall(trailing, bindSym"ozarkLimitResult",
    trailing[1].strVal & " LIMIT " & $perPage &
      " OFFSET " & $((page - 1) * perPage), bracket)
  result = sql

#
# ORDER BY clauses
#

proc writeOrderByStatement(dirSql: string, sql: NimNode,
                           cols: seq[string]): NimNode {.compileTime.} =
  let kind = resultKindOf(trailingCallOf(sql))
  if kind notin ["ozarkSelectResult", "ozarkWhereResult", "ozarkWhereInResult",
                 "ozarkRawSQLResult", "ozarkGroupByResult", "ozarkHavingResult"]:
    error("`ORDER BY` cannot follow `" & kind & "`.", sql)
  withColumnsCheck(modelSymOf(sql), cols):
    discard
  let trailing = trailingCallOf(sql)
  var bracket = paramsBracketOf(trailing)
  # ascending is expressed by omitting the direction keyword entirely:
  # the compile-time SQL parser only recognizes `DESC`
  var newText = trailing[1].strVal & " ORDER BY " & cols.join(", ")
  if dirSql.len > 0:
    newText &= " " & dirSql
  swapResultCall(trailing, bindSym"ozarkOrderByResult", newText, bracket)
  result = sql

macro orderBy*(sql: untyped, cols: static openArray[string], dir: static Order = Desc): untyped =
  ## Define `ORDER BY` with an explicit sort direction
  if dir == Asc:
    result = writeOrderByStatement("", sql, @cols)
  else:
    result = writeOrderByStatement("DESC", sql, @cols)

macro orderAscBy*(sql: untyped, cols: static openArray[string]): untyped =
  ## Define `ORDER BY ... ASC`
  writeOrderByStatement("", sql, @cols)

macro orderDescBy*(sql: untyped, cols: static openArray[string]): untyped =
  ## Define `ORDER BY ... DESC`
  writeOrderByStatement("DESC", sql, @cols)

#
# DISTINCT / GROUP BY / HAVING clauses
#

macro unique*(sql: untyped): untyped =
  ## Apply `SELECT DISTINCT` to the current query. Can only be applied
  ## directly to a `select`/`selectAll` stage.
  let trailing = trailingCallOf(sql)
  if resultKindOf(trailing) != "ozarkSelectResult":
    error("`unique` can only be applied directly to a `select`/`selectAll` query. Got " &
      resultKindOf(trailing), sql)
  if trailing[1].strVal.startsWith("SELECT DISTINCT"):
    error("`unique` has already been applied to this query.", sql)
  trailing[1].strVal = "SELECT DISTINCT " & trailing[1].strVal["SELECT ".len .. ^1]
  result = sql

macro groupBy*(sql: untyped, cols: static openArray[string]): untyped =
  ## Define `GROUP BY` clause
  ensureStage(sql, ["ozarkSelectResult", "ozarkWhereResult",
                    "ozarkWhereInResult", "ozarkRawSQLResult"], "groupBy")
  withColumnsCheck(modelSymOf(sql), cols):
    discard
  let trailing = trailingCallOf(sql)
  var bracket = paramsBracketOf(trailing)
  swapResultCall(trailing, bindSym"ozarkGroupByResult",
    trailing[1].strVal & " GROUP BY " & cols.join(", "), bracket)
  result = sql

proc writeHavingStatement(op: static string, connector: static string,
                          sql: NimNode, col: string, val: NimNode): NimNode {.compileTime.} =
  let kind = resultKindOf(trailingCallOf(sql))
  if kind notin ["ozarkGroupByResult", "ozarkHavingResult"]:
    error("`having` must follow `groupBy` (or another `having` clause). Got " & kind, sql)
  let modelSym = modelSymOf(sql)
  let modelTuple = modelTupleOf(sql)
  let trailing = trailingCallOf(sql)
  withColumnCheck(modelSym, col):
    var bracket = paramsBracketOf(trailing)
    bracket.add(val)
    let sep = if kind == "ozarkHavingResult": " " & connector & " " else: " HAVING "
    swapResultCall(trailing, bindSym"ozarkHavingResult",
      trailing[1].strVal & sep & col & " " & op & " " & getPlaceholder(modelTuple, bracket.len),
      bracket)
    result = sql

macro having*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `HAVING col = val` (must follow `groupBy`)
  writeHavingStatement("=", "AND", sql, col, val)

macro havingGt*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `HAVING col > val`
  writeHavingStatement(">", "AND", sql, col, val)

macro havingGte*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `HAVING col >= val`
  writeHavingStatement(">=", "AND", sql, col, val)

macro havingLt*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `HAVING col < val`
  writeHavingStatement("<", "AND", sql, col, val)

macro havingLte*(sql: untyped, col: static string, val: untyped): untyped =
  ## Define `HAVING col <= val`
  writeHavingStatement("<=", "AND", sql, col, val)

#
# Aggregates
#
# Aggregates execute immediately and wrap the current query as a derived
# table: `SELECT <fn> FROM (<inner query>) AS ozark_agg`. Bound parameters
# accumulated by earlier clauses are carried over untouched.
#

proc emitScalarExecCall(fnText: string, sql: NimNode): NimNode {.compileTime.} =
  let trailing = trailingCallOf(sql)
  var bracket = paramsBracketOf(trailing)
  let wrapped = "SELECT " & fnText & " FROM (" & trailing[1].strVal & ") AS ozark_agg"
  var call = newCall(ident"execScalar", ident"dbcon",
                     newCall(bindSym"SqlQuery", newLit(wrapped)),
                     newLit(bracket.len))
  for v in bracket:
    call.add(v)
  result = call

macro count*(sql: untyped): untyped =
  ## Execute `SELECT COUNT(*)` over the current query and return the row
  ## count as `int64`.
  ensureStage(sql, selectFamilyResults, "count")
  let aggVar = genSym(nskLet, "ozarkAggValue")
  result = newStmtList(
    nnkLetSection.newTree(
      nnkIdentDefs.newTree(aggVar, newEmptyNode(),
        emitScalarExecCall("COUNT(*)", sql))),
    nnkIfExpr.newTree(
      nnkElifBranch.newTree(
        newCall(ident">", newDotExpr(aggVar, ident"len"), newLit(0)),
        newCall(bindSym"parseBiggestInt", aggVar)),
      nnkElseExpr.newTree(newLit(0'i64))))

macro countDistinct*(sql: untyped, col: static string): untyped =
  ## Execute `SELECT COUNT(DISTINCT col)` over the current query.
  ensureStage(sql, selectFamilyResults, "countDistinct")
  withColumnCheck(modelSymOf(sql), col):
    discard
  result = emitScalarExecCall("COUNT(DISTINCT " & col & ")", sql)

macro sumOf*(sql: untyped, col: static string): untyped =
  ## Execute `SELECT SUM(col)` over the current query. Returns the raw
  ## string value reported by the database driver.
  ensureStage(sql, selectFamilyResults, "sumOf")
  withColumnCheck(modelSymOf(sql), col):
    discard
  result = emitScalarExecCall("SUM(" & col & ")", sql)

macro avgOf*(sql: untyped, col: static string): untyped =
  ## Execute `SELECT AVG(col)` over the current query.
  ensureStage(sql, selectFamilyResults, "avgOf")
  withColumnCheck(modelSymOf(sql), col):
    discard
  result = emitScalarExecCall("AVG(" & col & ")", sql)

macro minOf*(sql: untyped, col: static string): untyped =
  ## Execute `SELECT MIN(col)` over the current query.
  ensureStage(sql, selectFamilyResults, "minOf")
  withColumnCheck(modelSymOf(sql), col):
    discard
  result = emitScalarExecCall("MIN(" & col & ")", sql)

macro maxOf*(sql: untyped, col: static string): untyped =
  ## Execute `SELECT MAX(col)` over the current query.
  ensureStage(sql, selectFamilyResults, "maxOf")
  withColumnCheck(modelSymOf(sql), col):
    discard
  result = emitScalarExecCall("MAX(" & col & ")", sql)

#
# Convenience selectors
#

macro findById*(modelTuple: untyped, id: untyped): untyped =
  ## Shorthand for fetching a single row by its `id` column.
  ## Finish the chain with `get()`.
  ## (Named `findById` because `system.find` shadows a bare `find`.)
  withTableCheck(modelTuple):
    let dbTable = getTableName($modelTuple[1][0])
    result = nnkBlockStmt.newTree(
      genSym(nskLabel, "ozarkBlockFind" & dbTable),
      newStmtList(
        newCall(bindSym"ozarkHoldModel", modelTuple),
        rebuildResultCall(bindSym"ozarkWhereResult",
          "SELECT * FROM " & dbTable & " WHERE id = " & getPlaceholder(modelTuple, 1),
          nnkBracket.newTree(id))))

macro first*(sql: untyped): untyped =
  ## Alias for `get()`: fetch a single row from the chained query.
  result = newCall(ident"get", sql)

#
# WRITE helpers
#

macro insertMany*(modelTuple: untyped, rows: untyped): untyped =
  ## Insert multiple rows with a single `INSERT` statement. All rows must
  ## declare the exact same set of columns, validated at compile time.
  withTableCheck modelTuple:
    var rowNodes: seq[NimNode]
    case rows.kind
    of nnkPrefix, nnkHiddenStdConv:
      for r in rows[1]: rowNodes.add(r)
    of nnkBracket:
      for r in rows: rowNodes.add(r)
    else:
      error("`insertMany` expects an array of `{col: value}` tables.", rows)
    if rowNodes.len == 0:
      error("`insertMany` requires at least one row.", rows)

    var cols: seq[string]
    var values = newNimNode(nnkBracket)
    var rowTuples: seq[string]
    var idx = 1
    for i, row in rowNodes:
      expectKind(row, nnkTableConstr)
      if i == 0:
        for kv in row:
          let col = $kv[0]
          withColumnCheck(modelTuple[1][0], col):
            cols.add(col)
      else:
        var rowCols: seq[string]
        for kv in row:
          rowCols.add($kv[0])
        if rowCols != cols:
          error("All rows passed to `insertMany` must share the same columns. Expected [" &
            cols.join(",") & "] got [" & rowCols.join(",") & "]", row)
      var rowPhs: seq[string]
      for kv in row:
        rowPhs.add(getPlaceholder(modelTuple, idx))
        values.add(kv[1])
        inc idx
      rowTuples.add("(" & rowPhs.join(", ") & ")")
    result =
      newCall(
        bindSym"ozarkInsertResult",
        newLit("insert into " & getTableName($modelTuple[1][0]) &
                " (" & cols.join(",") & ") VALUES " & rowTuples.join(", ")),
        nnkPrefix.newTree(ident"@", values))

macro upsert*(modelTuple: untyped, data: untyped,
              conflictCols: static openArray[string],
              updateCols: static openArray[string]): untyped =
  ## Insert a row, resolving conflicts on `conflictCols`. When a conflict
  ## occurs, `updateCols` are overwritten with the proposed values
  ## (`DO UPDATE SET c = EXCLUDED.c`). Pass an empty `updateCols` to emit
  ## `DO NOTHING`. Uses the portable `ON CONFLICT` syntax (PostgreSQL and
  ## SQLite 3.24+).
  withTableCheck modelTuple:
    expectKind(data, nnkTableConstr)
    var cols: seq[string]
    var values = newNimNode(nnkBracket)
    var valueIds: seq[int]
    var idx = 1
    for kv in data:
      let col = $kv[0]
      withColumnCheck(modelTuple[1][0], col):
        cols.add(col)
        values.add(kv[1])
        valueIds.add(idx)
        inc idx
    for col in conflictCols:
      withColumnCheck(modelTuple[1][0], col):
        discard
    for col in updateCols:
      withColumnCheck(modelTuple[1][0], col):
        discard

    var sqlTxt = "insert into " & getTableName($modelTuple[1][0]) &
      " (" & cols.join(",") & ") VALUES (" &
      valueIds.mapIt(getPlaceholder(modelTuple, it)).join(",") & ")"
    if conflictCols.len > 0:
      sqlTxt &= " ON CONFLICT (" & conflictCols.join(", ") & ")"
      if updateCols.len > 0:
        var setPairs: seq[string]
        for col in updateCols:
          setPairs.add(col & " = EXCLUDED." & col)
        sqlTxt &= " DO UPDATE SET " & setPairs.join(", ")
      else:
        sqlTxt &= " DO NOTHING"
    result =
      newCall(
        bindSym"ozarkUpsertResult",
        newLit(sqlTxt),
        nnkPrefix.newTree(ident"@", values))

macro increment*(modelTuple: untyped, col: static string, n: static int = 1): untyped =
  ## Emit `UPDATE table SET col = col + n`. Chainable with `.where().exec()`.
  withTableCheck modelTuple:
    withColumnCheck(modelTuple[1][0], col):
      discard
    let dbTable = getTableName($modelTuple[1][0])
    result = nnkBlockStmt.newTree(
      genSym(nskLabel, "ozarkBlockIncrement" & dbTable),
      newStmtList(
        newCall(bindSym"ozarkHoldModel", modelTuple),
        rebuildResultCall(bindSym"ozarkUpdateResult",
          "update " & dbTable & " set " & col & " = " & col & " + " & $n,
          nnkBracket.newTree())))

macro decrement*(modelTuple: untyped, col: static string, n: static int = 1): untyped =
  ## Emit `UPDATE table SET col = col - n`. Chainable with `.where().exec()`.
  withTableCheck modelTuple:
    withColumnCheck(modelTuple[1][0], col):
      discard
    let dbTable = getTableName($modelTuple[1][0])
    result = nnkBlockStmt.newTree(
      genSym(nskLabel, "ozarkBlockDecrement" & dbTable),
      newStmtList(
        newCall(bindSym"ozarkHoldModel", modelTuple),
        rebuildResultCall(bindSym"ozarkUpdateResult",
          "update " & dbTable & " set " & col & " = " & col & " - " & $n,
          nnkBracket.newTree())))

macro truncate*(modelTuple: untyped): untyped =
  ## Remove all rows from the model's table. Emits `DELETE FROM` so the
  ## behavior stays portable across drivers (no `TRUNCATE` support yet).
  withTableCheck modelTuple:
    let dbTable = getTableName($modelTuple[1][0])
    result = nnkBlockStmt.newTree(
      genSym(nskLabel, "ozarkBlockTruncate" & dbTable),
      newStmtList(
        newCall(bindSym"ozarkHoldModel", modelTuple),
        newCall(bindSym"ozarkRemoveResult",
          newLit("DELETE FROM " & dbTable))))

#
# Transactions
#

var ozarkTxDepth* {.threadvar.}: int
  ## Per-thread nesting depth used by `withTransaction`

macro withTransaction*(body: untyped): untyped =
  ## Run `body` inside a database transaction on a dedicated pooled
  ## connection. Commits on normal completion; rolls back when the body
  ## raises (secondary rollback errors are swallowed, the original
  ## exception is re-raised). The connection is always returned to the pool.
  ##
  ## Query chains inside the block use the injected `dbcon` automatically:
  ##
  ##   withTransaction do:
  ##     Models.table(Orders).insert({...}).exec()
  ##     Models.table(Audit).insert({...}).exec()
  ##
  ## Nested calls raise `OzarkConnectionError`.
  let getInstanceSym = ident"getInstance"
  let acquireConnSym = ident"acquireConn"
  let releaseConnSym = ident"releaseConn"
  let execSym = ident"exec"
  let errSym = ident"OzarkConnectionError"
  let dbErrSym = bindSym"DbError"
  result = quote do:
    block:
      if ozarkTxDepth > 0:
        raise newException(`errSym`, "Nested transactions are not supported")
      let db = `getInstanceSym`()
      assert db != nil, "Database manager not initialized. Call initOzarkDatabase first."
      assert db[].mainPool != nil,
        "DB pool not initialized. Call initOzarkPool first."
      let dbcon {.inject.} = `acquireConnSym`(db[].mainPool)
      inc ozarkTxDepth
      var committed = false
      try:
        `execSym`(dbcon, SqlQuery("BEGIN"))
        `body`
        `execSym`(dbcon, SqlQuery("COMMIT"))
        committed = true
      finally:
        dec ozarkTxDepth
        if not committed:
          try:
            `execSym`(dbcon, SqlQuery("ROLLBACK"))
          except `dbErrSym`:
            discard
        `releaseConnSym`(db[].mainPool, dbcon)

#
# EXISTS probes
#

proc emitExistsExec(sqlText: string, bracket: NimNode): NimNode {.compileTime.} =
  ## Build an immediately-executed boolean probe around `sqlText`. The
  ## driver-specific bool spellings ("t"/pg, "1"/sqlite, or "true") all map
  ## to true.
  let scalarCall = newCall(ident"execScalar", ident"dbcon",
                           newCall(bindSym"SqlQuery", newLit(sqlText)),
                           newLit(bracket.len))
  for v in bracket:
    scalarCall.add(v)
  let v = genSym(nskLet, "ozarkExistsValue")
  result = newStmtList(
    nnkLetSection.newTree(
      nnkIdentDefs.newTree(v, newEmptyNode(), scalarCall)),
    nnkIfExpr.newTree(
      nnkElifBranch.newTree(
        newCall(ident"in", v,
          nnkBracket.newTree(newLit("t"), newLit("1"), newLit("true"))),
        newLit(true)),
      nnkElseExpr.newTree(newLit(false))))

macro exists*(x: untyped): untyped =
  ## Two forms:
  ##
  ## - Chained: `...select(...).where(...).exists()` is true when the query
  ##   matches at least one row.
  ## - Table-level: `Models.table(Users).exists()` is true when the table
  ##   has at least one row.
  ##
  ## Both execute immediately and return `bool`.
  var stage: string
  case x.kind
  of nnkBlockExpr, nnkBlockStmt:
    let t = x[1][^1]
    if t.kind == nnkCall and t.len > 0 and t[0].kind in {nnkIdent, nnkSym}:
      stage = t[0].strVal
  of nnkCall:
    if x.len > 0 and x[0].kind in {nnkIdent, nnkSym}:
      stage = x[0].strVal
  else:
    discard

  if stage in selectFamilyResults:
    let trailing = trailingCallOf(x)
    var bracket = paramsBracketOf(trailing)
    result = emitExistsExec(
      "SELECT EXISTS(SELECT 1 FROM (" & trailing[1].strVal & ") AS ozark_ex)",
      bracket)
  else:
    withTableCheck(x):
      result = emitExistsExec(
        "SELECT EXISTS(SELECT 1 FROM " & getTableName($x[1][0]) & ")",
        nnkBracket.newTree())

#
# last / pluck
#

macro last*(sql: untyped): untyped =
  ## Fetch a single row ordered by highest `id`. Executes immediately and
  ## returns a `Collection[T]`. Cannot be combined with an explicit
  ## `orderBy` on the same chain.
  ensureStage(sql, ["ozarkSelectResult", "ozarkWhereResult", "ozarkWhereInResult",
                    "ozarkRawSQLResult"], "last")
  let trailing = trailingCallOf(sql)
  if "order by" in trailing[1].strVal.toLowerAscii:
    error("`last()` applies its own `ORDER BY id DESC`; remove the explicit orderBy from this chain first.", sql)
  withColumnCheck(modelSymOf(sql), "id"):
    discard
  var bracket = paramsBracketOf(trailing)
  swapResultCall(trailing, bindSym"ozarkLimitResult",
    trailing[1].strVal & " ORDER BY id DESC LIMIT 1", bracket)
  result = newCall(ident"get", sql)

macro pluck*(sql: untyped, col: static string): untyped =
  ## Execute the current query but collect only `col` into a
  ## `seq[string]`, one entry per matched row.
  ensureStage(sql, selectFamilyResults, "pluck")
  withColumnCheck(modelSymOf(sql), col):
    discard
  let trailing = trailingCallOf(sql)
  var bracket = paramsBracketOf(trailing)
  let wrapped = "SELECT " & col & " FROM (" & trailing[1].strVal & ") AS ozark_pluck"
  var call = newCall(ident"execColumn", ident"dbcon",
                     newCall(bindSym"SqlQuery", newLit(wrapped)),
                     newLit(bracket.len))
  for v in bracket:
    call.add(v)
  result = call