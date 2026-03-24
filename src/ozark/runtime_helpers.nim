# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

import std/[tables, sequtils, hashes, strutils]
import pkg/db_connector/[postgres, db_postgres, db_common]

import ./collection, ./model

type
  PreparedKey* = tuple[conn: pointer, stmtName: string]

var preparedRtCache* {.global.}: TableRef[PreparedKey, SqlPrepared] = newTable[PreparedKey, SqlPrepared]()
  ## A runtime cache for prepared SQL statements, keyed
  ## by a combination of the database connection pointer
  ## and a unique name for the prepared statement. This allows
  ## us to reuse prepared statements across multiple queries
  ## without having to prepare them again, improving performance

proc stmtNameFor(sql: SqlQuery, nParams: int): string =
  let sig = string(sql) & "|" & $nParams
  result = "ozark_stmt_" & toHex(cast[uint64](hash(sig)))

proc ensurePrepared*(db: DbConn, name: string, sql: SqlQuery, nParams: int): SqlPrepared =
  ## `name` is kept for API compatibility, but we use a
  ## deterministic name from SQL
  let stmtName = stmtNameFor(sql, nParams)
  let key: PreparedKey = (cast[pointer](db), stmtName)
  if key notin preparedRtCache:
    preparedRtCache[key] = prepare(db, stmtName, sql, nParams)
  result = preparedRtCache[key]
proc instantRowsToModels*[T](
  dbcon: DbConn,
  sql: SqlQuery,
  colNames: seq[string],
  nParams: int,
  params: varargs[string, `$`]
): Collection[T] =
  ## Execute a SQL query that returns multiple rows, and map each row to an
  ## instance of the specified model type T.
  var
    isEmpty = true
    results: Collection[T]
    cols: DBColumns = @[]
    colKeys: seq[string]
  let sqlPrepared = ensurePrepared(dbcon, "", sql, nParams)
  for row in instantRows(dbcon, cols, sqlPrepared, params):
    isEmpty = isEmpty and row.len == 0
    if isEmpty: continue
    if colKeys.len == 0:
      colKeys = cols.mapIt(it.name)
    var inst = new(T)
    var modelFields: seq[string] = @[]
    for fName, fValue in inst[].fieldPairs():
      modelFields.add(fName)
      if colNames[0] != "*" and fName in colNames:
        if fName in colKeys:
          when fValue.type is string:
            fValue = row[colKeys.find(fName)]
        else:
          raise newException(OzarkModelDefect, "Model field `" & $T & "." & fName & "` does not have a corresponding column in the SQL result")
      elif colNames[0] == "*":
        if fName in colKeys:
          when fValue.type is string:
            fValue = row[colKeys.find(fName)]
    when compiles(inst.extra):
      inst.extra = initTable[string, string]()
      for i, k in colKeys:
        if k notin modelFields:
          inst.extra[k] = row[i]
    results.entries.add(inst)
  results

proc getRowToModel*[T](
  dbcon: DbConn,
  sql: SqlQuery,
  nParams: int,
  params: varargs[string, `$`],
  assignProc: proc(inst: T, row: seq[string])
): Collection[T] =
  ## Execute a SQL query that is expected to return a single row,
  ## and map that row to an instance of the specified model type T
  let sqlPrepared = ensurePrepared(dbcon, "", sql, nParams)
  var
    row = getRow(dbcon, sqlPrepared, params)
    isEmpty = true
    results: Collection[T]
  for v in row:
    isEmpty = isEmpty and v.len == 0
  if row.len > 0 and not isEmpty:
    var inst = new(T)
    assignProc(inst, row)
    results.entries.add(inst)
  results
