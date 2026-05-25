# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

## This module implements the runtime logic for the SQLite driver, including
## managing prepared statements and mapping query results to model instances. It provides the core functionality that
## allows Ozark to execute SQL queries and return results as instances of user-defined models when using SQLite as the database backend.

import std/[tables, sequtils, hashes, strutils, times]
import pkg/db_connector/[db_sqlite, sqlite3, db_common]
import ../collection, ../model

type
  PreparedKey* = tuple[conn: pointer, stmtName: string]

var ozarkPreparedQueriesCache* {.global.}: TableRef[PreparedKey, SqlPrepared] = newTable[PreparedKey, SqlPrepared]()
  ## A runtime cache for prepared SQL statements, keyed
  ## by a combination of the database connection pointer
  ## and a unique name for the prepared statement. This allows
  ## us to reuse prepared statements across multiple queries
  ## without having to prepare them again, improving performance

proc stmtNameFor(sql: SqlQuery, nParams: int): string =
  let sig = string(sql) & "|" & $nParams
  result = "ozark_stmt_" & toHex(cast[uint64](hash(sig)))

proc ensurePrepared*(db: DbConn, name: string, sql: SqlQuery, nParams: int): SqlQuery =
  ## `name` is kept for API compatibility, but we use a
  ## deterministic name from SQL
  return sql
  # let stmtName = stmtNameFor(sql, nParams)
  # let key: PreparedKey = (cast[pointer](db), stmtName)
  # if key notin ozarkPreparedQueriesCache:
  #   ozarkPreparedQueriesCache[key] = prepare(db, string(sql))
  # result = ozarkPreparedQueriesCache[key]

# proc clearPreparedByKey*(key: PreparedKey) =
#   ## Finalize and remove a single prepared statement by its cache key.
#   if key in ozarkPreparedQueriesCache:
#     let ps = ozarkPreparedQueriesCache[key]
#     finalize(ps)
#     ozarkPreparedQueriesCache.del(key)

# proc clearPreparedForSql*(db: DbConn, sql: SqlQuery, nParams: int) =
#   ## Finalize and remove the prepared statement for the given SQL/nParams on this connection.
#   let stmtName = stmtNameFor(sql, nParams)
#   let key: PreparedKey = (cast[pointer](db), stmtName)
#   clearPreparedByKey(key)

# proc clearAllPreparedForConn*(db: DbConn) =
#   ## Finalize and remove all cached prepared statements for a given connection.
#   var toRemove: seq[PreparedKey] = @[]
#   for k, _ in ozarkPreparedQueriesCache.pairs:
#     if k.conn == cast[pointer](db):
#       toRemove.add(k)
#   for k in toRemove:
#     let ps = ozarkPreparedQueriesCache[k]
#     finalize(ps)
#     ozarkPreparedQueriesCache.del(k)

proc instantRowsToModels*[T](dbcon: DbConn, sql: SqlQuery, colNames: seq[string],
                          nParams: int, params: varargs[string, `$`]): Collection[T] =
  ## Execute a SQL query that returns multiple rows, and map
  ## each row to an instance of the specified model type T
  var
    isEmpty = true
    results: Collection[T]
    cols: DBColumns = @[]
    colKeys: seq[string]

  # Prepare statement and bind params (uses low-level sqlite3 PStmt)
  var stmtPrepared: SqlPrepared = prepare(dbcon, string(sql))
  var stmt = cast[PStmt](stmtPrepared)
  if stmt == nil:
    raise newException(ValueError, "prepare returned nil stmt")

  # clear previous bindings and bind provided params (1-based)
  discard clear_bindings(stmt)
  for i in 0..<params.len:
    let idx = int32(i + 1)
    let p = params[i]
    var bres = SQLITE_OK
    # choose appropriate bind according to param shape
    # if p.len > 0 and p.allIt(it.isDigit()):
    #   # integer
    #   let v = parseInt(p)
    #   bres = bind_int64(stmt, idx, v)
    # elif p.len > 0 and (p.contains('.') or p.toLowerAscii().contains('e')):
    #   # float-ish
    #   let f = parseFloat(p)
    #   bres = bind_double(stmt, idx, f)
    # else:
    # fallback to text
    bres = bind_text(stmt, idx, p.cstring, -1, SQLITE_TRANSIENT)
    if bres != SQLITE_OK:
      raise newException(ValueError, "sqlite3_bind failed for param " & $idx & " rc=" & $bres)

  # collect column names once
  let ccount = int(column_count(stmt))
  if ccount > 0:
    colKeys.setLen(ccount)
    for i in 0..<ccount:
      let cname = column_name(stmt, int32(i))
      colKeys[i] = if cname == nil: "" else: $(cname)

  # Now step through the results

  var rc = step(stmt)
  while rc == SQLITE_ROW:
    isEmpty = false
    var inst = new(T)
    var modelFields: seq[string] = @[]
    for fName, fValue in inst[].fieldPairs():
      modelFields.add(fName)
      if colNames.len > 0 and colNames[0] != "*" and fName in colNames:
        if fName in colKeys:
          let idx = colKeys.find(fName)
          let txt = column_text(stmt, int32(idx))
          if txt == nil:
            when fValue.type is string:
              fValue = ""  # map NULL -> empty string for string fields
          else:
            when fValue.type is string:
              fValue = $(txt)
        else:
          raise newException(OzarkModelDefect, "Model field `" & $T & "." & fName & "` does not have a corresponding column in the SQL result")
      elif colNames.len > 0 and colNames[0] == "*":
        if fName in colKeys:
          let idx = colKeys.find(fName)
          let txt = column_text(stmt, int32(idx))
          if txt == nil:
            when fValue.type is string:
              fValue = ""
          else:
            when fValue.type is string:
              fValue = $(txt)

    when compiles(inst.extra):
      inst.extra = initTable[string, string]()
      for i, k in colKeys:
        if k notin modelFields:
          let txt = column_text(stmt, int32(i))
          inst.extra[k] = if txt == nil: "" else: $(txt)

    results.entries.add(inst)
    rc = step(stmt)

  if rc != SQLITE_DONE:
    # unexpected non-row, non-done return code
    raise newException(ValueError, "sqlite3_step failed with rc=" & $rc)
 
  # reset statement so it can be reused later
  # discard reset(stmt)
  finalize(stmtPrepared)
  results

proc getRowToModel*[T](dbcon: DbConn, sql: SqlQuery, nParams: int, params: varargs[string, `$`],
                    assignProc: proc(inst: T, row: seq[string])): Collection[T] =
  ## Execute a SQL query that is expected to return a single row,
  ## and map that row to an instance of the specified model type T
  # let sqlPrepared = ensurePrepared(dbcon, "", sql, nParams)
  var
    row = getRow(dbcon, sql, params)
    isEmpty = true
  for v in row:
    isEmpty = isEmpty and v.len == 0
  if row.len > 0 and not isEmpty:
    var inst = new(T)
    assignProc(inst, row)
    result.entries.add(inst)
  result

proc toDbValue*(v: bool): string =
  ## Convert a boolean value to a form suitable for SQLite (1 for true, 0 for false)
  if v: "1" else: "0"

proc toDbValue*(v: string): string =
  ## Convert a string value to a form suitable for PostgreSQL
  for i in 0..<v.len:
    case v[i]
    of 't':
      if i + 3 == v.len and v[i..i+3] == "true":
        return "1"
    of 'f':
      if i + 4 == v.len and v[i..i+4] == "false":
        return "0"
    else: discard
  v

proc fromDBValue*[T](v: string): T =
  ## Convert a string value from the database into the specified type T.
  when T is bool:
    if v == "1" or v == "true": return true
    if v == "0" or v == "false": return false
    else:
      raise newException(ValueError, "Cannot convert value `" & v & "` to bool")
  elif T is DateTime:
    parse(v, "yyyy-MM-dd'T'HH:mm:sszzz")
  elif T is string:
    return v
  else:
    raise newException(ValueError, "Unsupported type for fromDBValue: " & $T)

proc toDbValue*(v: DateTime): string =
  ## SQLite stores ISO‑8601 with a `T` separator and a colon in the offset.
  const isoFmt = "yyyy-MM-dd'T'HH:mm:sszzz"
  v.format(isoFmt)
