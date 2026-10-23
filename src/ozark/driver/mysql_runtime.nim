# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#
import std/[tables, sequtils, hashes, strutils, times]
import pkg/db_connector/[db_mysql, db_common, mysql]
import ../collection, ../model

type
  PreparedKey* = tuple[conn: pointer, stmtName: string]

var ozarkPreparedQueriesCache* {.global.}: TableRef[PreparedKey, SqlQuery] = newTable[PreparedKey, SqlQuery]()

proc stmtNameFor(sql: SqlQuery, nParams: int): string =
  let sig = string(sql) & "|" & $nParams
  result = "ozark_stmt_" & toHex(cast[uint64](hash(sig)))

proc ensurePrepared*(db: DbConn, name: string, sql: SqlQuery, nParams: int): SqlQuery =
  return sql

proc instantRowsToModels*[T](dbcon: DbConn, sql: SqlQuery, colNames: seq[string],
                          nParams: int, params: varargs[string, `$`]): Collection[T] =
  var
    isEmpty = true
    results: Collection[T]
    colKeys: seq[string]

  dbcon.exec(sql, params)
  var pres = mysql.useResult(PMySQL dbcon)
  if pres == nil:
    return results

  let L = int(mysql.numFields(pres))
  colKeys.setLen(L)
  for i in 0..<L:
    let f = mysql.fetchFieldDirect(pres, cast[mysql.cuint](i))
    let nm = if f == nil: nil else: f.name
    colKeys[i] = if nm == nil: "" else: $nm

  var row: cstringArray
  while true:
    row = mysql.fetchRow(pres)
    if row == nil: break
    isEmpty = false
    var inst = new(T)
    var modelFields: seq[string] = @[]
    for fName, fValue in inst[].fieldPairs():
      modelFields.add(fName)
      if colNames.len > 0 and colNames[0] != "*" and fName in colNames:
        if fName in colKeys:
          let idx = colKeys.find(fName)
          let txt = row[idx]
          if txt == nil:
            when fValue.type is string:
              fValue = ""
          else:
            when fValue.type is string:
              fValue = $txt
        else:
          raise newException(OzarkModelDefect, "Model field `" & $T & "." & fName & "` does not have a corresponding column in the SQL result")
      elif colNames.len > 0 and colNames[0] == "*":
        if fName in colKeys:
          let idx = colKeys.find(fName)
          let txt = row[idx]
          if txt == nil:
            when fValue.type is string:
              fValue = ""
          else:
            when fValue.type is string:
              fValue = $txt
    when compiles(inst.extra):
      inst.extra = initTable[string, string]()
      for i, k in colKeys:
        if k notin modelFields:
          let txt = row[i]
          inst.extra[k] = if txt == nil: "" else: $txt
    results.entries.add(inst)

  mysql.freeResult(pres)
  results

proc getFirstToModel*[T](dbcon: DbConn, sql: SqlQuery, colNames: seq[string],
                         nParams: int, params: varargs[string, `$`]): Collection[T] =
  result = instantRowsToModels[T](dbcon, sql, colNames, nParams, params)
  if result.entries.len > 1:
    result.entries.setLen(1)

proc getRowToModel*[T](dbcon: DbConn, sql: SqlQuery, nParams: int, params: varargs[string, `$`],
                    assignProc: proc(inst: T, row: seq[string])): Collection[T] =
  let row = dbcon.getRow(sql, params)
  var isEmpty = true
  for v in row:
    isEmpty = isEmpty and v.len == 0
  if row.len > 0 and not isEmpty:
    var inst = new(T)
    assignProc(inst, row)
    result.entries.add(inst)

proc toDbValue*(v: bool): string =
  if v: "1" else: "0"

proc toDbValue*(v: string): string =
  if v == "true": "1"
  elif v == "false": "0"
  else: v

proc parseDateTimeFlexible(v: string): DateTime =
  var s = v.strip()
  if s.endsWith("Z"):
    s = s[0 .. ^2] & "+00:00"
  const fmts = [
    "yyyy-MM-dd'T'HH:mm:sszzz",
    "yyyy-MM-dd'T'HH:mm:sszz",
    "yyyy-MM-dd HH:mm:sszzz",
    "yyyy-MM-dd HH:mm:sszz",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss"
  ]
  for fmt in fmts:
    try:
      return parse(s, fmt)
    except CatchableError, Defect:
      discard
  if 'T' in s:
    let s2 = s.replace("T", " ")
    for fmt in fmts:
      try:
        return parse(s2, fmt)
      except CatchableError, Defect:
        discard
  raise newException(ValueError, "Cannot parse DateTime `" & v & "`")

proc fromDBValue*[T](v: string): T =
  when T is bool:
    if v == "1" or v == "true": return true
    if v == "0" or v == "false": return false
    else: raise newException(ValueError, "Cannot convert value `" & v & "` to bool")
  elif T is DateTime:
    parseDateTimeFlexible(v)
  elif T is string:
    return v
  else:
    raise newException(ValueError, "Unsupported type for fromDBValue: " & $T)

proc toDbValue*(v: DateTime): string =
  const fmt = "yyyy-MM-dd HH:mm:ss"
  v.format(fmt)

proc execColumn*(dbcon: DbConn, sql: SqlQuery, nParams: int,
                 params: varargs[string, `$`]): seq[string] =
  let rows = dbcon.getAllRows(sql, params)
  for r in rows:
    if r.len > 0:
      result.add(r[0])

proc execScalar*(dbcon: DbConn, sql: SqlQuery, nParams: int,
                 params: varargs[string, `$`]): string =
  let row = dbcon.getRow(sql, params)
  if row.len > 0:
    result = row[0]
  else:
    result = ""

proc execRows*(dbcon: DbConn, sql: SqlQuery, nParams: int,
               params: varargs[string, `$`]): seq[seq[string]] =
  let rows = dbcon.getAllRows(sql, params)
  for r in rows:
    if r.len > 0:
      var values: seq[string]
      values.newSeq(r.len)
      for i in 0 ..< r.len:
        values[i] = r[i]
      result.add(values)