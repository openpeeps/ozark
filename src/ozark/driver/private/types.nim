# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

import std/macros
import pkg/openparser/sql

type
  DataType* = enum
    BigInt = "int8"
    BigSerial = "serial18"
    Bit = "bit"
    BitVarying = "varbit" # $1
    Boolean = "boolean"
    Box = "box"
    Bytea = "bytea"
    Char = "char" # $1
    Varchar = "varchar" # $1
    Cidr = "cidr"
    Circle = "circle"
    Date = "date"
    DoublePrecision = "float8"
    Inet = "inet"
    Int = "int"
    Int4 = "int4"
    Interval = "interval"
    Json = "json"
    Jsonb = "jsonb"
    Line = "line"
    Lseg = "lseg"
    Macaddr = "macaddr"
    Macaddr8 = "macaddr8"
    Money = "money"
    Numeric = "numeric"
    Path = "path"
    PGLsn = "pg_lsn"
    PGSnapshot = "pg_snapshot"
    Point = "point"
    Polygon = "polygon"
    Real = "float4"
    SmallInt = "int2"
    SmallSerial = "serial2"
    Serial = "serial"
    Text = "text"
    Time = "time" # $1
    Timezone = "timez"
    Timestamp = "timestamp"
    TimestampTz = "timestamptz"
    TsQuery = "tsquery"
    TsVector = "tsvector"
    Uuid = "uuid"
    Enum = "enum"

    # SQLite-specific types
    Integer = "integer"
    Blob = "blob"

proc countSqlArgs*(args: NimNode): int {.compileTime.} =
  case args.kind
  of nnkEmpty:
    0
  of nnkBracket:
    args.len
  of nnkHiddenStdConv:
    if args.len > 1 and args[1].kind == nnkBracket:
      args[1].len
    else: 1
  else:
    1

proc appendSqlArgs*(callNode: var NimNode, args: NimNode) {.compileTime.} =
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

proc getDriverType*(n: NimNode): SqlDriver = SqlDriver(n[1][1].intval)