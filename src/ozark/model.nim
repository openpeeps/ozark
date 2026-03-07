# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

import std/[macros, macrocache, tables, strutils, options, sequtils]

import pkg/threading/once
import pkg/[parsesql, voodoo/setget]

import ./private/types
export tables, setget

type
  Model* = object of RootObj

  SchemaTable* = OrderedTableRef[string, ref Model]
    ## A cache table that holds all models defined at compile-time.

  ModelsTable* = object
    ## A global table that holds all models defined at compile-time.
    ## Also, models defined at runtime will be added here.
    schemas*: SchemaTable

var
  Models*: ptr ModelsTable
    ## A global table that holds all models defined at compile-time.
    ## Also, models defined at runtime will be added here.
  o = createOnce()

const
  StaticSchemas* = CacheTable"StaticSchema"
    ## A cache table that holds all models defined at compile-time.
    ## This allows us to determine if a model exists at compile-time
    ## and also to access its schema definition.
var SqlSchemas* {.compileTime.} = newTable[string, newTable[string, SqlNode]()]()

proc initModels() =
  # Initialize the Models singleton
  once(o):
    Models = createShared(ModelsTable)
    Models.schemas = SchemaTable()

initModels()

proc getDatatype*(dt: string): (DataType, Option[seq[string]]) =
  ## Helper to get the datatype from string.
  ## If the data type requires additional parameters, it will
  ## return a Node with the parameters.
  result[0] = parseEnum[DataType](dt.toLowerAscii)
  case result[0]
  of DataType.Char, DataType.Varchar:
    result[1] = some(@[dt])
  else:
    result[1] = none(seq[string])

proc getTableName*(id: string): string =
  ## Helper to convert a model name to a table name.
  add result, id[0].toLowerAscii
  for c in id[1..^1]:
    if c.isUpperAscii:
      add result, "_"
      add result, c.toLowerAscii
    else:
      add result, c


proc toSqlDefaultLiteral(n: NimNode): string =
  ## Converts Nim literal nodes to SQL literal text.
  case n.kind
  of nnkIntLit, nnkInt8Lit, nnkInt16Lit, nnkInt32Lit, nnkInt64Lit,
     nnkUIntLit, nnkUInt8Lit, nnkUInt16Lit, nnkUInt32Lit, nnkUInt64Lit,
     nnkFloatLit, nnkFloat32Lit, nnkFloat64Lit:
    n.repr
  of nnkStrLit:
    "'" & n.strVal.replace("'", "''") & "'"
  of nnkIdent:
    let v = n.strVal.toLowerAscii
    case v
    of "true": "TRUE"
    of "false": "FALSE"
    of "null": "NULL"
    else:
      error("Unsupported default identifier literal: " & n.repr, n)
  of nnkNilLit:
    "NULL"
  else:
    error("Unsupported default literal type: " & n.repr, n)

proc parseFieldDecl(field: NimNode): tuple[fieldCall: NimNode, defaultSql: Option[string]] =
  ## Accepts:
  ##   fieldHead: TypeExpr
  ##   fieldHead: TypeExpr = defaultExpr
  case field.kind
  of nnkCall:
    result.fieldCall = field
    result.defaultSql = none(string)
  of nnkAsgn:
    if field[0].kind != nnkCall:
      raise newException(ValueError, "Invalid field declaration (expected `name: Type` on lhs): " & field.repr)
    result.fieldCall = field[0]
    result.defaultSql = some(toSqlDefaultLiteral(field[1]))
  else:
    error("Invalid field declaration: " & field.repr, field)

proc parseFieldHead(head: NimNode): tuple[name: NimNode, pragmas: seq[string]] =
  ## Parses:
  ##   id
  ##   username {.notnull, unique.}
  case head.kind
  of nnkIdent:
    result.name = head
  of nnkPragmaExpr:
    case head[0].kind
    of nnkIdent:
      result.name = head[0]
    of nnkAccQuoted:
      result.name = head[0][0]
    else:
      error("Invalid field identifier: " & head.repr, head)

    for p in head[1]:
      case p.kind
      of nnkIdent:
        result.pragmas.add(p.strVal.toLowerAscii)
      of nnkCall:
        # Supports pragma with args, keeps full repr for custom handling later
        result.pragmas.add(p.repr.toLowerAscii)
      else:
        error("Invalid pragma in field declaration: " & p.repr, p)
  else:
    error("Invalid field declaration head: " & head.repr, head)

proc parseTypeAndDefault(n: NimNode): tuple[typeExpr: NimNode, defaultSql: Option[string]] =
  ## Accepts:
  ##   TypeExpr
  ##   TypeExpr = defaultExpr
  case n.kind
  of nnkIdent, nnkCall:
    result.typeExpr = n
    result.defaultSql = none(string)
  of nnkAsgn:
    # Example: Asgn(Ident "Boolean", Ident "false")
    result.typeExpr = n[0]
    result.defaultSql = some(toSqlDefaultLiteral(n[1]))
  of nnkDotExpr:
    # Handles model references like `Users.id`
    if n[0].kind == nnkIdent and n[1].kind == nnkIdent:
      let reftableName = getTableName($n[0])
      if StaticSchemas.hasKey(reftableName):
        result.typeExpr = n
        result.defaultSql = none(string)
      else:
        error("Referenced model '" & n[0].strVal & "' not found for field '" &
          $n[1] & "'. Make sure to define the referenced model before using it.", n[0])
  else:
    raise newException(ValueError, "Invalid type/default expression: " & n.repr)

proc parseDatatypeExpr(typeExpr: NimNode): (DataType, Option[seq[string]]) =
  ## Parses:
  ##   Serial
  ##   Varchar(50)
  case typeExpr.kind
  of nnkIdent:
    result[0] = parseEnum[DataType](typeExpr.strVal.toLowerAscii)
    result[1] = none(seq[string])
  of nnkCall:
    result = getDatatype(typeExpr[0].strVal)
    let params = typeExpr[1..^1].map(proc(it: NimNode): string =
      case it.kind
      of nnkIntLit:
        $it.intVal
      of nnkStrLit:
        it.strVal
      else:
        error("Invalid datatype parameter: " & it.repr, it)
    )
    if result[1].isSome:
      result[1] = some(params)
  of nnkDotExpr:
    let tableName = getTableName(typeExpr[0].strVal)
    if StaticSchemas.hasKey(tableName):
        let refSchema = SqlSchemas[tableName]
        if refSchema.hasKey(typeExpr[1].strVal):
          if refSchema.hasKey(typeExpr[1].strVal):
            let refFieldNode = refSchema[typeExpr[1].strVal]
            result[0] = parseEnum[DataType](refFieldNode[1].strVal.toLowerAscii)
            result[1] = none(seq[string])
          else:
            error("Referenced field '" & typeExpr[1].strVal & "' not found in model '" &
              typeExpr[0].strVal & "'. Make sure to define the referenced field before using it.", typeExpr[1])
        else:
          error("Referenced model '" & typeExpr[0].strVal &
              "' not found for field '" &
              typeExpr[1].strVal &
              "'. Make sure to define the referenced model before using it.",
            typeExpr[0])
  else:
    error("Invalid datatype expression: " & typeExpr.repr, typeExpr)

proc pragmaToConstraint(p: string): string =
  ## Map Nim pragmas to SQL constraint tokens.
  case p
  of "notnull": "NOT NULL"
  of "unique": "UNIQUE"
  of "pk", "primarykey": "PRIMARY KEY"
  else: p.toUpperAscii

proc parseObjectField(tableName: string, field, fieldIdent: NimNode) =
  ## Parses one `newModel` field:
  ##   id: Serial
  ##   username {.notnull.}: Varchar(50)
  field.expectKind(nnkCall)
  field[1].expectKind(nnkStmtList)
  if field[1].len == 0:
    raise newException(ValueError, "Missing datatype for field: " & field.repr)

  let (fieldName, pragmas) = parseFieldHead(field[0])
  let (typeExpr, defaultSql) = parseTypeAndDefault(field[1][0])
  let datatype = parseDatatypeExpr(typeExpr)

  # Nim object field
  fieldIdent.add(nnkPostfix.newTree(ident"*", fieldName))

  # SQL schema field
  let colDefNode = SqlNode(kind: nkColumnDef)
  colDefNode.add(newNode(nkIdent, $fieldName))

  if datatype[1].isNone:
    colDefNode.add(newNode(nkIdent, $datatype[0]))
  else:
    let colDefCall = newNode(nkCall)
    colDefCall.add(newNode(nkIdent, $datatype[0]))
    for param in datatype[1].get:
      colDefCall.add(newNode(nkIntegerLit, param))
    colDefNode.add(colDefCall)

  for p in pragmas:
    colDefNode.add(newNode(nkIdent, pragmaToConstraint(p)))
  if defaultSql.isSome:
    colDefNode.add(newNode(nkIdent, "DEFAULT " & defaultSql.get))
  SqlSchemas[tableName][$fieldName] = colDefNode

macro newModel*(id, fields: untyped) =
  ## Macro for defining a new model at compile time.
  ## This macro will create a Nim object that represents
  ## the model and also register it in the StaticSchemas table.
  result = newNimNode(nnkStmtList)
  let tableName = getTableName($id)
  if StaticSchemas.hasKey(tableName):
    result = StaticSchemas[tableName]
    return # Model already defined, skip redefinition (allows for multiple imports without conflicts)
  var modelFields = newNimNode(nnkRecList)
  # var modelSchema = newTable[string, SqlNode]()
  SqlSchemas[tableName] = newTable[string, SqlNode]()
  for field in fields:
    var fieldIdent = newNimNode(nnkIdentDefs)
    if field.kind == nnkCall:
      parseObjectField(tableName, field, fieldIdent)
      fieldIdent.add(ident"string")
      fieldIdent.add(newEmptyNode())
      modelFields.add(fieldIdent)
    else:
      raise newException(ValueError, "Invalid field declaration: " & field.repr)

  result = newStmtList(
    nnkTypeSection.newTree(
      nnkTypeDef.newTree(
        nnkPragmaExpr.newTree(
          nnkPostfix.newTree(ident("*"), id),
          nnkPragma.newTree(ident"getters")
        ),
        newEmptyNode(),
        nnkRefTy.newTree(
          nnkObjectTy.newTree(
            newEmptyNode(),
            nnkOfInherit.newTree(
              newIdentNode("Model")
            ),
            modelFields
          )
        )
      )
    ),
    newCall(ident"expandGetters"),
    nnkAsgn.newTree(
      nnkBracketExpr.newTree(
        nnkDotExpr.newTree(
          nnkBracketExpr.newTree(
            newIdentNode("Models"),
          ),
          newIdentNode("schemas")
        ),
        newLit(tableName)
      ),
      newCall(id)
    )
  )
  # register the model in the StaticSchemas table
  # this allows us to determine if a model exists at compile-time
  # and also to access its schema definition.
  StaticSchemas[tableName] = result
