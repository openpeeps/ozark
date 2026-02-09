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
  add result, id[0].toLowerAscii
  for c in id[1..^1]:
    if c.isUpperAscii:
      add result, "_"
      add result, c.toLowerAscii
    else:
      add result, c

macro newModel*(id, fields: untyped) =
  ## Macro for defining a new model at compile time.
  ## This macro will create a Nim object that represents
  ## the model and also register it in the StaticSchemas table.
  result = newNimNode(nnkStmtList)
  let tableName = getTableName($id)
  if StaticSchemas.hasKey(tableName):
    raise newException(ValueError, "Model with id '" & $id & "' already exists.")
  var modelFields = newNimNode(nnkRecList)
  var modelSchema = newTable[string, SqlNode]()
  for field in fields:
    var fieldIdent = newNimNode(nnkIdentDefs)
    case field.kind
    of nnkCall:
      case field[0].kind
      of nnkIdent:
        field[1].expectKind(nnkStmtList)
        let fieldName = field[0]
        var datatype: (DataType, Option[seq[string]])
        if field[1][0].kind == nnkIdent:
          datatype[0] = parseEnum[DataType](field[1][0].strVal.toLowerAscii())
        elif field[1][0].kind == nnkCall:
          datatype = getDatatype(field[1][0][0].strVal)
          let params = field[1][0][1..^1].map(proc(it: NimNode): string =
            case it.kind
            of nnkIntLit:
              return $it.intVal
            of nnkStrLit:
              return it.strVal
            else:
              raise newException(ValueError, "Invalid parameter type for data type '" & datatype[0].repr & "'")
          )
          if datatype[1].isSome:
            datatype[1] = some(params)
        else:
          raise newException(ValueError, "Invalid data type for field '" & $fieldName & "'")
        
        fieldIdent.add(nnkPostfix.newTree(ident"*", fieldName))
        let colDefNode = SqlNode(kind: nkColumnDef)
        colDefNode.add(newNode(nkIdent, $fieldName))
        colDefNode.add(newNode(nkIdent, $datatype))
        modelSchema[$(fieldName)] = colDefNode
      of nnkAccQuoted:
        field[0][0].expectKind(nnkIdent)
        let id = field[0][0]
        fieldIdent.add(nnkPostfix.newTree(ident"*", field[0][0]))
      of nnkPragmaExpr:
        var id: NimNode
        if field[0][0].kind == nnkIdent:
          id = field[0][0]
        elif field[0][0].kind == nnkAccQuoted:
          id = field[0][0][0]
          fieldIdent.add(nnkPostfix.newTree(ident"*", id))
      else: discard
      fieldIdent.add(ident"string")
      fieldIdent.add(newEmptyNode())
    else: discard
    modelFields.add(fieldIdent)

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
