import std/[macros, net, strutils, tables]

import pkg/threading/once
import pkg/db_connector/db_postgres

export db_postgres
export Port, strVal, `%`

type
  DBConnectionPool* = ref object
    connections*: seq[DBConn]
    busyConnections*: seq[DBConn]

  DBDriver* = enum
    PostgreSQLDriver
    MYSQLDriver
    SQLiteDriver

  DBConnection = ref object
    driver: DBDriver
    address*, name*, user*, password*: string
    # dbConn: DBConn
    # pool: DBConnectionPool
    port: Port

  DBConnections = OrderedTableRef[string, DBConnection]

  Ozark = object
    dbs: DBConnections
      # holds credentials for multiple Database Connections
    maindb: DBConnection
      # credentials for the main database connection

var
  DB: ptr Ozark
  o = createOnce()

proc getInstance*(): ptr Ozark =
  ## Get the singleton instance of the database manager
  once(o):
    DB = createShared(Ozark)
  result = DB

proc initOzarkDatabase*(address, name, user, password: string,
                port: Port, driver: DBDriver = DBDriver.PostgreSQLDriver) =
  ## Initializes the singleton instance of the database manager
  ## using provided credentials as main database
  let db = getInstance()
  db[].maindb = DBConnection(
    address: address,
    user: user,
    name: name,
    password: password,
    port: port,
    driver: driver
  )

proc `[]=`*(db: Ozark, id: string, dbCon: DBConnection) =
  ## Add new database connection credentials
  db[id] = dbCon

proc add*(db: Ozark, id: string, dbCon: DBConnection) {.inline.} =
  ## Add new database connection credentials
  db[id] = dbCon

macro withDB*(body) =
  ## Use the current database context to run database queries.
  ## 
  ## This macro will open a connection to the database,
  ## execute the body, and then close the connection.
  result = newStmtList()
  add result, quote do:
    let db = getInstance()
    assert db != nil, "Database manager not initialized. Call initDBManager first."
    let dbcon {.inject.} =
        open(db[].maindb.address, db[].maindb.user,
                db[].maindb.password, db[].maindb.name)
    defer:
      dbcon.close()
    block:
      `body`