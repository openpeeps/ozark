# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

import std/[macros, net, strutils, tables, locks, os]

import pkg/threading/once
import pkg/db_connector/db_postgres

export db_postgres
export Port, strVal, `%`

type
  DBConnectionPool* = ref object
    connections*: seq[DBConn]      # available
    busyConnections*: seq[DBConn]  # checked-out
    maxSize*: int
    lock: Lock

  DBDriver* = enum
    PostgreSQLDriver
    MYSQLDriver
    SQLiteDriver

  DBConnection* = ref object
    driver*: DBDriver
    address*, name*, user*, password*: string
    port*: Port

  DBConnections* = OrderedTableRef[string, DBConnection]

  Ozark = object
    dbs: DBConnections
    maindb: DBConnection
    mainPool: DBConnectionPool

var
  DB: ptr Ozark
  o = createOnce()

proc getInstance*(): ptr Ozark =
  ## Get the singleton instance of the database manager
  once(o):
    DB = createShared(Ozark)
    DB[].dbs = newOrderedTable[string, DBConnection]() # init map
  result = DB

proc initOzarkDatabase*(address, name, user, password: string,
                port: Port = Port(5432),
                driver: DBDriver = DBDriver.PostgreSQLDriver) =
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

macro withDB*(body: untyped) =
  ## Use the current database context to run database queries.
  ## 
  ## This macro will open a connection to the database,
  ## execute the body, and then close the connection.
  ## 
  ## For more efficient connection management, consider using `withDBPool` instead.
  result = newStmtList()
  add result, quote do:
    block:
      # This block ensures that multiple withDB calls can be nested without
      # interfering with each other's connections.
      let db = getInstance()
      assert db != nil, "Database manager not initialized. Call initDBManager first."
      let dbcon {.inject.} =
          open(db[].maindb.address, db[].maindb.user,
                  db[].maindb.password, db[].maindb.name)
      defer:
        dbcon.close()
      block:
        `body`

macro withDatabase*(id: static string, body: untyped) =
  ## Use the specified database context to run database queries.
  ## 
  ## This macro will open a connection to the database,
  ## execute the body, and then close the connection.
  ## 
  ## For more efficient connection management, consider using `withDBPool` instead.
  result = newStmtList()
  add result, quote do:
    block:
      # ensure multiple withDB calls can be nested without interfering with
      # each other's connections.
      let db = getInstance()
      assert db != nil, "Database manager not initialized. Call initDBManager first."
      assert db.hasKey(id), "Database connection with id `" & id & "` not found."
      let dbcon {.inject.} =
          open(db[id].address, db[$id].user,
                  db[id].password, db[$id].name)
      defer:
        dbcon.close()
      block:
        `body`

proc openConn(cfg: DBConnection): DBConn =
  # Currently only supports PostgreSQL, but can be extended to support other databases
  case cfg.driver
  of PostgreSQLDriver:
    open(cfg.address, cfg.user, cfg.password, cfg.name)
  else:
    raise newException(ValueError, "Only PostgreSQL driver pool is currently implemented.")

proc initOzarkPool*(size: Positive = 10) =
  ## Initialize main DB connection pool.
  let db = getInstance()
  assert db[].maindb != nil, "Main DB credentials not initialized. Call initOzarkDatabase first."

  var pool = DBConnectionPool(
    maxSize: size.int,
    connections: @[],
    busyConnections: @[]
  )
  initLock(pool.lock)

  for _ in 0..<size.int:
    pool.connections.add(openConn(db[].maindb))

  db[].mainPool = pool

proc closeOzarkPool*() =
  ## Close all pooled connections.
  let db = getInstance()
  if db[].mainPool.isNil: return

  acquire(db[].mainPool.lock)
  defer: release(db[].mainPool.lock)

  for c in db[].mainPool.connections:
    c.close()
  for c in db[].mainPool.busyConnections:
    c.close()

  db[].mainPool.connections.setLen(0)
  db[].mainPool.busyConnections.setLen(0)

proc acquireConn*(pool: DBConnectionPool, timeoutMs: int = 5000): DBConn =
  ## Borrow one connection from pool, waiting up to timeoutMs.
  let stepMs = 10
  var waited = 0
  while waited <= timeoutMs:
    acquire(pool.lock)
    if pool.connections.len > 0:
      result = pool.connections.pop()
      pool.busyConnections.add(result)
      release(pool.lock)
      return
    release(pool.lock)
    sleep(stepMs)
    inc(waited, stepMs)

  raise newException(ValueError, "Timed out waiting for a DB connection from pool.")

proc releaseConn*(pool: DBConnectionPool, conn: DBConn) =
  ## Return a connection to pool.
  acquire(pool.lock)
  defer: release(pool.lock)

  var idx = -1
  for i, c in pool.busyConnections:
    if c == conn:
      idx = i
      break

  if idx >= 0:
    pool.busyConnections.del(idx)
    pool.connections.add(conn)

macro withDBPool*(body: untyped) =
  ## Run queries using a pooled connection.
  result = newStmtList()
  add result, quote do:
    block:
      let db = getInstance()
      assert db != nil, "Database manager not initialized. Call initOzarkDatabase first."
      assert db[].mainPool != nil, "DB pool not initialized. Call initOzarkPool first."
      let dbcon {.inject.} = acquireConn(db[].mainPool)
      defer:
        releaseConn(db[].mainPool, dbcon)
      `body`