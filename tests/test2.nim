import os, unittest, strutils

const pqlibPath = currentSourcePath().parentDir / "greskewelbox" / "bin" / "16.9.0" / "darwin" / "lib"
const greskewel_lib = pqlibPath / "libpq.dylib"

{.passL: "-Wl,-rpath," & pqlibPath.}

import pkg/db_connector/db_postgres
import pkg/greskewel

import ../src/ozark

var greskew = initEmbeddedPostgres(
  PostgresConfig(
    basePath: getCurrentDir() / "tests" / "greskewelbox",
  )
)

newModel Users:
  id: Serial
  username: Varchar(50)
  name: Varchar(100)
  email: Varchar(100)

{.push dynlib: greskewel_lib.}
test "init embedded postgres and create tables":
  greskew.init()
  greskew.start()

  initOzarkDatabase("localhost", "postgres", "postgres", "postgres", Port(5432))
  withDB do:
    Models.table(Users).prepareTable().exec()
    Models.table(Users).dropTable(cascade = true).exec()
    Models.table(Users).prepareTable().exec()

  initOzarkPool(15)

suite "INSERT and SELECT queries":
  test "insert and select data":
    withDBPool do:
      var name = "John"
      let id = Models.table(Users).insert({
        name: name,
        username: "john1232",
        email: "test@example.com",
      }).execGet() # returns the id of the inserted row

      let res = Models.table(Users).selectAll()
                      .where("id", $id)
                      .getAll()

      check res.isEmpty == false
      check parseInt(res.entries[0].id) == id
      check res.entries[0].name == "John"
      check res.entries[0].username == "john1232"
      check res.entries[0].email == "test@example.com"

  test "select specific columns":
    withDBPool do:
      let res = Models.table(Users)
                      .select(["name", "email"])
                      .where("name", "John").get()
      check res.isEmpty == false
      check res.get(0).name == "John"
      check res.get(0).email == "test@example.com"

suite "WHERE queries":
  test "where query":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .where("name", "John").get()
      check res.isEmpty == false
      # check res.get(0).name == "John"

  test "whereNot query":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .whereNot("name", "John").get()
      check res.isEmpty

  test "orWhere query":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .where("name", "Ghost")
                      .orWhere("name", "John").get()
      check res.isEmpty == false
      check res.get(0).name == "John"
  
  test "orWhereNot query":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .whereNot("name", "John")
                      .orWhereNot("name", "Ghost").get()
      check res.isEmpty == false
      check res.get(0).name == "John"
  

suite "LIKE queries":
  test "like query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereLike("name", "Jo").get()
      check res.isEmpty == false
      check res.get(0).name == "John"

  test "whereStartsLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereStartsLike("name", "Jo").get()
      check res.isEmpty == false
      check res.get(0).name == "John"
  

  test "whereEndsLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereEndsLike("name", "hn").get()
      check res.isEmpty == false
      check res.get(0).name == "John"
  
  test "whereNotLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereNot("name", "Ghost").get()
      check res.isEmpty == false
      check res.get(0).name == "John"

  test "wereNotStartsLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereNotStartsLike("name", "Gh").get()
      check res.isEmpty == false
      check res.get(0).name == "John"

  test "whereNotEndsLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereNotEndsLike("name", "st").get()
      check res.isEmpty == false
      check res.get(0).name == "John"

suite "IN queries":
  test "whereNotIn query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereNotIn("name", "John").get()
      check res.isEmpty == true
{.pop.}

test "close embedded postgres":
  greskew.stop()
  greskew.dispose()