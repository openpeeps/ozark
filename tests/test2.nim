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
      let id = Models.table(Users).insert({
        name: "John Doe",
        username: "johndoe",
        email: "johndoe@example.com",
      }).execGet() # returns the id of the inserted row

      let res = Models.table(Users).selectAll()
                      .where("id", $id)
                      .getAll()

      check res.isEmpty == false
      check parseInt(res.entries[0].id) == id
      check res.entries[0].name == "John Doe"
      check res.entries[0].username == "johndoe"
      check res.entries[0].email == "johndoe@example.com"

  test "select specific columns":
    withDBPool do:
      let res = Models.table(Users)
                      .select(["name", "email"])
                      .where("name", "John Doe").get()
      check res.isEmpty == false
      check res.get(0).name == "John Doe"
      check res.get(0).email == "johndoe@example.com"
      check res.get(0).username == "" # not selected, should be empty

suite "WHERE queries":
  test "where query":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .where("name", "John Doe").get()
      check res.isEmpty == false
      # check res.get(0).name == "John Doe"

  test "whereNot query":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .whereNot("name", "John Doe").get()
      check res.isEmpty

  test "orWhere query":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .where("name", "Ghost")
                      .orWhere("name", "John Doe").get()
      check res.isEmpty == false
      check res.get(0).name == "John Doe"
  
  test "orWhereNot query":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .whereNot("name", "John Doe")
                      .orWhereNot("name", "Ghost").get()
      check res.isEmpty == false
      check res.get(0).name == "John Doe"
  

suite "LIKE queries":
  test "like query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereLike("name", "Jo").get()
      check res.isEmpty == false
      check res.get(0).name == "John Doe"

  test "whereStartsLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereStartsLike("name", "Jo").get()
      check res.isEmpty == false
      check res.get(0).name == "John Doe"
  

  test "whereEndsLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereEndsLike("name", "oe").get()
      check res.isEmpty == false
      check res.get(0).name == "John Doe"
  
  test "whereNotLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereNot("name", "Ghost").get()
      check res.isEmpty == false
      check res.get(0).name == "John Doe"

  test "wereNotStartsLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereNotStartsLike("name", "Gh").get()
      check res.isEmpty == false
      check res.get(0).name == "John Doe"

  test "whereNotEndsLike query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereNotEndsLike("name", "st").get()
      check res.isEmpty == false
      check res.get(0).name == "John Doe"

suite "IN queries":
  test "whereNotIn query":
    withDBPool do:
      let res = Models.table(Users).select("name")
                      .whereNotIn("name", "John Doe").get()
      check res.isEmpty == true

suite "RAW queries":
  test "raw where query":
    withDBPool do:
      let res = Models.rawSQL("SELECT name FROM users WHERE name = $1", "Alice")
                       .getWith(Users)
      assert res.isEmpty

{.pop.}

test "close embedded postgres":
  greskew.stop()
  greskew.dispose()