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

test "run raw sql queries":

  greskew.init()
  greskew.start()

  {.push dynlib: greskewel_lib.}
  initOzarkDatabase("localhost", "postgres", "postgres", "postgres", Port(5432))
  withDB do:
    Models.rawSQL("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username VARCHAR(50), name VARCHAR(100), email VARCHAR(100))")
          .exec()
  {.pop.}

test "insert and select data":
  {.push dynlib: greskewel_lib.}
  withDB do:
    let id = Models.table("users").insert({
      name: "John",
      username: "john1232",
      email: "test@example.com",
    }).execGet() # returns the id of the inserted row

    
    let res = Models.table("users").select("*").where("id", $id).getAll(Users)
    check res.isEmpty == false
    check parseInt(res.entries[0].id) == id
    check res.entries[0].name == "John"
    check res.entries[0].username == "john1232"
    check res.entries[0].email == "test@example.com"
  {.pop.}

test "whereLike query":
  {.push dynlib: greskewel_lib.}
  withDB do:
    let res = Models.table("users").select("name").whereLike("name", "Jo").get(Users)
    check res.isEmpty == false
    check res.get(0).name == "John"
  {.pop.}

test "whereStartsLike query":
  {.push dynlib: greskewel_lib.}
  withDB do:
    let res = Models.table("users").select("name").whereStartsLike("name", "Jo").get(Users)
    check res.isEmpty == false
    check res.get(0).name == "John"
  {.pop.}

test "whereEndsLike query":
  {.push dynlib: greskewel_lib.}
  withDB do:
    let res = Models.table("users").select("name").whereEndsLike("name", "hn").get(Users)
    check res.isEmpty == false
    check res.get(0).name == "John"
  {.pop.}

test "whereNotLike query":
  {.push dynlib: greskewel_lib.}
  withDB do:
    let res = Models.table("users").select("name").whereNot("name", "Ghost").get(Users)
    check res.isEmpty == false
    check res.get(0).name == "John"
  {.pop.}  

test "wereNotStartsLike query":
  {.push dynlib: greskewel_lib.}
  withDB do:
    let res = Models.table("users").select("name").whereNotStartsLike("name", "Gh").get(Users)
    check res.isEmpty == false
    check res.get(0).name == "John"
  {.pop.}

test "whereNotEndsLike query":
  {.push dynlib: greskewel_lib.}
  withDB do:
    let res = Models.table("users").select("name").whereNotEndsLike("name", "st").get(Users)
    check res.isEmpty == false
    check res.get(0).name == "John"
  {.pop.}


test "close embedded postgres":
  greskew.stop()
  greskew.dispose()