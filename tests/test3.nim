import os, unittest, strutils, times
import ../src/ozark/driver/sqlite

newModel Users:
  id {.pk.}: Integer
  username {.unique.}: Varchar(50)
  name: Varchar(100)
  email {.notnull, unique.}: Varchar(100)
  created_at {.notnull.}: TimestampTz
  updated_at: TimestampTz

newModel SubscriptionPlan:
  id {.pk.}: Integer
  name {.notnull, unique.}: Varchar(50)
  price {.notnull.}: Money

newModel Subscriptions:
  id {.pk.}: Integer
  user_id: Users.id
  plan {.notnull.}: SubscriptionPlan.id
  created_at {.notnull.}: TimestampTz

suite "SQLite driver tests":
  test "init sqlite and create tables":
    # Use an in-memory SQLite database for testing
    initOzarkDatabase("mytest.db")
    withDB do:
      Models.table(Users).prepareTable().exec()
      Models.table(Users).dropTable(cascade = true).exec()
      Models.table(Users).prepareTable().exec()
      Models.table(SubscriptionPlan).prepareTable().exec()
      Models.table(Subscriptions).prepareTable().exec()

  test "insert and select data (sqlite)":
    withDBPool do:
      let id = Models.table(Users).insert({
        name: "Jane Doe",
        username: "janedoe",
        email: "janedoe@example.com",
        created_at: $now()
      }).execGet()

      let res = Models.table(Users).selectAll()
                      .where("id", $id)
                      .getAll()
      check res.isEmpty == false
      check res.entries[0].name == "Jane Doe"
      check res.entries[0].username == "janedoe"
      check res.entries[0].email == "janedoe@example.com"

  test "update data (sqlite)":
    withDBPool do:
      Models.table(Users).update({
        name: "John Doe",
        username: "johndoe",
        email: "test@example.com",
        updated_at: $now()
      }).where("id", "1").exec()

  test "where and like queries (sqlite)":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .where("username", "johndoe").get()
      check res.isEmpty == false
      
      let likeRes = Models.table(Users)
                         .select("name")
                         .whereLike("name", "Jo").get()
      check likeRes.isEmpty == false
      check likeRes.get(0).name == "John Doe"

  test "in and not in queries (sqlite)":
    withDBPool do:
      let res = Models.table(Users)
                      .select("name")
                      .whereIn("name", ["John Doe"]).get()
      check res.isEmpty == false
      let notInRes = Models.table(Users)
                         .select("name")
                         .whereNotIn("name", ["John Doe"]).get()
      check notInRes.isEmpty == true

  # test "raw query (sqlite)":
  #   withDBPool do:
  #     let res = Models.rawSQL("SELECT name FROM users WHERE name = ?", "Jane Doe")
  #                     .getWith(Users)
  #     check res.isEmpty == false
  #     check res.get(0).name == "Jane Doe"