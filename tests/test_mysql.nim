import os, unittest, strutils, times

when defined(mysqlTests):
  import ../src/ozark/driver/mysql

  newModel Users:
    id {.pk.}: Serial
    username {.unique.}: Varchar(50)
    name: Varchar(100)
    email {.notnull, unique.}: Varchar(100)
    created_at {.notnull.}: TimestampTz
    updated_at: TimestampTz

  newModel SubscriptionPlan:
    id {.pk.}: Serial
    name {.notnull, unique.}: Varchar(50)
    price {.notnull.}: Money

  newModel Metrics:
    id {.pk.}: Serial
    hits: Int
    delta: Int

  suite "MySQL driver tests":
    setup:
      initOzarkDatabase("localhost", "ozark_test", "root", "root", Port(3306))
      withDB do:
        Models.table(Users).prepareTable().exec()
        Models.table(SubscriptionPlan).prepareTable().exec()
        Models.table(Metrics).prepareTable().exec()
        Models.table(Users).truncate().exec()
        Models.table(SubscriptionPlan).truncate().exec()
        Models.table(Metrics).truncate().exec()
      initOzarkPool(4)

    test "insert and select data (mysql)":
      withDBPool do:
        let id = Models.table(Users).insert({
          name: "Jane Doe",
          username: "janedoe",
          email: "janedoe@example.com",
          created_at: $now()
        }).execGet()
        let res = Models.table(Users).selectAll().where("id", $id).getAll()
        check res.isEmpty == false
        check res.entries[0].name == "Jane Doe"

    test "upsert (mysql)":
      withDBPool do:
        Models.table(Users).insert({
          name: "Alice", username: "alice", email: "alice@x.io", created_at: $now()
        }).exec()
        let before = Models.table(Users).selectAll().count()
        Models.table(Users).upsert({
          username: "alice",
          name: "Alice Updated",
          email: "alice@x.io",
          created_at: $now()
        }, ["username"], ["name"]).exec()
        check Models.table(Users).selectAll().count() == before
        let row = Models.table(Users).selectAll().where("username", "alice").get()
        check row.get(0).name == "Alice Updated"

    test "aggregates (mysql)":
      withDBPool do:
        Models.table(SubscriptionPlan).insertMany([
          {name: "basic", price: "10"},
          {name: "pro", price: "20"},
          {name: "team", price: "30"}
        ]).exec()
        check Models.table(SubscriptionPlan).selectAll().sumOf("price") == "60.00"
        check Models.table(SubscriptionPlan).selectAll().count() == 3

    test "transactions (mysql)":
      withDBPool do:
        Models.table(Metrics).truncate().exec()
        withTransaction do:
          discard Models.table(Metrics).insert({hits: "1", delta: "0"}).execGet()
          Models.table(Metrics).insert({hits: "2", delta: "0"}).exec()
        check Models.table(Metrics).selectAll().count() == 2

else:
  echo "Skipping MySQL tests (compile with -d:mysqlTests against a running MySQL)."
  quit(0)