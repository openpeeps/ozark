# Runnable example for Ozark ORM using the SQLite driver.
#
# Run with:
#   cd examples && nim c -r sqlite.nim
# (or `clue build examples/sqlite.nim` from the package root)

import os, strutils, times

import ozark/model
import ozark/query
import ozark/driver/sqlite

# --- Models ---------------------------------------------------------------

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

newModel Metrics:
  id {.pk.}: Integer
  hits: Int
  delta: Int

# --- Main -----------------------------------------------------------------

proc main() =
  initOzarkDatabase("ozark_example.db")
  withDB do:
    Models.table(Users).prepareTable().exec()
    Models.table(SubscriptionPlan).prepareTable().exec()
    Models.table(Metrics).prepareTable().exec()
    # start clean
    Models.table(Users).truncate().exec()
    Models.table(SubscriptionPlan).truncate().exec()
    Models.table(Metrics).truncate().exec()

  initOzarkPool(4)

  withDBPool do:
    # ---- Insert ---------------------------------------------------------
    let id = Models.table(Users).insert({
      name: "John Doe",
      username: "johndoe",
      email: "johndoe@example.com",
      created_at: $now(),
    }).execGet()
    echo "inserted id = ", id

    Models.table(Users).insertMany([
      {username: "alice", name: "Alice", email: "alice@x.io", created_at: $now()},
      {username: "bob", name: "Bob", email: "bob@x.io", created_at: $now()},
      {username: "carol", name: "Carol", email: "carol@x.io", created_at: $now()},
    ]).exec()

    Models.table(SubscriptionPlan).insertMany([
      {name: "basic", price: "10"},
      {name: "pro", price: "20"},
      {name: "team", price: "30"},
    ]).exec()

    # ---- Select --------------------------------------------------------
    let all = Models.table(Users).selectAll().getAll()
    echo "users: ", all.len
    for u in all: echo "  ", u.username, " <" & u.email & ">"

    let one = Models.table(Users).findById($id).get()
    if one.len > 0:
      echo "found: ", one.get(0).name

    # ---- Where filters --------------------------------------------------
    let filtered = Models.table(Users).select(["name", "email"])
                      .where("name", "Bob").get()
    if filtered.len > 0:
      echo "Bob's email: ", filtered.get(0).email

    let likeRes = Models.table(Users).select("name")
                      .whereLike("name", "Jo").get()
    if likeRes.len > 0:
      echo "like Jo: ", likeRes.get(0).name

    let inRes = Models.table(Users).select("name")
                  .whereIn("name", ["Alice", "Bob"]).getAll()
    echo "in(Alice,Bob): ", inRes.len

    let rangeRes = Models.table(Users).select("id")
                    .whereBetween("id", "1", "3").getAll()
    echo "between 1..3: ", rangeRes.len

    let nulls = Models.table(Users).select("id").whereNull("updated_at").getAll()
    echo "null updated_at: ", nulls.len

    # ---- Ordering / pagination ----------------------------------------
    let asc = Models.table(Users).selectAll()
                  .orderAscBy(["username"]).getAll()
    echo "first by username: ", asc.get(0).username

    let page = Models.table(Users).selectAll()
                  .orderAscBy(["username"]).limit(2).offset(1).getAll()
    echo "page (offset 1, limit 2): ", page.len, " -> ", page.get(0).username

    let paginated = Models.table(Users).selectAll()
                      .orderAscBy(["username"]).paginate(2, 2).getAll()
    echo "paginate(2,2): ", paginated.get(0).username

    # ---- Group by / having / aggregates -------------------------------
    discard Models.table(Users).insert({
      username: "alice2", name: "Alice", email: "alice2@x.io", created_at: $now()
    }).execGet()

    let names = Models.table(Users).select(["name"]).unique().getAll()
    echo "distinct names: ", names.len

    let groups = Models.table(Users).select(["name"])
                    .groupBy(["name"]).havingGte("name", "B").getAll()
    echo "groups >= B: ", groups.len

    echo "count users: ", Models.table(Users).selectAll().count()
    echo "sum prices:  ", Models.table(SubscriptionPlan).selectAll().sumOf("price")
    echo "min price:   ", Models.table(SubscriptionPlan).selectAll().minOf("price")
    echo "max price:   ", Models.table(SubscriptionPlan).selectAll().maxOf("price")
    echo "avg price:   ", Models.table(SubscriptionPlan).selectAll().avgOf("price")
    echo "distinct prices: ", Models.table(SubscriptionPlan).selectAll().countDistinct("price")

    let filteredSum = Models.table(SubscriptionPlan).selectAll()
                        .whereGte("price", "20").sumOf("price")
    echo "sum (price>=20): ", filteredSum

    # ---- Update / counters --------------------------------------------
    Models.table(Users).update({name: "John Doe Updated"}).where("id", $id).exec()
    let updated = Models.table(Users).findById($id).get()
    if updated.len > 0: echo "updated name: ", updated.get(0).name

    Models.table(Metrics).insert({hits: "10", delta: "0"}).exec()
    let mId = Models.table(Metrics).selectAll().last().get(0).id
    Models.table(Metrics).increment("hits", 5).where("id", mId).exec()
    Models.table(Metrics).decrement("hits").where("id", mId).exec()
    let bumped = Models.table(Metrics).findById(mId).get()
    if bumped.len > 0: echo "hits after +/-: ", bumped.get(0).hits

    # ---- Upsert --------------------------------------------------------
    let before = Models.table(Users).selectAll().count()
    Models.table(Users).upsert({
      username: "alice",
      name: "Alice Updated",
      email: "alice@x.io",
      created_at: $now(),
    }, ["username"], ["name"]).exec()
    echo "upsert kept count: ", Models.table(Users).selectAll().count(), " (was ", before, ")"

    # ---- Terminals -----------------------------------------------------
    let raw = Models.table(Users).selectAll().limit(2).getRaw()
    echo "raw rows: ", raw.len
    let plucked = Models.table(Metrics).selectAll()
                    .orderAscBy(["hits"]).pluck("hits")
    echo "plucked hits: ", plucked
    echo "exists users: ", Models.table(Users).exists()
    echo "exists ghost: ", Models.table(Users).selectAll().where("name", "Ghost").exists()

    # ---- Raw SQL -------------------------------------------------------
    # (rawSQL with bound params is supported; see README. Here we keep it
    #  param-free to stay on the simplest code path.)
    let rawRes = Models.rawSQL("SELECT name FROM users").getRaw()
    echo "rawSQL rows: ", rawRes.len

    # ---- Transactions --------------------------------------------------
    Models.table(Metrics).truncate().exec()
    withTransaction do:
      discard Models.table(Metrics).insert({hits: "1", delta: "0"}).execGet()
      Models.table(Metrics).insert({hits: "2", delta: "0"}).exec()
    echo "tx committed rows: ", Models.table(Metrics).selectAll().count()

    # rollback demo
    var raised = false
    try:
      withTransaction do:
        Models.table(Metrics).insert({hits: "7", delta: "0"}).exec()
        raise newException(CatchableError, "boom")
    except CatchableError:
      raised = true
    echo "rolled back, raised=", raised, " rows=", Models.table(Metrics).selectAll().count()

  echo "done."

main()