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

newModel Metrics:
  id {.pk.}: Integer
  hits: Int
  delta: Int

suite "Tier 1 query builder (sqlite)":
  test "truncate, insertMany and count":
    withDBPool do:
      Models.table(Subscriptions).truncate().exec()
      Models.table(SubscriptionPlan).truncate().exec()
      Models.table(Users).truncate().exec()

      Models.table(Users).insertMany([
        {username: "alice", name: "Alice", email: "alice@x.io", created_at: $now()},
        {username: "bob", name: "Bob", email: "bob@x.io", created_at: $now()},
        {username: "carol", name: "Carol", email: "carol@x.io", created_at: $now()},
        {username: "dave", name: "Dave", email: "dave@x.io", created_at: $now()}
      ]).exec()

      let total = Models.table(Users).selectAll().count()
      check total == 4

  test "find and first":
    withDBPool do:
      let res = Models.table(Users).findById("2").get()
      check res.isEmpty == false
      check res.get(0).name == "Bob"

      let firstRes = Models.table(Users).selectAll()
                          .where("name", "Carol").first()
      check firstRes.isEmpty == false
      check firstRes.get(0).username == "carol"

  test "orderBy asc and desc":
    withDBPool do:
      let asc = Models.table(Users).selectAll()
                      .orderAscBy(["username"]).getAll()
      check asc.len == 4
      check asc.get(0).username == "alice"
      check asc.get(3).username == "dave"

      let desc = Models.table(Users).selectAll()
                        .orderDescBy(["username"]).getAll()
      check desc.get(0).username == "dave"

      let generic = Models.table(Users).select("username")
                          .orderBy(["username"], Asc).getAll()
      check generic.get(0).username == "alice"

  test "limit after bare select":
    withDBPool do:
      let res = Models.table(Users).selectAll().limit(1).getAll()
      check res.len == 1

  test "offset and paginate":
    withDBPool do:
      let res = Models.table(Users).selectAll()
                      .orderAscBy(["username"])
                      .limit(2).offset(1).getAll()
      check res.len == 2
      check res.get(0).username == "bob"
      check res.get(1).username == "carol"

      let page = Models.table(Users).selectAll()
                       .orderAscBy(["username"])
                       .paginate(2, 2).getAll()
      check page.len == 2
      check page.get(0).username == "carol"
      check page.get(1).username == "dave"

  test "comparison predicates":
    withDBPool do:
      check Models.table(Users).select("id").whereGt("id", "2").getAll().len == 2
      check Models.table(Users).select("id").whereGte("id", "3").getAll().len == 2
      check Models.table(Users).select("id").whereLt("id", "3").getAll().len == 2
      check Models.table(Users).select("id").whereLte("id", "2").getAll().len == 2

  test "null predicates":
    withDBPool do:
      let nulls = Models.table(Users).select("id").whereNull("updated_at").getAll()
      check nulls.len == 4

      Models.table(Users).update({updated_at: $now()}).where("id", "1").exec()

      let notNulls = Models.table(Users).select("id").whereNotNull("updated_at").getAll()
      check notNulls.len == 1

  test "between predicates":
    withDBPool do:
      let inside = Models.table(Users).select("id")
                         .whereBetween("id", "2", "3").getAll()
      check inside.len == 2
      let outside = Models.table(Users).select("id")
                           .whereNotBetween("id", "2", "3").getAll()
      check outside.len == 2

  test "chained IN/LIKE keep placeholder indexes":
    withDBPool do:
      # AND-chained whereIn after a where must not collide placeholders
      let none = Models.table(Users).select("id")
                       .where("id", "1").whereIn("name", ["Bob"]).get()
      check none.isEmpty

      let orIn = Models.table(Users).select("id")
                       .where("id", "1")
                       .orWhereIn("name", ["Bob", "Carol"]).getAll()
      check orIn.len == 3

      let likeChain = Models.table(Users).select("username")
                             .where("id", "4")
                             .whereStartsLike("username", "da").get()
      check likeChain.isEmpty == false
      check likeChain.get(0).username == "dave"

  test "distinct via unique":
    withDBPool do:
      discard Models.table(Users).insert({
        username: "alice2",
        name: "Alice",
        email: "alice2@x.io",
        created_at: $now()
      }).execGet()

      let names = Models.table(Users).select(["name"]).unique().getAll()
      check names.len == 4
      check names.contains("name", "Alice")

  test "groupBy and having":
    withDBPool do:
      let eq = Models.table(Users).select(["name"])
                     .groupBy(["name"])
                     .having("name", "Alice").getAll()
      check eq.len == 1

      let gte = Models.table(Users).select(["name"])
                       .groupBy(["name"])
                       .havingGte("name", "B").getAll()
      check gte.len == 3

  test "aggregates":
    withDBPool do:
      Models.table(SubscriptionPlan).insertMany([
        {name: "basic", price: "10"},
        {name: "pro", price: "20"},
        {name: "team", price: "30"}
      ]).exec()

      check Models.table(SubscriptionPlan).selectAll().count() == 3
      check Models.table(SubscriptionPlan).selectAll().sumOf("price") == "60"
      check Models.table(SubscriptionPlan).selectAll().minOf("price") == "10"
      check Models.table(SubscriptionPlan).selectAll().maxOf("price") == "30"
      check Models.table(SubscriptionPlan).selectAll().avgOf("price") == "20.0"
      check Models.table(SubscriptionPlan).selectAll().countDistinct("price") == "3"

      # aggregates over filtered queries carry bound parameters
      let filtered = Models.table(SubscriptionPlan).selectAll()
                             .whereGte("price", "20").sumOf("price")
      check filtered == "50"

  test "upsert":
    withDBPool do:
      let before = Models.table(Users).selectAll().count()

      Models.table(Users).upsert({
        username: "alice",
        name: "Alice Updated",
        email: "alice@x.io",
        created_at: $now()
      }, ["username"], ["name"]).exec()

      check Models.table(Users).selectAll().count() == before

      let row = Models.table(Users).selectAll()
                      .where("username", "alice").get()
      check row.get(0).name == "Alice Updated"

  test "increment and decrement":
    withDBPool do:
      Models.table(Metrics).prepareTable().exec()
      Models.table(Metrics).truncate().exec()

      let id = Models.table(Metrics).insert({
        hits: "10",
        delta: "0"
      }).execGet()

      Models.table(Metrics).increment("hits", 5).where("id", $id).exec()
      let bumped = Models.table(Metrics).findById($id).get()
      check bumped.get(0).hits == "15"

      Models.table(Metrics).decrement("hits").where("id", $id).exec()
      let dropped = Models.table(Metrics).findById($id).get()
      check dropped.get(0).hits == "14"
suite "Tier 2 transactions & terminals (sqlite)":
  test "transaction commits on success":
    withDBPool do:
      Models.table(Metrics).truncate().exec()
      withTransaction do:
        discard Models.table(Metrics).insert({hits: "1", delta: "0"}).execGet()
        Models.table(Metrics).insert({hits: "2", delta: "0"}).exec()
      check Models.table(Metrics).selectAll().count() == 2

  test "transaction rolls back when the body raises":
    withDBPool do:
      Models.table(Metrics).truncate().exec()
      var raised = false
      try:
        withTransaction do:
          Models.table(Metrics).insert({hits: "7", delta: "0"}).exec()
          raise newException(CatchableError, "boom")
      except CatchableError:
        raised = true
      check raised
      check Models.table(Metrics).selectAll().count() == 0

  test "transaction rolls back on constraint violation":
    withDBPool do:
      let before = Models.table(Users).selectAll().count()
      var raised = false
      try:
        withTransaction do:
          Models.table(Users).insert({
            username: "tx1",
            name: "Tx",
            email: "tx1@x.io",
            created_at: $now()
          }).exec()
          # duplicate unique email aborts the whole transaction
          Models.table(Users).insert({
            username: "tx2",
            name: "Tx2",
            email: "alice@x.io",
            created_at: $now()
          }).exec()
      except DbError:
        raised = true
      check raised
      check Models.table(Users).selectAll().count() == before

  test "nested transaction raises":
    withDBPool do:
      var nestedRaised = false
      withTransaction do:
        try:
          withTransaction do:
            discard
        except OzarkConnectionError:
          nestedRaised = true
      check nestedRaised

  test "getRaw returns raw rows":
    withDBPool do:
      Models.table(Metrics).truncate().exec()
      Models.table(Metrics).insertMany([
        {hits: "10", delta: "0"},
        {hits: "20", delta: "0"},
        {hits: "30", delta: "0"}
      ]).exec()
      let rows = Models.table(Metrics).selectAll()
                      .orderAscBy(["hits"]).limit(2).getRaw()
      check rows.len == 2
      check rows[0][1] == "10"
      check rows[1][1] == "20"

  test "exists chained and table-level":
    withDBPool do:
      Models.table(Metrics).truncate().exec()
      check Models.table(Metrics).exists() == false
      check Models.table(Metrics).selectAll().where("hits", "10").exists() == false

      discard Models.table(Metrics).insert({hits: "10", delta: "0"}).execGet()

      check Models.table(Metrics).exists() == true
      check Models.table(Metrics).selectAll().where("hits", "10").exists() == true
      check Models.table(Metrics).selectAll().where("hits", "99").exists() == false

  test "last orders by id":
    withDBPool do:
      Models.table(Metrics).truncate().exec()
      discard Models.table(Metrics).insert({hits: "5", delta: "0"}).execGet()
      discard Models.table(Metrics).insert({hits: "9", delta: "0"}).execGet()
      let l = Models.table(Metrics).selectAll().last()
      check l.isEmpty == false
      check l.get(0).hits == "9"

  test "pluck collects a single column":
    withDBPool do:
      Models.table(Metrics).truncate().exec()
      Models.table(Metrics).insertMany([
        {hits: "3", delta: "0"},
        {hits: "6", delta: "0"}
      ]).exec()
      let vals = Models.table(Metrics).selectAll()
                      .orderAscBy(["hits"]).pluck("hits")
      check vals == @["3", "6"]
