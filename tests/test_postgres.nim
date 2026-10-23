import os, unittest, strutils, times

const pqlibPath = currentSourcePath().parentDir / "greskewelbox" / "bin" / "16.9.0" / "darwin" / "lib"
const greskewel_lib = pqlibPath / "libpq.dylib"

# Make libpq.dylib (downloaded by test1 via greskewel) discoverable at runtime
{.passl: "-Wl,-rpath," & pqlibPath.}
# Skip the whole module when the embedded-postgres binaries are not present yet
when not fileExists(pqlibPath / "libpq.dylib"):
  {.error: "test2 requires greskewel binaries; run `clue test` once so test1 downloads them first".}

import pkg/greskewel
import ../src/ozark/driver/psql

var greskew = initEmbeddedPostgres(
  PostgresConfig(
    basePath: getCurrentDir() / "tests" / "greskewelbox",
  )
)

newModel Users:
  id {.pk.}: Serial
  username {.unique.}: Varchar(50)
  name: Varchar(100)
  email {.notnull, unique.}: Varchar(100)
  created_at {.notnull.}: TimestampTz

newModel SubscriptionPlan:
  id {.pk.}: Serial
  name {.notnull, unique.}: Varchar(50)
  price {.notnull.}: Money

newModel Subscriptions:
  id {.pk.}: Serial
  user_id: Users.id
  plan {.notnull.}: SubscriptionPlan.id
  created_at {.notnull.}: TimestampTz

{.push dynlib: greskewel_lib.}
test "init embedded postgres and create tables":
  greskew.init()
  greskew.start()

  initOzarkDatabase("localhost", "postgres", "postgres",
                      "postgres", Port(5432), driver = SqlDriver.pgsql)
  withDB do:
    Models.table(Users).prepareTable().exec()
    Models.table(Users).dropTable(cascade = true).exec()
    Models.table(Users).prepareTable().exec()
    Models.table(SubscriptionPlan).prepareTable().exec()
    Models.table(Subscriptions).prepareTable().exec()

  initOzarkPool(15)
  sleep(200)

suite "INSERT and SELECT queries":
  test "insert and select data":
    withDBPool do:
      let id = Models.table(Users).insert({
        name: "John Doe",
        username: "johndoe",
        email: "johndoe@example.com",
        created_at: $now()
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
                      .whereNotIn("name", ["John Doe"])
                      .get()
      check res.isEmpty == true

# suite "Resumable Queries":
#   test "chain where clauses based on runtime conditions":
#     withDBPool do:
#       var baseQuery = Models.table(Users).select("name").extractSQL()
#       let filterByName = true
#       if filterByName:
#         baseQuery = baseQuery.fromSQL.where("name", "John Doe").extractSQL()

#       baseQuery = baseQuery.fromSQL().whereIn("email", "johndoe@example.com").extractSQL()
#       let res = baseQuery.fromSQL().getAll()
#       check res.isEmpty == false
#       check res.get(0).name == "John Doe"

#   test "chain where clauses with whereNot based on runtime conditions":
#     withDBPool do:
#       var baseQuery = Models.table(Users).select("name").extractSQL()
#       var emailAddress: string
#       let filterByName = true
#       if filterByName:
#         emailAddress = "johndoe@example.com"
#         baseQuery = baseQuery.fromSQL().whereNot("name", "Ghost").extractSQL()
#       else:
#         emailAddress = "none@example.com"

#       baseQuery = baseQuery.fromSQL().whereIn("email", emailAddress).extractSQL()
#       let res = baseQuery.fromSQL().getAll()
#       check res.isEmpty == false
#       check res.get(0).name == "John Doe"

suite "RAW queries":
  test "raw where query":
    withDBPool do:
      let res = Models.rawSQL("SELECT name FROM users WHERE name = $1", "Alice")
                       .getWith(Users)
      assert res.isEmpty
  test "raw query with subqueries":
    withDBPool do:
      let res = Models.rawSQL("""
SELECT 
  users.*,
  (SELECT COUNT(*) FROM subscriptions WHERE subscriptions.user_id = users.id) AS subscriptions_count
FROM users ORDER BY users.id DESC LIMIT 20 OFFSET 0;""").getWith(Users)

newModel PgMetrics:
  id {.pk.}: Serial
  hits: Int
  note: Varchar(50)

suite "Tier 1 query builder (pg)":
  test "truncate, insertMany and count":
    withDBPool do:
      Models.table(Subscriptions).truncate().exec()
      Models.table(SubscriptionPlan).truncate().exec()
      Models.table(Users).truncate().exec()

      Models.table(Users).insertMany([
        {username: "alice", name: "Alice", email: "alice@x.io", created_at: $now()},
        {username: "bob", name: "Bob", email: "bob@x.io", created_at: $now()},
        {username: "carol", name: "Carol", email: "carol@x.io", created_at: $now()}
      ]).exec()

      check Models.table(Users).selectAll().count() == 3

  test "findById, first and returning":
    withDBPool do:
      # pg sequences do not reset on TRUNCATE, so capture the real id
      let ins = Models.table(Users).insert({
        username: "dave",
        name: "Dave",
        email: "dave@x.io",
        created_at: $now()
      }).returning(["id", "name"])
      check ins.len == 1
      check ins[0][1] == "Dave"

      let res = Models.table(Users).findById(ins[0][0]).get()
      check res.isEmpty == false
      check res.get(0).username == "dave"

      let firstRes = Models.table(Users).selectAll()
                          .where("name", "Bob").first()
      check firstRes.get(0).email == "bob@x.io"

  test "orderBy and pagination":
    withDBPool do:
      let asc = Models.table(Users).selectAll()
                      .orderAscBy(["username"]).getAll()
      check asc.len == 4
      check asc.get(0).username == "alice"

      let desc = Models.table(Users).selectAll()
                        .orderDescBy(["username"]).getAll()
      check desc.get(0).username == "dave"

      let page = Models.table(Users).selectAll()
                       .orderDescBy(["username"])
                       .limit(2).offset(1).getAll()
      check page.len == 2
      check page.get(0).username == "carol"
      check page.get(1).username == "bob"

      let paginated = Models.table(Users).selectAll()
                            .orderAscBy(["username"]).paginate(2, 2).getAll()
      check paginated.get(0).username == "carol"

  test "predicates":
    withDBPool do:
      Models.table(PgMetrics).prepareTable().exec()
      Models.table(PgMetrics).truncate().exec()
      Models.table(PgMetrics).insertMany([
        {hits: "10", note: "a"},
        {hits: "20", note: "b"},
        {hits: "30", note: "c"}
      ]).exec()

      check Models.table(PgMetrics).select("id").whereGt("hits", "15").getAll().len == 2
      check Models.table(PgMetrics).select("id").whereLte("hits", "20").getAll().len == 2
      check Models.table(PgMetrics).select("id").whereBetween("hits", "15", "25").getAll().len == 1
      check Models.table(PgMetrics).select("id").whereNotNull("note").getAll().len == 3

      # AND-chained whereIn must keep placeholder indexes consistent
      let none = Models.table(PgMetrics).select("id")
                       .where("hits", "10").whereIn("note", ["b"]).get()
      check none.isEmpty

      let orIn = Models.table(PgMetrics).select("id")
                       .where("hits", "10").orWhereIn("hits", ["20", "30"]).getAll()
      check orIn.len == 3

  test "unique, groupBy and having":
    withDBPool do:
      let names = Models.table(Users).select(["name"]).unique().getAll()
      check names.len == 4

      let groups = Models.table(Users).select(["name"])
                         .groupBy(["name"])
                         .havingGte("name", "B").getAll()
      check groups.len == 3

  test "upsert and increment/decrement":
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

      Models.table(PgMetrics).prepareTable().exec()
      Models.table(PgMetrics).truncate().exec()
      let mIns = Models.table(PgMetrics).insert({hits: "10", note: "n"}).returning(["id"])

      Models.table(PgMetrics).increment("hits", 5).where("id", mIns[0][0]).exec()
      check Models.table(PgMetrics).findById(mIns[0][0]).get().get(0).hits == "15"

      Models.table(PgMetrics).decrement("hits", 3).where("id", mIns[0][0]).exec()
      check Models.table(PgMetrics).findById(mIns[0][0]).get().get(0).hits == "12"
{.pop.}

suite "Tier 2 transactions & terminals (pg)":
  test "transaction commits on success":
    withDBPool do:
      Models.table(PgMetrics).truncate().exec()
      withTransaction do:
        discard Models.table(PgMetrics).insert({hits: "1", note: "a"}).execGet()
        Models.table(PgMetrics).insert({hits: "2", note: "b"}).exec()
      check Models.table(PgMetrics).selectAll().count() == 2

  test "transaction rolls back when the body raises":
    withDBPool do:
      Models.table(PgMetrics).truncate().exec()
      var raised = false
      try:
        withTransaction do:
          Models.table(PgMetrics).insert({hits: "7", note: "x"}).exec()
          raise newException(CatchableError, "boom")
      except CatchableError:
        raised = true
      check raised
      check Models.table(PgMetrics).selectAll().count() == 0

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
      Models.table(PgMetrics).truncate().exec()
      Models.table(PgMetrics).insertMany([
        {hits: "10", note: "a"},
        {hits: "20", note: "b"},
        {hits: "30", note: "c"}
      ]).exec()
      let rows = Models.table(PgMetrics).selectAll()
                      .orderAscBy(["hits"]).limit(2).getRaw()
      check rows.len == 2
      check rows[0][1] == "10"
      check rows[1][1] == "20"

  test "exists chained and table-level":
    withDBPool do:
      Models.table(PgMetrics).truncate().exec()
      check Models.table(PgMetrics).exists() == false
      check Models.table(PgMetrics).selectAll().where("hits", "10").exists() == false

      let ins = Models.table(PgMetrics).insert({hits: "10", note: "a"}).returning(["id"])
      check ins.len == 1

      check Models.table(PgMetrics).exists() == true
      check Models.table(PgMetrics).selectAll().where("hits", "10").exists() == true
      check Models.table(PgMetrics).selectAll().where("hits", "99").exists() == false

  test "last orders by id":
    withDBPool do:
      Models.table(PgMetrics).truncate().exec()
      discard Models.table(PgMetrics).insert({hits: "5", note: "a"}).execGet()
      discard Models.table(PgMetrics).insert({hits: "9", note: "b"}).execGet()
      let l = Models.table(PgMetrics).selectAll().last()
      check l.isEmpty == false
      check l.get(0).hits == "9"

  test "pluck collects a single column":
    withDBPool do:
      Models.table(PgMetrics).truncate().exec()
      Models.table(PgMetrics).insertMany([
        {hits: "3", note: "a"},
        {hits: "6", note: "b"}
      ]).exec()
      let vals = Models.table(PgMetrics).selectAll()
                      .orderAscBy(["hits"]).pluck("hits")
      check vals == @["3", "6"]

test "close embedded postgres":
  greskew.stop()
  greskew.dispose()