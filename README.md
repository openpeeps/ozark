<p align="center">
  <img alt="Ozark ORM" src="https://github.com/openpeeps/ozark/blob/main/.github/ozark-logo-v21.png" width="250px" height="125px"><br>
  A magical ORM for the Nim language 👑
</p>

<p align="center">
  <code>nimble install ozark</code>
</p>

<p align="center">
  <a href="https://openpeeps.github.io/ozark/">API reference</a><br>
  <img src="https://github.com/openpeeps/ozark/workflows/test/badge.svg" alt="Github Actions">  <img src="https://github.com/openpeeps/ozark/workflows/docs/badge.svg" alt="Github Actions">
</p>

## 😍 Key Features
- [x] Macro-based query builder with a fluent API
- [x] Compile-time SQL validation & type safety
- [x] Support for PostgreSQL and SQLite
- [x] Transactions (`withTransaction`) with automatic commit/rollback
- [x] Upserts, batch inserts, aggregates, pagination, raw SQL with bound params
- [ ] Async query execution (coming soon)
- [x] Pluginkit-backed migration system with compile-time validated DSL

> [!NOTE]
> Ozark is still in active development. Expect bugs and breaking changes. Contributions are welcome!

## Examples

A complete, runnable example lives in [`examples/sqlite.nim`](examples/sqlite.nim)
(SQLite, no external services). Compile and run it with:

```bash
cd examples && nim c -r sqlite.nim
```

### Connecting to the database
Initialize the database connection with the given parameters

```nim
import ozark/database

initOzarkDatabase("localhost", "mydb", "myuser", "mypassword", 5432.Port)
```

#### `withDB` or `withDBPool`
Use `withDB` to execute queries then automatically close the connection after the block is executed. Use `withDBPool` to execute queries using a connection pool for better performance in concurrent scenarios.

```nim
withDB do:
  # execute queries here

withDBPool do:
  # execute queries here
```

#### `withTransaction`
Run a block inside a database transaction on a dedicated pooled connection. Commits on success, rolls back when the block raises, and always returns the connection to the pool.

```nim
withTransaction do:
  Models.table(Users).insert({...}).exec()
  Models.table(Audit).insert({...}).exec()
```

### Define a Model
Define a model by creating a new type that inherits from `Model` and specifying the fields with their types. See Ozark's Types documentation for supported field types and options. 

```nim
import ozark/model

newModel Users:
  id: Serial
  username: Varchar(50)
  name: Varchar(100)
  email: Varchar(100)

newModel Subscription:
  id: Serial
  user_id: Users.id
  plan: Varchar(50)
  status: Varchar(20)
  created_at: TimestampTz
  updated_at: TimestampTz
```

### Create the tables
TO create a database table you can use the `prepareTable` macro.
```nim
withDB do:
  Models.table(Users).prepareTable().exec()
```

#### Drop a table
```nim
withDB do:
  Models.table(Users).dropTable(cascade = true).exec()
```

### Querying the database
For simple queries, use the macro-based query builder. It provides a fluent API for constructing SQL in a type-safe way. Model and column names are validated at compile time. All `where*` values are bound parameters (no string interpolation, no SQL injection).

Every read chain starts with `selectAll`, `select(col|cols)` or `findById`, adds optional filter/sort clauses, and ends with a terminal (`get`, `getAll`, `getRaw`, `pluck`, `exists`, `count`, etc.). Every write chain ends with `exec`, `execGet` or `returning` (PostgreSQL only).

#### Insert

```nim
import ozark/query

withDBPool do:
  # single row, returns the new id
  let id = Models.table(Users).insert({
    name: "John Doe",
    username: "johndoe",
    email: "johndoe@example.com",
  }).execGet()

  # multiple rows in one statement (all rows must share the same columns)
  Models.table(Users).insertMany([
    {username: "alice", name: "Alice", email: "alice@x.io"},
    {username: "bob", name: "Bob", email: "bob@x.io"},
  ]).exec()

  # upsert on conflict (PostgreSQL + SQLite 3.24+).
  # Empty updateCols => DO NOTHING
  Models.table(Users).upsert({
    username: "alice",
    name: "Alice Updated",
    email: "alice@x.io",
  }, ["username"], ["name"]).exec()

  # PostgreSQL only: return arbitrary columns
  let rows = Models.table(Users).insert({
    username: "dave", name: "Dave", email: "dave@x.io"
  }).returning(["id", "name"]) # seq[seq[string]]
```

#### Select

```nim
withDBPool do:
  # all columns, all rows
  let all = Models.table(Users).selectAll().getAll()

  # specific columns (unselected string fields read as "")
  let some = Models.table(Users)
                .select(["name", "email"])
                .where("name", "John Doe").get()

  # single column
  let names = Models.table(Users)
                .select("name")
                .where("name", "John Doe").get()

  # DISTINCT
  let distinctNames = Models.table(Users).select(["name"]).unique().getAll()

  # by primary key
  let one = Models.table(Users).findById("2").get()

  # first matching row (alias for `.get()`)
  let first = Models.table(Users).selectAll()
                  .where("name", "Bob").first()

  # last inserted row (ORDER BY id DESC LIMIT 1)
  let last = Models.table(Users).selectAll().last()
```

#### Where filters

```nim
withDBPool do:
  # equality / inequality
  discard Models.table(Users).select("name").where("name", "John Doe").get()
  discard Models.table(Users).select("name").whereNot("name", "Ghost").get()
  discard Models.table(Users).select("name")
            .where("name", "Ghost").orWhere("name", "John Doe").get()
  discard Models.table(Users).select("name")
            .whereNot("name", "John Doe").orWhereNot("name", "Ghost").get()

  # comparison
  discard Models.table(Users).select("id").whereGt("id", "2").getAll()
  discard Models.table(Users).select("id").whereGte("id", "3").getAll()
  discard Models.table(Users).select("id").whereLt("id", "3").getAll()
  discard Models.table(Users).select("id").whereLte("id", "2").getAll()
  discard Models.table(Users).select("id")
            .where("id", "1").orWhereGt("id", "10").getAll()

  # NULL
  discard Models.table(Users).select("id").whereNull("updated_at").getAll()
  discard Models.table(Users).select("id").whereNotNull("updated_at").getAll()

  # BETWEEN
  discard Models.table(Users).select("id").whereBetween("id", "2", "3").getAll()
  discard Models.table(Users).select("id").whereNotBetween("id", "2", "3").getAll()
  discard Models.table(Users).select("id").orWhereBetween("id", "2", "3").getAll()

  # IN / NOT IN (also orWhereIn / orWhereNotIn)
  discard Models.table(Users).select("name").whereIn("name", ["Alice", "Bob"]).getAll()
  discard Models.table(Users).select("name").whereNotIn("name", ["Alice"]).getAll()
  discard Models.table(Users).select("id")
            .where("id", "1").orWhereIn("name", ["Bob", "Carol"]).getAll()

  # LIKE: contains / prefix / suffix (+ NOT and OR variants)
  discard Models.table(Users).select("name").whereLike("name", "Jo").get()
  discard Models.table(Users).select("name").whereStartsLike("name", "Jo").get()
  discard Models.table(Users).select("name").whereEndsLike("name", "oe").get()
  discard Models.table(Users).select("name").whereNotLike("name", "Gh").get()
  discard Models.table(Users).select("name").whereNotStartsLike("name", "Gh").get()
  discard Models.table(Users).select("name").whereNotEndsLike("name", "st").get()
  discard Models.table(Users).select("name")
            .where("id", "4").whereStartsLike("username", "da").get()
  discard Models.table(Users).select("name").orWhereLike("name", "Jo").get()
  discard Models.table(Users).select("name")
            .orWhereStartsLike("name", "Jo").orWhereEndsLike("name", "oe").get()
```

#### Ordering, limit, pagination

```nim
withDBPool do:
  # ORDER BY (explicit direction, or Asc/Desc helpers)
  discard Models.table(Users).selectAll().orderAscBy(["username"]).getAll()
  discard Models.table(Users).selectAll().orderDescBy(["username"]).getAll()
  discard Models.table(Users).select("username").orderBy(["username"], Asc).getAll()
  discard Models.table(Users).selectAll().orderBy(["username"], Desc).getAll()

  # LIMIT / OFFSET (static ints, applied before OFFSET)
  discard Models.table(Users).selectAll().limit(1).getAll()
  discard Models.table(Users).selectAll()
            .orderAscBy(["username"]).limit(2).offset(1).getAll()

  # paginate(page, perPage), page is 1-based
  discard Models.table(Users).selectAll()
            .orderAscBy(["username"]).paginate(2, 2).getAll()
```

#### Group by, having, aggregates

```nim
withDBPool do:
  # GROUP BY must precede HAVING
  discard Models.table(Users).select(["name"])
            .groupBy(["name"]).having("name", "Alice").getAll()
  discard Models.table(Users).select(["name"])
            .groupBy(["name"]).havingGte("name", "B").getAll()
  # also: havingGt / havingLt / havingLte

  # aggregates execute immediately (no .get/.getAll)
  let total: int64 = Models.table(Users).selectAll().count()
  let distinctPrices: string = Models.table(Plans).selectAll().countDistinct("price")
  let sum: string = Models.table(Plans).selectAll().sumOf("price")
  let avg: string = Models.table(Plans).selectAll().avgOf("price")
  let lo: string = Models.table(Plans).selectAll().minOf("price")
  let hi: string = Models.table(Plans).selectAll().maxOf("price")

  # aggregates respect filters and bound params
  let filteredSum = Models.table(Plans).selectAll()
                      .whereGte("price", "20").sumOf("price")
```

#### Update and delete

```nim
withDBPool do:
  # UPDATE ... WHERE ...
  Models.table(Users).update({name: "John Doe"}).where("id", "1").exec()

  # atomic counters (chainable with .where().exec())
  Models.table(Metrics).increment("hits", 5).where("id", "1").exec()
  Models.table(Metrics).decrement("hits").where("id", "1").exec() # default n = 1

  # DELETE ... WHERE ...
  Models.table(Users).removeRow().where("id", "1").exec()

  # delete all rows (portable DELETE FROM, no TRUNCATE)
  Models.table(Users).truncate().exec()
```

#### Terminals: how results come back

```nim
withDBPool do:
  # Collection[Model]: .get() = first match, .getAll() = all matches
  let c1 = Models.table(Users).selectAll().where("id", "1").get()
  let c2 = Models.table(Users).selectAll().where("id", "1").getAll()
  check c1.get(0).name == "John Doe"
  for u in c2: echo u.name

  # raw rows, no model mapping
  let raw: seq[seq[string]] = Models.table(Users).selectAll().limit(2).getRaw()

  # single column as seq[string]
  let hits: seq[string] = Models.table(Metrics).selectAll()
                              .orderAscBy(["hits"]).pluck("hits")

  # existence checks (chained or table-level), return bool
  let hasBob: bool = Models.table(Users).selectAll().where("name", "Bob").exists()
  let hasAny: bool = Models.table(Users).exists()

  # row count, returns int64
  let n: int64 = Models.table(Users).selectAll().count()
```

### Querying with raw SQL
When things are getting too complex for the query builder, you can use `rawSQL` to write raw SQL queries while still benefiting from **compile-time validation** & **type safety**. The `rawSQL` macro allows you to write raw SQL queries with **parameter binding** to **prevent SQL injection attacks**.

```nim
Models.rawSQL("SELECT * FROM users WHERE name = ?", "Alice")
      .getWith(Users)
```

### Migrations
Nim is compiled, so migrations ship as pluginkit dynamic libraries built from
your model definitions (full compile-time validation: unknown models/columns
fail the library build). Each migration lives in `migrations/` with a
`<name>_<YYYYMMDDHHMMSS>.nim` filename (timestamp last, since Nim module
names cannot start with a digit):

```nim
# migrations/add_users_20260915120000.nim
import app/models          # the app's models: migrations validate against them
import ozark/migration

newMigration addUsers:
  ## Add users table with unique email   # <- stored as the migration description
  up do:
    Models.table(Users).prepareTable().exec()
  down do:
    Models.table(Users).dropTable().exec()
```

```bash
nim c --app:lib --path:src migrations/add_users_20260915120000.nim
```

```nim
import ozark/driver/sqlite
import ozark/migration

initOzarkDatabase("app.db")
withDBPool do:
  migrate("migrations")    # apply pending in filename-version order
  # rollback("migrations", 1)
  # for s in migrationStatus("migrations"): echo s.info.version, " ", s.applied
```

Applied versions (with description, `compiledAt` build time and `runAt`
apply time) are tracked in the `ozark_migrations` table. Each migration runs
in its own transaction. Migrations support both drivers (`serial`/`integer`
DDL and `?`/`$N` placeholders are generated per driver); hand-written
`rawSQL` text is shared verbatim, so keep placeholder styles portable or
split per-driver migrations.

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/ozark/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/ozark/fork)

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
