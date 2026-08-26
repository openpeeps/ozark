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
- [ ] Migration system (coming soon)

> [!NOTE]
> Ozark is still in active development. Expect bugs and breaking changes. Contributions are welcome!

## Examples

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
For simple queries, you can use the macro-based query builder. The query builder provides a fluent API for constructing SQL queries in a type-safe way. The generated SQL is validated at compile time to catch errors early.

#### Insert data
```nim
import ozark/query

withDBPool do:
  let id = Models.table(Users).insert({
    name: "John Doe",
    username: "johndoe",
    email: "johndoe@example.com",
  }).execGet() # returns the id of the inserted row
```

#### Select Query
```nim
withDBPool do:
  let res = Models.table(Users)
                  .select(["name", "email"])
                  .where("name", "John")
                  .get()
```

### Querying with raw SQL
When things are getting too complex for the query builder, you can use `rawSQL` to write raw SQL queries while still benefiting from **compile-time validation** & **type safety**. The `rawSQL` macro allows you to write raw SQL queries with **parameter binding** to **prevent SQL injection attacks**.

```nim
Models.table(Users)
      .rawSQL("SELECT * FROM users WHERE name = ?", "Alice")
      .get(Users)
```

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/ozark/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/ozark/fork)

|  |  |
|---|---|
| <a href="https://opencode.ai/go?ref=BHMEEK48QX"><img src="https://github.com/openpeeps/pistachio/blob/main/.github/opencode.png" alt="OpenCode"></a> | Switch to **Open-Source LLMs** via OpenCode GO, choosing from a variety of powerful models such as DeepSeek, Qwen, Kimi, GLM-5, MiniMax, MiMo. 🍕 [Use our referral link to get started!](https://opencode.ai/go?ref=BHMEEK48QX)|

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
