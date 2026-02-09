<p align="center">
  <img alt="Ozark ORM" src="https://github.com/openpeeps/ozark/blob/main/.github/ozark-logo-v21.png" width="250px" height="125px"><br>
  A magical ORM for the Nim language 👑
</p>

<p align="center">
  <code>nimble install ozark</code>
</p>

<p align="center">
  <a href="https://github.com/">API reference</a><br>
  <img src="https://github.com/openpeeps/ozark/workflows/test/badge.svg" alt="Github Actions">  <img src="https://github.com/openpeeps/ozark/workflows/docs/badge.svg" alt="Github Actions">
</p>

> [!NOTE]
> Ozark is still in active development. Expect bugs and breaking changes. Contributions are welcome!

## 😍 Key Features
- [x] Macro-based query builder with a fluent API
- [x] Compile-time SQL validation & type safety
- [x] Support for PostgreSQL
- [ ] Async query execution (coming soon)
- [ ] Migration system (coming soon)

## Examples

### Connecting to the database
Initialize the database connection with the given parameters. Once initialized, you can use `withDB` scope to execute queries using the default connection. _todo multiple connections and connection pooling coming soon._
```nim
import ozark/database

initOzarkDatabase("localhost", "mydb", "myuser", "mypassword", 5432.Port)
```

#### Define a model
Define a model by creating a new type that inherits from `Model` and specifying the fields with their types. See Ozark's Types documentation for supported field types and options. 

```nim
import ozark/model

newModel User:
  id: Serial
  name: Varchar(100)
  age: Int
```

### Querying the database
For simple queries, you can use the macro-based query builder. The query builder provides a fluent API for constructing SQL queries in a type-safe way. The generated SQL is validated at compile time to catch errors early.

```nim
import ozark/query

withDB do:
  let id = User.insert({name: "Alice", age: 30}).execGet()
  let results: Collection[User] =
    Models.table("users").select("*")
          .where("id", id)
            # you can also use .where("id", "=", id) for more complex conditions
          .get(User)
            # the get macro will execute the query and return
            # a Collection of User instances.
  
  assert results.len == 1
  
  # getting the first result from the collection
  let user: User = results.first()
  
  # each field of the user instance is also type-safe and
  # can be accessed with the generated getters
  assert user.getId == 1
  assert user.getName == "Alice"
  assert user.getAge == 30
```

### Querying with raw SQL
When things are getting too complex for the query builder, you can use `rawSQL` to write raw SQL queries while still benefiting from **compile-time validation** & **type safety**. The `rawSQL` macro allows you to write raw SQL queries with **parameter binding** to **prevent SQL injection attacks**.

```nim
Models.table("users")
      .rawSQL("SELECT * FROM users WHERE name = ?", "Alice")
      .get(Users)
```

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/ozark/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/ozark/fork)
- 😎 [Get €20 in cloud credits from Hetzner](https://hetzner.cloud/?ref=Hm0mYGM9NxZ4)

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
