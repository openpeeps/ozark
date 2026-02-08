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

## 😍 Key Features
- [x] Macro-based query builder with a fluent API
- [x] Compile-time SQL validation
- [x] Runtime query builder for dynamic query construction
- [x] Support for PostgreSQL
- [ ] Async query execution (coming soon)
- [ ] Migration system (coming soon)

## Examples
A super simple example using Ozark to define a model and execute some queries:

```nim
import ozark

# define a model
newModel User:
  id: Serial
  name: Varchar(100)
  age: Int

# initialize the database connection
initOzarkDatbase("localhost", "mydb", "myuser", "mypassword", 5432.Port)

# use `withDB` scope to execute queries using
# the default database connection
withDB do:
  let id = User.insert({name: "Alice", age: 30}).execGet()
  assert id == "1"

  let results: Collection[User] = User.select("*").where("id", id).get()
  assert results.len == 1
  let user: User = results.first()
  assert user.id == 1
  assert user.name == "Alice"
  assert user.age == 30
```

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/ozark/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/ozark/fork)
- 😎 [Get €20 in cloud credits from Hetzner](https://hetzner.cloud/?ref=Hm0mYGM9NxZ4)

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
