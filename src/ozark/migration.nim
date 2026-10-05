# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark

## Pluginkit-backed migration support.
##
## Each migration is a Nim source file compiled as a dynamic library
## (`nim c --app:lib migrations/add_users_20260915120000.nim`) and loaded
## at runtime from a folder. Migration bodies reuse the normal Ozark DSL
## (`Models.table(..).prepareTable().exec()`, ...) with full compile-time
## validation: the migration must `import` the application's model modules,
## so unknown models/columns fail the library build.
##
## Migrations never touch a database connection directly. The `up`/`down`
## bodies are expanded twice (once per driver) and captured as SQL + params;
## the host executes them inside its own pooled connection/transaction and
## records `compiledAt`/`runAt` in the `ozark_migrations` tracking table.
##
## DSL:
##
## .. code-block:: nim
##   import app/models
##   import ozark/migration
##
##   newMigration addUsers:
##     ## Add users table with unique email
##     up do:
##       Models.table(Users).prepareTable().exec()
##     down do:
##       Models.table(Users).dropTable().exec()
##
## Host:
##
## .. code-block:: nim
##   import ozark/driver/sqlite
##   import ozark/migration
##
##   initOzarkDatabase("app.db")
##   withDBPool do:
##     migrate("migrations")        # apply pending, ordered by filename version
##     # rollback("migrations", 1)  # revert last applied
##     # for s in migrationStatus("migrations"): echo s.info.version, " ", s.applied

import std/[macros, macrocache, os, strutils, tables, times, dynlib, algorithm, sequtils]
import pkg/openparser/[sql, json]
import pkg/db_connector/db_common

import ./model, ./query
export model, query

const
  OzarkMigrationDriverPgsql* = 1
    ## Driver id for PostgreSQL (matches the `sqlDriver` const injected by the pg driver).
  OzarkMigrationDriverSqlite* = 3
    ## Driver id for SQLite (matches the `sqlDriver` const injected by the sqlite driver).
  ozarkMigrationExecStages = [
    "ozarkWhereResult", "ozarkRawSQLResult", "ozarkInsertResult",
    "ozarkCreateTableResult", "ozarkRemoveResult", "ozarkUpsertResult"
  ]

type
  OzarkMigrationError* = object of CatchableError
    ## Raised for migration definition, loading, or apply failures.

  OzarkMigrationStmt* = tuple[sql: string, params: seq[string]]
    ## One captured statement: validated SQL text plus bound values.

  OzarkMigrationInfo* = object
    ## Identity + audit metadata for a single migration library.
    name*, version*, description*, compiledAt*, filepath*: string

  OzarkMigrationStatus* = object
    ## `migrationStatus` entry: the migration plus whether it was applied.
    info*: OzarkMigrationInfo
    applied*: bool
    runAt*: string

  migration_info_fn* = proc(): cstring {.cdecl.}
    ## ABI of the exported `migration_get_info` symbol.
  migration_run_fn* = proc(driver: cint): cstring {.cdecl.}
    ## ABI of the exported `migration_up` / `migration_down` symbols.
    ## `driver` is 1 (pgsql) or 3 (sqlite); returns a JSON array of
    ## `{sql, params}` for that driver.

proc ozarkMigStmtsToJson*(stmts: seq[OzarkMigrationStmt]): JsonNode =
  ## Serializes captured statements for transfer across the library boundary.
  result = newJArray()
  for s in stmts:
    var params = newJArray()
    for p in s.params:
      params.add(%p)
    result.add(%*{"sql": s.sql, "params": params})

proc ozarkMigStmtsFromJson*(node: JsonNode): seq[OzarkMigrationStmt] =
  ## Parses the JSON produced by `migration_up` / `migration_down`.
  if node.kind != JArray:
    raise newException(OzarkMigrationError, "Invalid migration payload: expected array")
  for item in node:
    var params: seq[string] = @[]
    for p in item["params"]:
      params.add(p.getStr)
    result.add((sql: item["sql"].getStr, params: params))

proc ozarkMigrationTimestamp*(filename: string): string =
  ## Extracts the `YYYYMMDDHHMMSS` version suffix from a migration filename
  ## such as `add_users_20260915120000.nim`. The timestamp goes last because
  ## Nim module names cannot start with a digit, so timestamp-prefixed files
  ## would not compile as dynamic libraries.
  let base = splitFile(extractFilename(filename)).name
  let idx = base.rfind('_')
  let stamp = if idx > 0: base[idx + 1 .. ^1] else: ""
  if stamp.len != 14 or not stamp.allIt(it.isDigit):
    raise newException(OzarkMigrationError,
      "Migration filename `" & base & "` must end with a 14-digit timestamp " &
      "(e.g. `add_users_20260915120000.nim`)")
  stamp

proc ozarkMigrationSemver*(timestamp: string): string =
  ## Derives a semver-compatible version for the pluginkit manifest from a
  ## migration timestamp (`20260915120000` -> `2026.9.15`). Ordering always
  ## uses the full timestamp, never this value.
  let year = timestamp[0 .. 3]
  let month = $parseInt(timestamp[4 .. 5])
  let day = $parseInt(timestamp[6 .. 7])
  year & "." & month & "." & day

macro newMigration*(name: untyped, body: untyped): untyped =
  ## Defines a pluggable migration (see module docs). Only meaningful when
  ## compiled as a dynamic library (`nim c --app:lib`); using it in a normal
  ## application module is a compile-time error.
  if not compileOption("app", "lib"):
    error("newMigration `" & $name & "` must be compiled as a dynamic library " &
      "(`nim c --app:lib " & instantiationInfo(fullPaths = true).filename & "`)", name)
    return newStmtList()
  name.expectKind(nnkIdent)
  body.expectKind(nnkStmtList)

  # Split leading `##` doc comments (description) from the up/down blocks.
  var descrParts: seq[string] = @[]
  var rest = newStmtList()
  var seenCode = false
  for stmt in body:
    if not seenCode and stmt.kind == nnkCommentStmt:
      descrParts.add(stmt.strVal.strip())
    else:
      seenCode = true
      rest.add(stmt)
  let description = descrParts.join("\n").strip()

  var upBlock, downBlock: NimNode
  for stmt in rest:
    if stmt.kind == nnkCall and stmt.len == 2 and stmt[0].kind == nnkIdent and
        stmt[1].kind == nnkStmtList:
      let section = stmt[0].strVal.toLowerAscii
      if section == "up":
        if not upBlock.isNil:
          error("Duplicate `up` block in migration `" & $name & "`", stmt)
        upBlock = stmt[1]
      elif section == "down":
        if not downBlock.isNil:
          error("Duplicate `down` block in migration `" & $name & "`", stmt)
        downBlock = stmt[1]
      else:
        error("Expected `up do:` or `down do:` in migration `" & $name &
          "`, got `" & stmt[0].strVal & "`", stmt)
    else:
      error("Expected `up do:` / `down do:` blocks in migration `" & $name & "`", stmt)
  if upBlock.isNil:
    error("Migration `" & $name & "` is missing its `up do:` block", name)
  if downBlock.isNil:
    error("Migration `" & $name & "` is missing its `down do:` block", name)

  let srcFile = lineInfoObj(name).filename
  let timestamp =
    try: ozarkMigrationTimestamp(srcFile)
    except OzarkMigrationError as e: error(e.msg, name); ""
  let semver = ozarkMigrationSemver(timestamp)
  let migName = $name

  # Duplicate bodies so each driver flavor is validated + captured separately.
  # NOTE: the per-flavor `const sqlDriver` below is built with manual AST
  # (plain ident, no {.inject.}). Writing it literally inside `quote` would
  # inject it into the macro call-site scope (module top-level), colliding
  # across flavors at C level. Manual nodes keep it proc-local, where the
  # `table` template still resolves it during instantiation.
  proc mkFlavorProc(procName: string, driverVal: int,
                    flavorBody: NimNode): NimNode =
    let constSec = nnkConstSection.newTree(
      nnkConstDef.newTree(ident"sqlDriver", newEmptyNode(), newLit(driverVal)))
    let procBody = newStmtList(constSec)
    for s in flavorBody:
      procBody.add(copyNimTree(s))
    newProc(ident(procName), body = procBody)

  let upPgsqlImpl = mkFlavorProc("ozarkMigUpPgsql", 1, upBlock)
  let upSqliteImpl = mkFlavorProc("ozarkMigUpSqlite", 3, upBlock)
  let downPgsqlImpl = mkFlavorProc("ozarkMigDownPgsql", 1, downBlock)
  let downSqliteImpl = mkFlavorProc("ozarkMigDownSqlite", 3, downBlock)

  result = quote do:
    when not compileOption("app", "lib"):
      {.error: "newMigration bodies only compile as a dynamic library".}
    import pkg/pluginkit

    const ozarkMigName {.inject.} = `migName`
    const ozarkMigVersion {.inject.} = `timestamp`
    const ozarkMigDescription {.inject.} = `description`
    const ozarkMigCompiledAt {.inject.} = CompileDate & "T" & CompileTime & "Z"

    var ozarkMigManifest {.inject, global.} = PluginManifest(
      abiVersion: PluginAbiVersion,
      name: cstring(ozarkMigName),
      author: cstring(""),
      description: cstring(ozarkMigDescription),
      license: cstring(""),
      url: cstring(""),
      version: cstring(`semver`),
      permissions: permDBFullAccess,
      nimVersion: NimVersion
    )

    proc NimMain {.cdecl, importc.}
    {.push exportc, cdecl, dynlib.}
    proc plugin_get_manifest*(outManifest {.inject.}: ptr PluginManifest): cint =
      if outManifest.isNil: return 1
      outManifest[] = ozarkMigManifest
      return 0

    proc plugin_init*(): cint =
      NimMain()
      return 0

    proc plugin_deinit*() =
      GC_FullCollect()

    proc migration_get_info*(): cstring =
      ozarkMigJsonCache = $(%*{
        "name": ozarkMigName,
        "version": ozarkMigVersion,
        "description": ozarkMigDescription,
        "compiledAt": ozarkMigCompiledAt
      })
      ozarkMigJsonCache.cstring

    proc migration_up*(driver: cint): cstring =
      ozarkMigIsPgsql = driver == cint(OzarkMigrationDriverPgsql)
      var stmts: seq[OzarkMigrationStmt] = @[]
      ozarkMigCollector = addr stmts
      if driver == cint(OzarkMigrationDriverPgsql):
        ozarkMigUpPgsql()
      else:
        ozarkMigUpSqlite()
      ozarkMigJsonCache = $ozarkMigStmtsToJson(stmts)
      ozarkMigJsonCache.cstring

    proc migration_down*(driver: cint): cstring =
      ozarkMigIsPgsql = driver == cint(OzarkMigrationDriverPgsql)
      var stmts: seq[OzarkMigrationStmt] = @[]
      ozarkMigCollector = addr stmts
      if driver == cint(OzarkMigrationDriverPgsql):
        ozarkMigDownPgsql()
      else:
        ozarkMigDownSqlite()
      ozarkMigJsonCache = $ozarkMigStmtsToJson(stmts)
      ozarkMigJsonCache.cstring
    {.pop.}

  let headed = newStmtList(upPgsqlImpl, upSqliteImpl, downPgsqlImpl,
                             downSqliteImpl)
  for s in result:
    headed.add(copyNimTree(s))
  result = headed

  when defined(ozarkMigDebug):
    echo result.repr

when compileOption("app", "lib"):
  import pkg/pluginkit

  var ozarkMigCollector* {.global.}: ptr seq[OzarkMigrationStmt]
    ## Target statement list for the capturing `exec` below. Set by the
    ## generated `migration_up` / `migration_down` before running each body.
  var ozarkMigIsPgsql* {.global.}: bool
    ## True while capturing the PostgreSQL flavor (drives value mapping).
  var ozarkMigJsonCache* {.global.}: string
    ## Backing store for returned `cstring`s; keeps them alive across the ABI.

  proc migVal*(v: string): string = v
    ## Maps a bound value to its driver-specific database representation.
  proc migVal*(v: bool): string =
    if ozarkMigIsPgsql:
      (if v: "t" else: "f")
    else:
      (if v: "1" else: "0")
  proc migVal*(v: DateTime): string =
    if ozarkMigIsPgsql: v.format("yyyy-MM-dd HH:mm:sszz")
    else: v.format("yyyy-MM-dd'T'HH:mm:sszzz")
  proc migVal*[T](v: T): string = $v

  proc ozarkMigEmit*(sql: string, params: varargs[string]) =
    ## Appends one captured statement to the active collector.
    if ozarkMigCollector.isNil:
      raise newException(OzarkMigrationError,
        "Migration `exec()` called outside `up`/`down` capture")
    var ps: seq[string] = @[]
    for p in params:
      ps.add(p)
    ozarkMigCollector[].add((sql: sql, params: ps))

  macro exec*(sql: untyped): untyped =
    ## Capturing replacement for the driver `exec` terminals. Validates the
    ## generated SQL at compile time (like the drivers do) but collects
    ## `(sql, params)` instead of executing, so migration libraries stay
    ## connection-free.
    var trailing = sql
    case sql.kind
    of nnkBlockStmt, nnkBlockExpr:
      trailing = trailingCallOf(sql)
    of nnkCall:
      trailing = sql
    else:
      error("Migration `exec()` expects a query chain " &
        "(e.g. `Models.table(Users).prepareTable().exec()`)", sql)
    if trailing.kind != nnkCall or trailing.len < 2 or
        trailing[0].kind notin {nnkIdent, nnkSym}:
      error("Migration `exec()` expects a query chain " &
        "(e.g. `Models.table(Users).prepareTable().exec()`)", sql)
    let stage = resultKindOf(trailing)
    if stage notin ozarkMigrationExecStages:
      error("Migration `exec()` cannot finalize `" & stage &
        "`. Supported: prepareTable/dropTable/truncate/insert/update/removeRow/upsert/rawSQL " &
        "(each optionally followed by `where`), all terminated by `.exec()`. " &
        "Scalar terminals (`get`, `getAll`, `execGet`, aggregates) are not supported in v1.", sql)
    if trailing[1].kind != nnkStrLit:
      error("Migration `exec()` could not read the generated SQL literal", sql)
    let sqlText = trailing[1].strVal
    if stage != "ozarkUpsertResult":
      try:
        discard parseSQL(normalizeInLists(sqlText))
      except SqlParseError as e:
        error("Migration SQL validation failed: " & e.msg, sql)
    if trailing.len == 2:
      trailing.add(nnkPrefix.newTree(ident"@", nnkBracket.newTree()))
    let bracket = paramsBracketOf(trailing)
    var call = newCall(bindSym"ozarkMigEmit", newLit(sqlText))
    for v in bracket:
      call.add(newCall(bindSym"migVal", v))
    result = call

  macro rawSQL*(models: ptr ModelsTable, sql: static string,
                values: varargs[untyped]): untyped =
    ## Capturing replacement for the driver `rawSQL`: validates static SQL at
    ## compile time (including model checks for SELECTs) and returns an
    ## intermediate chainable node for `.exec()`.
    try:
      let sqlNode = parseSQL(sql)
      case sqlNode.sons[0].kind
      of nkSelect:
        let fromNode = sqlNode.sons[0].sons[1]
        if fromNode.kind == nkFrom:
          for table in fromNode.sons:
            if table.len > 0 and table[0].kind == nkIdent:
              if not StaticSchemas.hasKey(getTableName(table[0].strVal)):
                raise newException(OzarkModelDefect,
                  "Unknown model `" & table[0].strVal & "`")
      else: discard
    except SqlParseError as e:
      raise newException(OzarkModelDefect, "SQL Parsing Error: " & e.msg)
    let blockIdent = genSym(nskLabel, "ozarkMigRawSQL")
    # NB: the drivers pass `nil` here, which no longer type-checks as a
    # method-call base (`var x: typeof(nil)`). A dummy int works because the
    # whole block is consumed by `.exec()` before codegen; only the trailing
    # `ozarkRawSQLResult` node is read.
    # NB2: `values` arrives as nnkArgList, so rewrap it into a real bracket
    # (the drivers splice it raw, which breaks param extraction downstream).
    var valsBracket = nnkBracket.newTree()
    for v in values:
      valsBracket.add(v)
    result = nnkBlockStmt.newTree(
      blockIdent,
      newStmtList(
        newCall(bindSym"ozarkHoldModel", newLit(0)),
        newCall(
          bindSym"ozarkRawSQLResult",
          newLit(sql),
          nnkPrefix.newTree(ident"@", valsBracket)
        )
      )
    )

else:
  import pkg/pluginkit

  const ozarkMigrationsTableDDL* =
    "CREATE TABLE IF NOT EXISTS ozark_migrations (" &
    "version TEXT PRIMARY KEY, name TEXT, description TEXT, " &
    "compiled_at TEXT, run_at TEXT)"
    ## DDL for the migration tracking table (portable across drivers).

  proc parseOzarkMigrationInfo*(payload, filepath: string): OzarkMigrationInfo =
    ## Parses a `migration_get_info` JSON payload.
    let node =
      try: parseJson(payload)
      except CatchableError as e:
        raise newException(OzarkMigrationError,
          "Invalid migration info from `" & filepath & "`: " & e.msg)
    OzarkMigrationInfo(
      name: node["name"].getStr,
      version: node["version"].getStr,
      description: node.getOrDefault("description").getStr,
      compiledAt: node.getOrDefault("compiledAt").getStr,
      filepath: filepath
    )

  proc ozarkMigPlaceholder*(driver, idx: int): string =
    ## Bound-parameter placeholder for hand-written host SQL.
    if driver == OzarkMigrationDriverPgsql: "$" & $idx else: "?"

  proc ozarkRunAt*(): string =
    ## Current UTC timestamp for the `run_at` tracking column.
    now().utc.format("yyyy-MM-dd'T'HH:mm:ss'Z'")

  type
    OzarkLoadedMigration* = object
      ## A discovered migration library with resolved symbols.
      info*: OzarkMigrationInfo
      pluginId*: string
      runUp*, runDown*: migration_run_fn

  proc loadOzarkMigrations*(manager: PluginManager,
                            dir: string): seq[OzarkLoadedMigration] =
    ## Discovers (`*.so`/`*.dylib`/`*.dll`), loads, and activates every
    ## migration library in `dir`, ordered by filename version. Raises
    ## `OzarkMigrationError` on duplicates or non-migration plugins.
    if not dirExists(dir):
      raise newException(OzarkMigrationError,
        "Migrations directory not found: " & dir)
    var seen = initTable[string, string]()
    for kind, path in walkDir(dir):
      if kind != pcFile: continue
      if splitFile(path).ext.toLowerAscii notin [".so", ".dylib", ".dll"]:
        continue
      let id = manager.load(path)
      let plugin = manager.getPlugin(id)
      let handle = plugin.getHandle()
      let infoFn = cast[migration_info_fn](handle.symAddr("migration_get_info"))
      let runUp = cast[migration_run_fn](handle.symAddr("migration_up"))
      let runDown = cast[migration_run_fn](handle.symAddr("migration_down"))
      if infoFn.isNil or runUp.isNil or runDown.isNil:
        manager.unload(id)
        raise newException(OzarkMigrationError,
          "Not a migration plugin (missing migration_* symbols): " & path)
      manager.activate(id)
      let info = parseOzarkMigrationInfo($infoFn(), path)
      if seen.hasKey(info.version):
        raise newException(OzarkMigrationError,
          "Duplicate migration version `" & info.version & "` in `" &
          seen[info.version] & "` and `" & path & "`")
      seen[info.version] = path
      result.add(OzarkLoadedMigration(
        info: info, pluginId: id, runUp: runUp, runDown: runDown))
    result.sort(proc(a, b: OzarkLoadedMigration): int =
      cmp(a.info.version, b.info.version))

  template withOzarkMigrationLibs*(dir: string, loaded, body) =
    ## Loads every migration in `dir`, exposes them as `loaded`, unloads all
    ## afterwards. Must be used where `dbcon`/`sqlDriver` are NOT required
    ## (loading only); running statements needs the templates below.
    block:
      var ozarkMigManager {.inject.} = PluginManager(
        callbacks: PluginManagerCallbacks())
      var loaded {.inject.} = loadOzarkMigrations(ozarkMigManager, dir)
      try:
        body
      finally:
        for ozarkEntry in loaded:
          try: ozarkMigManager.unload(ozarkEntry.pluginId)
          except CatchableError: discard

  template migrate*(dir: string) =
    ## Applies all pending migrations from `dir`, in filename-version order.
    ## Call inside `withDB`/`withDBPool` so `dbcon` (and the driver-injected
    ## `sqlDriver` const) are in scope. Each migration runs in its own
    ## transaction; `run_at` is recorded only on commit.
    block:
      let ozarkMigDriver {.inject.} = int(sqlDriver)
      withOzarkMigrationLibs(dir, ozarkLoadedMigrations):
        dbcon.exec(SqlQuery(ozarkMigrationsTableDDL))
        var ozarkAppliedVersions: seq[string] = @[]
        for ozarkRow in dbcon.getAllRows(
            SqlQuery("SELECT version FROM ozark_migrations")):
          if ozarkRow.len > 0:
            ozarkAppliedVersions.add(ozarkRow[0])
        for ozarkEntry in ozarkLoadedMigrations:
          if ozarkEntry.info.version in ozarkAppliedVersions: continue
          let ozarkPayload =
            try: $ozarkEntry.runUp(cint(ozarkMigDriver))
            except CatchableError as ozarkBuildErr:
              raise newException(OzarkMigrationError,
                "Migration `" & ozarkEntry.info.version & "` (" &
                ozarkEntry.info.name & ") failed to render: " &
                ozarkBuildErr.msg)
          let ozarkStmts =
            try: ozarkMigStmtsFromJson(parseJson(ozarkPayload))
            except CatchableError as ozarkParseErr:
              raise newException(OzarkMigrationError,
                "Migration `" & ozarkEntry.info.version & "` (" &
                ozarkEntry.info.name & ") returned invalid payload: " &
                ozarkParseErr.msg)
          dbcon.exec(SqlQuery("BEGIN"))
          var ozarkCommitted = false
          try:
            for ozarkStmt in ozarkStmts:
              try:
                dbcon.exec(SqlQuery(ozarkStmt.sql), ozarkStmt.params)
              except DbError as ozarkExecErr:
                raise newException(OzarkMigrationError,
                  "Migration `" & ozarkEntry.info.version & "` (" &
                  ozarkEntry.info.name & ") failed on `" & ozarkStmt.sql &
                  "`: " & ozarkExecErr.msg)
            let ozarkPh = [
              ozarkMigPlaceholder(ozarkMigDriver, 1),
              ozarkMigPlaceholder(ozarkMigDriver, 2),
              ozarkMigPlaceholder(ozarkMigDriver, 3),
              ozarkMigPlaceholder(ozarkMigDriver, 4),
              ozarkMigPlaceholder(ozarkMigDriver, 5)
            ]
            dbcon.exec(SqlQuery(
              "INSERT INTO ozark_migrations (version, name, description, " &
              "compiled_at, run_at) VALUES (" & ozarkPh.join(", ") & ")"),
              @[ozarkEntry.info.version, ozarkEntry.info.name,
                ozarkEntry.info.description, ozarkEntry.info.compiledAt,
                ozarkRunAt()])
            dbcon.exec(SqlQuery("COMMIT"))
            ozarkCommitted = true
          finally:
            if not ozarkCommitted:
              try: dbcon.exec(SqlQuery("ROLLBACK"))
              except DbError: discard
              raise newException(OzarkMigrationError,
                "Migration `" & ozarkEntry.info.version & "` (" &
                ozarkEntry.info.name & ") rolled back")

  template rollback*(dir: string, steps: int = 1) =
    ## Reverts the last `steps` applied migrations (runs their `down`
    ## bodies, newest first). Call inside `withDB`/`withDBPool`.
    block:
      let ozarkMigDriver {.inject.} = int(sqlDriver)
      withOzarkMigrationLibs(dir, ozarkLoadedMigrations):
        dbcon.exec(SqlQuery(ozarkMigrationsTableDDL))
        var ozarkAppliedRows = dbcon.getAllRows(SqlQuery(
          "SELECT version FROM ozark_migrations ORDER BY version DESC"))
        var ozarkToRevert: seq[string] = @[]
        for ozarkRow in ozarkAppliedRows:
          if ozarkRow.len > 0 and ozarkToRevert.len < steps:
            ozarkToRevert.add(ozarkRow[0])
        var ozarkByVersion = initTable[string, OzarkLoadedMigration]()
        for ozarkEntry in ozarkLoadedMigrations:
          ozarkByVersion[ozarkEntry.info.version] = ozarkEntry
        for ozarkVersion in ozarkToRevert:
          if not ozarkByVersion.hasKey(ozarkVersion):
            raise newException(OzarkMigrationError,
              "Cannot roll back `" & ozarkVersion &
              "`: migration library missing from `" & dir & "`")
          let ozarkEntry = ozarkByVersion[ozarkVersion]
          let ozarkPayload = $ozarkEntry.runDown(cint(ozarkMigDriver))
          let ozarkStmts = ozarkMigStmtsFromJson(parseJson(ozarkPayload))
          dbcon.exec(SqlQuery("BEGIN"))
          var ozarkCommitted = false
          try:
            for ozarkStmt in ozarkStmts:
              try:
                dbcon.exec(SqlQuery(ozarkStmt.sql), ozarkStmt.params)
              except DbError as ozarkExecErr:
                raise newException(OzarkMigrationError,
                  "Rollback `" & ozarkVersion & "` failed on `" &
                  ozarkStmt.sql & "`: " & ozarkExecErr.msg)
            dbcon.exec(SqlQuery(
              "DELETE FROM ozark_migrations WHERE version = " &
              ozarkMigPlaceholder(ozarkMigDriver, 1)), @[ozarkVersion])
            dbcon.exec(SqlQuery("COMMIT"))
            ozarkCommitted = true
          finally:
            if not ozarkCommitted:
              try: dbcon.exec(SqlQuery("ROLLBACK"))
              except DbError: discard
              raise newException(OzarkMigrationError,
                "Rollback `" & ozarkVersion & "` rolled back")

  template migrationStatus*(dir: string): seq[OzarkMigrationStatus] =
    ## Lists every discovered migration with its applied state. Call inside
    ## `withDB`/`withDBPool`.
    block:
      var ozarkStatuses: seq[OzarkMigrationStatus] = @[]
      withOzarkMigrationLibs(dir, ozarkLoadedMigrations):
        dbcon.exec(SqlQuery(ozarkMigrationsTableDDL))
        var ozarkApplied = initTable[string, string]()
        for ozarkRow in dbcon.getAllRows(SqlQuery(
            "SELECT version, run_at FROM ozark_migrations")):
          if ozarkRow.len > 1:
            ozarkApplied[ozarkRow[0]] = ozarkRow[1]
        for ozarkEntry in ozarkLoadedMigrations:
          ozarkStatuses.add(OzarkMigrationStatus(
            info: ozarkEntry.info,
            applied: ozarkApplied.hasKey(ozarkEntry.info.version),
            runAt: ozarkApplied.getOrDefault(ozarkEntry.info.version)))
      ozarkStatuses
