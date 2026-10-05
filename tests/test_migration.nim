import std/[os, osproc, unittest, strutils, times]
import ../src/ozark/driver/sqlite
import ../src/ozark/migration

import ../src/ozark/model

newModel Users:
  id: Serial
  username: Varchar(50)
  email: Varchar(100)

const
  testsDir = currentSourcePath().parentDir()
  ozarkSrc = testsDir / "../src"

var workDir = getTempDir() / "ozark_migration_test"
var migDir = workDir / "migrations"

proc nimExe(): string =
  result = findExe("nim")
  assert result.len > 0, "nim not found in PATH"

proc buildMigration(srcName, outName: string): tuple[output: string, exitCode: int] =
  ## Compiles one migration fixture as a dynamic library.
  ## NB: `system.gorgeEx` is compile-time only; `execCmdEx` runs at test time.
  osproc.execCmdEx(nimExe() & " c --hints:off --app:lib" &
    " --path:" & quoteShell(ozarkSrc) &
    " --path:" & quoteShell(migDir) &
    " --out:" & quoteShell(migDir / outName) &
    " " & quoteShell(migDir / srcName))

proc setupFixtures() =
  removeDir(workDir)
  createDir(migDir)
  writeFile(migDir / "appmodels.nim", """
import ozark/model

newModel Users:
  id: Serial
  username: Varchar(50)
  email: Varchar(100)
""")
  writeFile(migDir / "add_users_20260915120000.nim", """
import appmodels
import ozark/migration

newMigration addUsers:
  ## Add users table with unique email
  ## Safe to re-run.
  up do:
    Models.table(Users).prepareTable().exec()
  down do:
    Models.table(Users).dropTable().exec()
""")
  writeFile(migDir / "seed_users_20260915150000.nim", """
import appmodels
import ozark/migration

newMigration seedUsers:
  ## Seed an admin user
  up do:
    Models.table(Users).insert({username: "admin", email: "admin@example.com"}).exec()
  down do:
    Models.rawSQL("DELETE FROM users WHERE username = 'admin'").exec()
""")
  for pair in [
    ("add_users_20260915120000.nim", "add_users_20260915120000.so"),
    ("seed_users_20260915150000.nim", "seed_users_20260915150000.so")
  ]:
    let (output, exitCode) = buildMigration(pair[0], pair[1])
    assert exitCode == 0, "Failed to build " & pair[0] & ":\n" & output

suite "pluginkit-backed migrations (sqlite)":
  test "migrate applies DDL+DML in version order with tracking":
    setupFixtures()
    let dbPath = workDir / "migtest.db"
    removeFile(dbPath)
    initOzarkDatabase(dbPath)
    withDBPool do:
      migrate(migDir)

      let statuses = migrationStatus(migDir)
      check statuses.len == 2
      check statuses[0].info.version == "20260915120000"
      check statuses[0].info.name == "addUsers"
      check statuses[0].applied
      check statuses[0].info.description ==
        "Add users table with unique email\nSafe to re-run."
      check statuses[0].info.compiledAt.len > 0
      check statuses[0].runAt.len > 0
      check statuses[1].info.version == "20260915150000"
      check statuses[1].applied

      let rows = Models.table(Users).selectAll().getRaw()
      check rows.len == 1

      # second run is a no-op (idempotent)
      migrate(migDir)
      check Models.table(Users).selectAll().getRaw().len == 1

  test "rollback reverts newest first and clears tracking":
    let dbPath = workDir / "migtest.db"
    initOzarkDatabase(dbPath)
    withDBPool do:
      rollback(migDir, 1)
      var statuses = migrationStatus(migDir)
      check statuses[0].applied
      check not statuses[1].applied
      check Models.table(Users).selectAll().getRaw().len == 0

      rollback(migDir, 1)
      statuses = migrationStatus(migDir)
      check not statuses[0].applied
      check not statuses[1].applied

      # re-migrate after full rollback
      migrate(migDir)
      check Models.table(Users).selectAll().getRaw().len == 1

  test "unknown column fails the migration library build":
    writeFile(migDir / "bad_col_20260915160000.nim", """
import appmodels
import ozark/migration

newMigration badCol:
  up do:
    Models.table(Users).insert({nope: "x"}).exec()
  down do:
    Models.table(Users).dropTable().exec()
""")
    let (output, exitCode) = buildMigration(
      "bad_col_20260915160000.nim", "bad_col_20260915160000.so")
    check exitCode != 0
    check "nope" in output
    removeFile(migDir / "bad_col_20260915160000.nim")

  test "duplicate versions are rejected at load time":
    writeFile(migDir / "dup_20260915150000.nim", """
import appmodels
import ozark/migration

newMigration dupMig:
  up do:
    Models.table(Users).prepareTable().exec()
  down do:
    Models.table(Users).dropTable().exec()
""")
    let (output, exitCode) = buildMigration(
      "dup_20260915150000.nim", "dup_20260915150000.so")
    if exitCode != 0: echo output
    check exitCode == 0
    let dbPath = workDir / "migtest.db"
    initOzarkDatabase(dbPath)
    withDBPool do:
      var raised = false
      try:
        migrate(migDir)
      except OzarkMigrationError as e:
        raised = true
        check "Duplicate migration version" in e.msg
      check raised
    removeFile(migDir / "dup_20260915150000.nim")
    removeFile(migDir / "dup_20260915150000.so")
