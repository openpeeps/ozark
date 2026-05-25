import std/[macros, strutils, macrocache]
import pkg/openparser/sql

const StaticDbTypes = CacheTable"StaticDbTypes"

proc initStaticDriver*(name: static string, driver: static SqlDriver) {.compileTime.} =
  StaticDbTypes[name] = newLit($driver)

proc getSqlDriver*(name: static string = "main"): SqlDriver {.compileTime.} =
  ## Get the SQL driver type specified in static context, if any.
  ## 
  ## This is used by the query macros to determine which SQL syntax to generate based on the
  ## specified driver. If no static driver is specified, it defaults to PostgreSQL.
  if StaticDbTypes.hasKey(name):
    result = parseEnum[SqlDriver]($StaticDbTypes[name][0])
  else:
    result = SqlDriver.psql