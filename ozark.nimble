# Package

version       = "0.1.5"
author        = "George Lemon"
description   = "A magical ORM for Nim"
license       = "MIT"
srcDir        = "src"


# Dependencies

requires "nim >= 2.0.0"
requires "db_connector"

requires "threading"

# temporary until we have a stable release of parsesql
# requires "https://github.com/georgelemon/parsesql#vgelemon"
# requires "parsesql"

requires "openparser"
requires "voodoo#head"

# this should not be a dependency as we use it only in tests
requires "greskewel"
