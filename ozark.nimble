# Package

version       = "0.1.4"
author        = "George Lemon"
description   = "A magical ORM for Nim"
license       = "MIT"
srcDir        = "src"


# Dependencies

requires "nim >= 2.0.0"
requires "db_connector"

requires "threading"
requires "https://github.com/georgelemon/parsesql#vgelemon" # temporary until we have a stable release of parsesql

requires "jsony#head"
requires "voodoo#head"

requires "greskewel"
