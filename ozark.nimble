# Package

version       = "0.1.5"
author        = "George Lemon"
description   = "A magical ORM for Nim"
license       = "MIT"
srcDir        = "src"


# Dependencies

requires "nim >= 2.0.0"
requires "db_connector >= 0.1.0"
requires "threading >= 0.2.1"
requires "openparser >= 0.1.2"
requires "voodoo >= 0.1.9"

# this should not be a dependency as we use it only in tests
# but `nimble test` looks for dependencies in the main package
requires "greskewel"
