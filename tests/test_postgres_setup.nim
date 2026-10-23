import unittest, os
import pkg/greskewel

import ../src/ozark

var greskew: EmbeddedPostgres
test "init embedded postgres":
  greskew = initEmbeddedPostgres(
    PostgresConfig(
      basePath: getCurrentDir() / "tests" / "greskewelbox",
    )
  )
  greskew.downloadBinaries()
