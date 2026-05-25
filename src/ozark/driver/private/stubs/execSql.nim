block:
  let sqlPrepared = ensurePrepared(dbcon, "ozark_instant_$3", SqlQuery("$1"), $4)
  dbcon.exec(sqlPrepared$2)