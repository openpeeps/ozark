block:
  let sqlPrepared =
    prepare(dbcon, "ozark_insert_$3",
      SqlQuery("$1 RETURNING id"), $4)
  tryInsertID(dbcon, sqlPrepared, $2)