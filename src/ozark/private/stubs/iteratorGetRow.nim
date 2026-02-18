block:
  let sqlPrepared = prepare(dbcon, "ozark_instant_$7", SqlQuery("$1"), $6)
  var
    row = getRow(dbcon, sqlPrepared$5)
    isEmpty = true
    results: Collection[$2]
  for v in row:
    isEmpty = isEmpty and v.len == 0
  if row.len > 0 and not isEmpty:
    var inst = new($2)
    $3
    results.entries.add(inst)
  results