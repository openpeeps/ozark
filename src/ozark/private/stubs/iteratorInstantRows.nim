block:
  var
    isEmpty = true
    results: Collection[$2]
    cols: DBColumns = @[]
    colKeys: seq[string]
  let colNames = [$3]
  let sqlPrepared = prepare(dbcon, "ozark_instant_$7", SqlQuery("$1"), $6)
  for row in instantRows(dbcon, cols, sqlPrepared $5):
    isEmpty = isEmpty and row.len == 0
    if isEmpty: continue # skip empty rows
    if colKeys.len == 0:
      colKeys = cols.mapIt(it.name) # extract column names from DBColumns
    var inst = new($2)
    for fName, fValue in inst[].fieldPairs():
      if colNames[0] != "*" and fName in colNames:
        # first check if the column name is in the list of
        # selected columns, then find its index and assign the value from the row
        if fName in colKeys:
          fValue = row[colKeys.find(fName)]
        else:
          raise newException(OzarkModelDefect,
            "Model field `$2." & fName & "` does not have a corresponding column in the SQL result")
      elif colNames[0] == "*":
        if fName in colKeys:
          fValue = row[colKeys.find(fName)]
    results.entries.add(inst)
  results