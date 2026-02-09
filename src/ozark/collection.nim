# A magical ORM for the Nim language
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/ozark
import ./model

type
  Collection*[T] = object
    ## A simple collection type that can hold
    ## a sequence of items of any type.
    entries*: seq[T]
      # The items in the collection. This can be
      # any type, but in the context of Ozark, it will
      # typically be a sequence of model instances.

proc len*[T](col: Collection[T]): int =
  ## Get the number of items in the collection.
  result = col.entries.len

proc isEmpty*[T](col: Collection[T]): bool =
  ## Check if the collection is empty.
  result = col.entries.len == 0

proc first*[T](col: Collection[T]): T =
  ## Get the first item in the collection.
  result = col.entries[0]

proc contains*[T](col: Collection[T], key, val: string): bool =
  ## Check if the collection contains an
  ## item with the given key and value.
  for entry in col.entries:
    if entry.hasKey(key):
      if entry[key].value == val:
        return true # Found a match, return true
  
iterator items*[T](col: Collection[T]): T =
  ## Iterate over the items in the collection.
  for item in col.entries:
    yield item

iterator mitems*[T](col: Collection[T]): var T =
  ## Iterate over the items in the collection, allowing modification.
  for item in col.entries.mitems:
    yield item
