# Doc: store nested documents in Tarantool

Using this module you can store and retrieve dictionaries with very
little overhead. It figures out document schema on the fly, and
progressively updates it when new fields are received.

The schema is stored and updated transparently.

## Getting started

```lua
doc = require 'doc'

box.cfg{}
doc.init()

box.schema.create_space("test", {if_not_exists = true})
doc.create_index(box.space.test, "primary",
                 {parts={'id', 'unsigned'}, if_not_exists=true})


local v = {id=1, foo=2, bar={a=3, b=4}, c="quux"}


box.space.test:put(doc.flatten(box.space.test, v))

v = doc.unflatten(box.space.test, box.space.test:get(1))
```

## Interface

### `doc.init()`
    Initializes internal structures. Call it before any other stuff.

### `doc.flatten(space, tbl)`
    Converts table `tbl` to flat array, updating schema for space `space` as necessary.

### `doc.unflatten(space, tbl)`
    Converts flat array `tbl` to a nested document, according to schema for space `space`.

### `create_index(index_name, options)`
    Behaves similar to `box.space.create_index()`, but allows to specify string field names in addition to numeric in `parts`.

### `field_key(space, field_name)`
    Returns integer key for field named `field_name` in a flattened document.
    If you need a key for nested documents, use dot notation, like: `"foo.bar.id"`.
