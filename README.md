# Efficiently store JSON documents in Tarantool spaces

Well, not exactly JSON, but Lua tables, because Tarantool speaks
Lua. But you can convert any JSON to a table with the `json` module.

You may use this module to receive and store structured data you get
from external world. It has a few important strengths:

-   You are not forced to define any kind of schema for your documents
-   Still, they are stored with very little redundancy
-   You can build indices on arbitrary fields (even nested)
-   There are convenient high-level functions for data manipulation
-   The module works transparently for local spaces, remote spaces and even sharded spaces!
-   You can do "eventually consistent" selects and joins across sharded spaces!


## Example usage

Boilerplate:

    doc = require('document')
    json = require('json')

    box.cfg{}

    box.schema.create_space('test', {if_not_exists = true})
    doc.create_index(box.space.test, 'primary',
                     {parts={'id', 'unsigned'}, if_not_exists=true})

Actual data manipulation

    doc.insert(box.space.test, {id=1, foo="foo", bar={baz=3}})
    doc.insert(box.space.test, {id=2, foo="bar", bar={baz=0}})

    print('All tuples')
    for r in doc.select(box.space.test) do
        print('tuple:', json.encode(r))
    end

    print('Tuples where bar.baz > 0')
    for r in doc.select(box.space.test, {{'$bar.baz', '>', 0}}) do
        print('tuple:', json.encode(r))
    end

    print('Deleting a tuple where primary key == 2')
    doc.delete(box.space.test, {{"$id", "==", 2}})


## How it works

A naive implementation would have just stored JSON documents as
strings inside a tuple, and extracted indices into separate fields of
the tuple.

A more optimized approach is what mongodb or postgresql are doing:
instead of storing JSON documents as text, invent a compact binary
format and store it inside a tuple.

But we decided to take another approach, and dynamically figure out
document schema. We walk through the incoming document and put each
leaf element into a separate tuple field, essentially "flattening" it.
If we already saw such field previously, then schema already contains
a mapping between path in the document and a position inside the
tuple. If not, then we extend the schema and add a new field,
assigning a new rightmost column in the tuple to store its data.

When data is selected back, we reconstruct the original object using
document schema.

Our experiments show that most documents can achieve 5x to 10x
compression due to the method, because the schema is stored only once
per space.

## Queries

Queries are written using Lua tables, and are just lists of conditions
of the following form:

    {left, op, right}

Where `left` and `right` parts of the condition are either regular
values or references to field name, and `op` is a comparison operator.

Example values for `left` and `right`:

-   `1`
-   `nil`
-   `"foo"`
-   `"$id"`

Here, the `"$id"` is a special form that references tuple field by
name. You can put a "path" there, separated with ".", like
`"$foo.bar.val"`.

Example values for `op`:

-   `">"`
-   `">="`
-   `"=="`
-   `"<="`
-   `"<"`

Query examples:

-   `{{"$id", ">", 10}}`
-   `{{"$id", ">", 10}, {"$id", "<", 100}}`
-   `{{"$user.name", "==", "foo"}, {"$qty", "==", 0}}`


## High level API

### `doc.insert(space, tbl)`

Insert document `tbl` into `space`.

### `doc.delete(space, query)`

Insert table `tbl` into `space`.

Delete documents from `space`, that match `query` (see Queries above)

### `doc.select(space, query)`

Select documents from `space` that match `query` (see Queries above)
and return an iterator to the result set.

### `doc.join(space1, space2, query)`

Perform an inner join of spaces `space1` and `space2`, where both
items satisfy `query` (see Queries above).

## Low level API

### `doc.flatten(space, tbl)`

Converts document tbl to flat array, updating schema for space space as necessary.

### `doc.unflatten(space, tbl)`

Converts flat array tbl to a nested document, according to schema for space space.

### `create_index(index_name, options)`

Behaves similar to box.space.create<sub>index</sub>(), but allows to specify string field names in addition to numeric in parts.

### `field_key(space, field_name)`

Returns integer key for field named field<sub>name</sub> in a flattened document. If you need a key for nested documents, use dot notation, like: "foo.bar.id".
