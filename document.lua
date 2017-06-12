#!/usr/bin/env tarantool

local math = require('math')
local json = require('json')
local yaml = require 'yaml'
local shard = require 'shard'

local MAX_REMOTE_CONNS_TO_CACHE = 100

local local_schema_cache = {}
local remote_schema_cache = {}
setmetatable(remote_schema_cache, { __mode = 'k' })

local local_schema_id = nil
local remote_schema_id = {}

local function is_array(table)
    local max = 0
    local count = 0
    for k, v in pairs(table) do
        if type(k) == "number" then
            if k > max then max = k end
            count = count + 1
        else
            return false
        end
    end
    if max > count * 2 then
        return false
    end

    return true
end

local function split(inputstr, sep)
        if sep == nil then
                sep = "%s"
        end
        local t={} ; i=1
        for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
                t[i] = str
                i = i + 1
        end
        return t
end

local function startswith(str1, str2)
    return string.sub(str1, 1, string.len(str2)) == str2
end

local function get_tarantool_type(value)
    local type_name = type(value)
    if type_name == "string" then
        return "string"
    elseif type_name == "number" then
        return "scalar"
    elseif type_name == "boolean" then
        return "scalar"
    elseif type_name == "table" then
        if is_array(value) then
            return "array"
        else
            return "map"
        end
    end

    return nil
end

local function split(inputstr, sep)
        if sep == nil then
                sep = "%s"
        end
        local t={}
        local i=1
        for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
                t[i] = str
                i = i + 1
        end
        return t
end

local function shallowcopy(orig)
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in pairs(orig) do
            copy[orig_key] = orig_value
        end
    else -- number, string, boolean, etc
        copy = orig
    end
    return copy
end

local function flatten_schema(schema)
    local function flatten_schema_rec(res, path, schema)
        for k, v in pairs(schema) do
            local subpath = nil
            if path == nil then
                subpath = k
            else
                subpath = path .. "." .. k
            end

            if v[1] == nil then
                flatten_schema_rec(res, subpath, v)
            else
                res[v[1]] = {[subpath] = v[2] }
            end
        end
    end

    local res = {}

    if schema ~= nil then
        flatten_schema_rec(res, nil, schema)
    end

    return res
end

local function unflatten_schema(schema)
    local root = {}

    for i, v in ipairs(schema) do
        for k, type_name in pairs(v) do
            local parts = split(k, '.')
            local node = root
            for part_no = 1,#parts-1 do
                local part = parts[part_no]
                if node[part] == nil then
                    node[part] = {}
                end
                node = node[part]
            end

            node[parts[#parts]] = {i, type_name}
        end
    end


    return root
end

local function get_schema(space)
    if space.connection == nil then
        if local_schema_id ~= box.internal.schema_version() then
            local_schema_cache = {}
            local_schema_id = box.internal.schema_version()
        end

        local cached = local_schema_cache[space.id]

        if cached == nil then
            local flat = space:format()
            cached = unflatten_schema(flat)
            local_schema_cache[space.id] = cached
        end

        return cached
    else
        local remote = space.connection

        local conn_id = remote
        local conn = remote_schema_cache[conn_id]

        if not conn then
            if #remote_schema_cache > MAX_REMOTE_CONNS_TO_CACHE then
                remote_schema_cache = {}
                remote_schema_id = {}
            end

            conn = {}
            remote_schema_cache[conn_id] = conn
        end

        local schema_id = remote_schema_id[conn_id]

        if schema_id ~= remote.schema_version then
            conn = {}
            remote_schema_cache[conn_id] = conn
            remote_schema_id[conn_id] = remote.schema_version
        end

        local cached = conn[space.id]

        if cached == nil then
            -- There's a bug in net.box when space objects are not
            -- updated on schema change. We then have to re-request
            -- the object from net.box
            local flat = remote.space[space.id]:format()
            cached = unflatten_schema(flat)
            conn[space.id] = cached
        end

        return cached
    end
end

local function set_schema(space, schema, old_schema)
    schema = flatten_schema(schema)
    old_schema = flatten_schema(old_schema)

    if space.connection == nil then
        space:format(schema)
    else

        local remote = space.connection
        local result = remote:call('_document_remote_set_schema', {space.id, schema, old_schema})
        if result ~= nil then
            remote:reload_schema()
        end

        return result
    end
end

function _document_remote_set_schema(space_id, schema, old_schema)
    if box.session.user() ~= 'admin' then
        return box.session.su('admin', _document_remote_set_schema,
                              space_id, schema, old_schema)
    end

    local local_schema = box.space[space_id]:format()

    if #local_schema ~= #old_schema then
        return nil
    end

    -- Compare local and the previous version of remote schema
    for i, v in pairs(local_schema) do
        local lhs = v
        local rhs = old_schema[i]

        for lhs_key, lhs_type_name in pairs(lhs) do
            for rhs_key, rhs_type_name in pairs(rhs) do
                if lhs_key ~= rhs_key or lhs_type_name ~= rhs_type_name then
                    return nil
                end
                break
            end
            break
        end
    end

    box.space[space_id]:format(schema)

    return schema
end

local function schema_get_max_index(schema)
    local max_index = 0

    for _, v in pairs(schema or {}) do
        if v[1] == nil then
            max_index = math.max(max_index, schema_get_max_index(v))
        else
            max_index = math.max(max_index, v[1])
        end
    end
    return max_index
end

local function extend_schema(tbl, schema)
    local function extend_schema_rec(tbl, schema, max_index)
        schema = shallowcopy(schema)
        for k, v in pairs(tbl) do
            if type(v) == "table" then
                local new_schema = nil
                new_schema, max_index = extend_schema_rec(
                    v, schema[k] or {}, max_index)
                schema[k] = new_schema
            elseif schema[k] == nil then
                max_index = max_index + 1
                schema[k] = {max_index, get_tarantool_type(v)}
            end
        end

        return schema, max_index
    end

    local max_index = schema_get_max_index(schema)
    local _ = nil
    schema, _ = extend_schema_rec(tbl, schema or {}, max_index)
    return schema
end

local function flatten_table(tbl, schema)
    local function flatten_table_rec(res, tbl, schema)
        for k, v in pairs(tbl) do
            local entry = schema[k]
            if entry == nil then
                return nil
            end

            if type(v) == "table" then
                flatten_table_rec(res, v, entry)
            else
                res[entry[1]] = v
            end
        end
        return res
    end

    if tbl == nil then
        return nil
    end

    return flatten_table_rec({}, tbl, schema)
end

local function unflatten_table(tbl, schema)
    local function unflatten_table_rec(tbl, schema)
        local res = {}
        local is_empty = true
        for k, v in pairs(schema) do
            if v[1] == nil then
                local subres = unflatten_table_rec(tbl, v)
                if subres ~= nil then
                    res[k] = subres
                    is_empty = false
                end
            elseif tbl[v[1]] ~= nil then
                res[k] = tbl[v[1]]
                is_empty = false
            end
        end
        if is_empty then
            return nil
        end

        return res
    end

    if tbl == nil then
        return nil
    end

    return unflatten_table_rec(tbl, schema)
end

local function flatten(space_or_schema, tbl)

    if tbl == nil then
        return nil
    end

    if type(tbl) ~= "table" then
        return tbl
    end

    if space_or_schema == nil then
        return nil
    end

    local is_space = true
    local schema = nil

    if getmetatable(space_or_schema) == nil then
        is_space = false
        schema = space_or_schema
    else
        schema = get_schema(space_or_schema)
    end

    local result = flatten_table(tbl, schema)

    if result ~= nil then
        return result
    end

    if not is_space then
        return nil
    end

    local new_schema = nil
    if space_or_schema.connection == nil then
        new_schema = extend_schema(tbl, schema)
        set_schema(space_or_schema, new_schema)
    else

        while true do
            schema = get_schema(space_or_schema)
            new_schema = extend_schema(tbl, schema)
            local result = set_schema(space_or_schema, new_schema, schema)
            if result ~= nil then
                break
            end
        end
    end


    return flatten_table(tbl, new_schema)
end

local function unflatten(space_or_schema, tbl)
    if tbl == nil then
        return nil
    end

    if space_or_schema == nil then
        return nil
    end

    local schema = nil

    if getmetatable(space_or_schema) == nil then
        schema = space_or_schema
    else
        schema = get_schema(space_or_schema)
    end

    return unflatten_table(tbl, schema)
end

local function schema_add_path(schema, path, path_type)
    local path_dict = split(path, ".")

    local root = schema
    for k, v in ipairs(path_dict) do
        if k ~= #path_dict then
            if schema[v] == nil then
                schema[v] = {}
                schema = schema[v]
            else
                schema = schema[v]
            end
        else
            local max_index = schema_get_max_index(root)

            schema[v] = {max_index + 1, path_type}
        end
    end
    return root
end

local function document_get_field_by_path(doc, path)
    local path_dict = split(path, ".")

    for k, v in ipairs(path_dict) do
        doc = doc[v]
        if doc == nil then
            return nil
        end
    end

    if type(doc) == "table" then
        return nil
    end

    return doc
end

local function schema_get_field_key(schema, path)
    local path_dict = split(path, ".")

    for k, v in ipairs(path_dict) do
        schema = schema[v]
        if schema == nil then
            return nil
        end
    end


    if type(schema[1]) == "table" then
        return nil
    end

    return schema[1]
end

local function field_key(space_or_schema, path)
    local schema = nil

    if getmetatable(space_or_schema) == nil then
        schema = space_or_schema
    else
        schema = get_schema(space_or_schema)
    end

    return schema_get_field_key(schema, path)
end

local function field_index(space, path)
    local key = field_key(space, path)

    for _, idx in pairs(space.index) do
        local parts = idx.parts
        if #parts == 1 and parts[1].fieldno == key then
            return idx
        end
    end

    return nil
end

local function create_index(space, index_name, orig_options)
    local schema = get_schema(space)

    local options = {}

    if orig_options ~= nil then
        options = shallowcopy(orig_options)
    end

    options.parts = {'id', 'unsigned'}

    if orig_options ~= nil and orig_options.parts ~= nil then
        options.parts = orig_options.parts
    end

    local res = {}
    for k, v in ipairs(options.parts) do
        if k % 2 == 1 then
            if type(v) == "string" then
                local field_key = schema_get_field_key(schema, v)
                if field_key == nil then
                    schema = schema_add_path(schema, v, options.parts[k+1])
                    set_schema(space, schema)
                    field_key = schema_get_field_key(schema, v)
                end
                table.insert(res, field_key)
            else
                table.insert(res, v)
            end
        else
            table.insert(res, v)
        end
    end
    options.parts = res

    space:create_index(index_name, options)

end

local function op_to_tarantool(op_str)
    if op_str == "==" then
        return "EQ"
    elseif op_str == "<" then
        return "LT"
    elseif op_str == "<=" then
        return "LE"
    elseif op_str == ">" then
        return "GT"
    elseif op_str == ">=" then
        return "GE"
    else
        return nil
    end
end

local function op_to_function(op_str)
    if op_str == "==" then
        return function(lhs, rhs) return lhs == rhs end
    elseif op_str == "<" then
        return function(lhs, rhs) return lhs < rhs end
    elseif op_str == "<=" then
        return function(lhs, rhs) return lhs <= rhs end
    elseif op_str == ">" then
        return function(lhs, rhs) return lhs > rhs end
    elseif op_str == ">=" then
        return function(lhs, rhs) return lhs >= rhs end
    else
        return nil
    end
end

local function invert_op(op_str)
    if op_str == "==" then
        return "=="
    elseif op_str == "<" then
        return ">"
    elseif op_str == "<=" then
        return ">="
    elseif op_str == ">" then
        return "<"
    elseif op_str == ">=" then
        return "<="
    else
        return nil
    end
end

local function condition_type(condition)
    local is_left = false
    local is_right = false

    if type(condition[1]) == "string" and startswith(condition[1], "$") then
        is_left = true
    end

    if type(condition[3]) == "string" and startswith(condition[3], "$") then
        is_right = true
    end

    if is_left and is_right then
        return "both"
    elseif is_left then
        return "left"
    elseif is_right then
        return "right"
    else
        return nil
    end
end

local function condition_get_index(space, condition)
    local schema = get_schema(space)

    if type(condition[1]) ~= "string" or not startswith(condition[1], "$") then
        return nil
    end

    local field = string.sub(condition[1], 2, -1)

    local index = field_index(space, field)

    return index
end

local function validate_select_condition(condition)
    if #condition ~= 3 then
        error("Malformed condition: " .. json.encode(condition))
    end

    if condition_type(condition) ~= "left" then
        error("Condition should have field name on the left and value on the right")
    end

    if op_to_tarantool(condition[2]) == nil then
        error("Operation not supported: " .. condition[2])
    end
end

local function validate_join_condition(condition)
    if #condition ~= 3 then
        error("Malformed condition: " .. json.encode(condition))
    end

    if condition_type(condition) == nil then
        error("Condition should have at least one field name")
    end

    if op_to_tarantool(condition[2]) == nil then
        error("Operation not supported: " .. condition[2])
    end
end


local function take_batch(it, batch_size)
    local res = {}

    for i=1,batch_size do
        local val = it()
        if val == nil then
            return res
        end
        res[i] = val
    end

    return res
end

local function space_type(space)
    if space.connection ~= nil then
        return "net.box"
    elseif space.q_insert ~= nil then
        return "shard"
    else
        return "space"
    end
end

local function get_underlying_spaces(space)
    if space_type(space) == "shard" then
        local spaces = {}

        for _, zone in pairs(shard.shards) do
            local space = zone[1].conn.space[space.name]
            table.insert(spaces, space)
        end
        return spaces
    else
        return {space}
    end
end

local function space_get_primary_key(space)
    local idx = space.index[0]
    local key = idx.parts[1]

    return key
end

local function shard_get_primary_key_path(candidate_space)
    local space = nil

    for _, zone in pairs(shard.shards) do
        space = zone[1].conn.space[candidate_space.name]
        break
    end

    local idx = space.index[0]
    local key = idx.parts[1].fieldno

    local format = space:format()

    local key_fmt = format[key]

    for name, _ in pairs(key_fmt) do
        return name
    end

    return nil
end

local function local_document_insert(space, value)
    local flat = flatten(space, value)
    space:replace(flat)
end

local function net_box_document_insert(space, value)
    local flat = flatten(space, value)
    space:replace(flat)
end

local function shard_document_insert(space, value)
    if space == nil then
        error("space should not be nil")
    end

    local space_name = space.name

    if value == nil then
        error("value should not be nil")
    end

    local primary_key_path = shard_get_primary_key_path(space)

    local shard_key = document_get_field_by_path(value, primary_key_path)

    if shard_key == nil then
        error("doc should have " .. shard_key_name)
    end

    local nodes = shard.shard(shard_key)
    local node = nodes[1]

    local function ins(space_obj)
        if getmetatable(space_obj) == nil then
            error("space is not net.box space")
        end

        local flat = flatten(space_obj, value)

        if flat == nil then
            error("Failed to flatten trade")
        end

        space_obj:replace(flat)
    end

    shard:space_call(space_name, node, ins)
end

local function document_insert(space, value)
    if space_type(space) == "space" then
        return local_document_insert(space, value)
    elseif space_type(space) == "net.box" then
        return net_box_document_insert(space, value)
    else
        return shard_document_insert(space, value)
    end
end

local function local_tuple_select(space, query, options)
    options = options or {}
    local limit = options.limit
    local schema = get_schema(space)
    local skip = nil
    local primary_value = nil
    local op = "ALL"
    local index = space.index[0]

    if query == nil then
        query = {}
    end

    for i=1,#query do
        if condition_get_index(space, query[i]) ~= nil then

            local primary_condition = query[i]

            validate_select_condition(primary_condition)

            local primary_field = string.sub(primary_condition[1], 2, -1)

            index = field_index(space, primary_field)
            op = op_to_tarantool(primary_condition[2])
            primary_value = primary_condition[3]
            skip = i
            break
        end
    end

    local checks = {}

    for i=1,#query do
        if i ~= skip then
            local condition = query[i]
            validate_select_condition(condition)
            local field = string.sub(condition[1], 2, -1)

            table.insert(checks, {field_key(space, field),
                                  op_to_function(condition[2]),
                                  condition[3]})
        end
    end

    local result = {}

    local fun, param, state = index:pairs(primary_value, {iterator = op})
    local count = 0

    local function gen()
        local val = nil
        state, val = fun(param, state)

        while state ~= nil do
            local matches = true

            for _, check in ipairs(checks) do
                local lhs = val[check[1]]
                local rhs = check[3]
                if not check[2](lhs, rhs) then
                    matches = false
                    break
                end
            end

            if matches then
                count = count + 1

                if limit ~= nil and count > limit then
                    return nil
                end
                return val
            end

            state, val = fun(param, state)
        end

        return nil
    end

    return gen
end

local function interruptible_tuple_select(space, query, options)
    options = options or {}
    local batch_size = options.batch_size or 10
    local schema = get_schema(space)
    local skip = nil
    local primary_value = nil
    local op = "ALL"
    local index = space.index[0]

    if query == nil then
        query = {}
    end

    for i=1,#query do
        if condition_get_index(space, query[i]) ~= nil then

            local primary_condition = query[i]

            validate_select_condition(primary_condition)

            local primary_field = string.sub(primary_condition[1], 2, -1)

            index = field_index(space, primary_field)
            op = op_to_tarantool(primary_condition[2])
            primary_value = primary_condition[3]
            skip = i
            break
        end
    end

    local primary_field_no = index.parts[1].fieldno
    local pk_field_no = space.index[0].parts[1].fieldno

    local checks = {}

    for i=1,#query do
        local condition = query[i]
        validate_select_condition(condition)
        local field = string.sub(condition[1], 2, -1)

        table.insert(checks, {field_key(space, field),
                              op_to_function(condition[2]),
                              condition[3]})
    end

    local result = {}

    local last_value = nil
    local processed_primary_keys = {}

    local function gen()
        local batch = {}
        local value = primary_value
        local opts = {iterator = op}

        if last_value ~= nil then
            value = last_value
            opts = {iterator = 'GE'}
        end

        for _, val in index:pairs(value, opts) do
            local new_value = val[primary_field_no]
            local pk = val[pk_field_no]

            if not processed_primary_keys[pk] then
                local matches = true

                for _, check in ipairs(checks) do
                    local lhs = val[check[1]]
                    local rhs = check[3]
                    if not check[2](lhs, rhs) then
                        matches = false
                        break
                    end
                end

                if matches then
                    table.insert(batch, val)
                end
            end

            if last_value == new_value then
                processed_primary_keys[pk] = true
            else
                processed_primary_keys = {}
                last_value = new_value
            end

            if #batch >= batch_size then
                break
            end
        end

        if #batch == 0 then
            return nil
        end

        return batch
    end

    return gen
end

local cursor_cache = {}

local function get_cursor(id)
    if id == nil then
        local cursor_id = #cursor_cache

        for i, cursor in pairs(cursor_cache) do
            if cursor == nil then
                cursor_id = i
                break
            end
        end

        local cursor = {}
        cursor_cache[cursor_id] = cursor
        return cursor
    else
        return cursor_cache[id]
    end
end

local function free_cursor(id)
    if id == nil then
        return
    end
    cursor_cache[id] = nil
end

function _document_remote_tuple_select(space_name, query, options, cursor_id)
    local space = box.space[space_name]

    local cursor = get_cursor(cursor_id)
    if cursor.gen == nil then
        cursor.gen = interruptible_tuple_select(space, query, options)
    end
    local gen = cursor.gen

    local batch = gen()

    if batch == nil or #batch == 0 then
        free_cursor(cursor_id)
        return {nil, nil}
    end

    return {batch, cursor_id}
end

local function remote_tuple_select(space, query, options)
    local conn = space.connection
    local space_name = space.name
    local batch = nil
    local cursor = nil
    local tuple_no = 0

    local ret = conn:call('_document_remote_tuple_select',
                          {space_name, query, options, nil})
    batch = ret[1]
    cursor = ret[2]

    local function gen()
        while true do
            if batch == nil then
                return nil
            end

            tuple_no = tuple_no + 1

            local tuple = batch[tuple_no]

            if tuple ~= nil then
                return tuple
            end

            if cursor ~= nil then
                ret = conn:call('_document_remote_tuple_select',
                                {space_name, query, batch_size, cursor})
                batch = ret[1]
                cursor = ret[2]
                tuple_no = 0
            else
                return nil
            end
        end
    end

    return gen
end

local function local_document_select(space, query, options)
    local fun = local_tuple_select(space, query, options)

    local function gen()
        return unflatten(space, fun())
    end

    return gen
end

local function tuple_select(space, query, options)
    if space_type(space) == "space" then
        return local_tuple_select(space, query, options)
    else
        return remote_tuple_select(space, query, options)
    end
end

local function remote_document_select(candidate_space, query, options)
    local state = nil
    local res = nil
    local count = 0
    options = options or {}
    local limit = options.limit
    local batch_size = options.batch_size or 1024

    local spaces = get_underlying_spaces(candidate_space)
    local space_no = 0
    local space = nil

    local space_iterator = nil
    local batch = nil

    local tuple_no = 0

    local select_next_space = nil
    local get_space_iterator = nil
    local select_next_batch = nil
    local select_next_tuple = nil

    select_next_space = function()
        space_no = space_no + 1
        space = spaces[space_no]
        if space == nil then
            return nil
        end

        return select_space_iterator
    end

    select_space_iterator = function()
        local leftover = nil
        if limit then
            leftover = limit - count

            if leftover <= 0 then
                return nil
            end
        end

        space_iterator = tuple_select(space, query,
                                      {limit=leftover, batch_size=batch_size})

        if space_iterator == nil then
            return nil
        end

        return select_next_batch
    end

    select_next_batch = function()
        if space_iterator == nil then
            return select_next_space
        end

        batch = take_batch(space_iterator, batch_size)

        if #batch < batch_size then
            space_iterator = nil
        end

        if #batch == 0 then
            return select_next_space
        end

        tuple_no = 0
        return select_next_tuple
    end

    select_next_tuple = function()
        tuple_no = tuple_no + 1

        local tuple = batch[tuple_no]

        if tuple == nil then
            return select_next_batch
        end

        count = count + 1

        if limit and count > limit then
            return nil
        end

        local res = unflatten(space, tuple)

        return select_next_tuple, res
    end

    state = select_next_space

    local function gen()
        while true do
            state, res = state()

            if state == nil then
                return nil
            end

            if res ~= nil then
                return res
            end
        end
    end

    return gen
end

local function document_select(space, query, options)
    if space_type(space) == "space" then
        return local_document_select(space, query, options)
    else
        return remote_document_select(space, query, options)
    end
end


local function local_batch_tuple_select(space, queries)
    local result = {}

    for i, query in ipairs(queries) do
        local it = local_tuple_select(space, query, options)
        local r = {}

        for value in it do
            table.insert(r, value)
        end
        result[i] = r
    end

    return result
end

function _document_remote_batch_tuple_select(space_name, queries)
    local space = box.space[space_name]

    return local_batch_tuple_select(space, queries, options)
end

local function batch_tuple_select(space, queries, options)
    if space_type(space) == "space" then
        return local_batch_tuple_select(space, queries, options)
    else
        local conn = space.connection
        local space_name = space.name
        return conn:call('_document_remote_batch_tuple_select',
                         {space_name, queries, options})
    end
end

local function local_document_delete(space, query)
    local schema = get_schema(space)
    local skip = nil
    local primary_value = nil
    local op = "ALL"
    local index = space.index[0]

    if query == nil then
        query = {}
    end

    for i=1,#query do
        if condition_get_index(space, query[i]) ~= nil then

            local primary_condition = query[i]

            validate_select_condition(primary_condition)

            local primary_field = string.sub(primary_condition[1], 2, -1)

            index = field_index(space, primary_field)
            op = op_to_tarantool(primary_condition[2])
            primary_value = primary_condition[3]
            skip = i
            break
        end
    end

    local pk_field_no = space.index[0].parts[1].fieldno
    local checks = {}

    for i=1,#query do
        if i ~= skip then
            local condition = query[i]
            validate_select_condition(condition)
            local field = string.sub(condition[1], 2, -1)

            table.insert(checks, {field_key(space, field),
                                  op_to_function(condition[2]),
                                  condition[3]})
        end
    end

    local result = {}

    for _, val in index:pairs(primary_value, {iterator = op}) do
        local pk = val[pk_field_no]
        space:delete(pk)
    end
end

function _document_remote_document_delete(space_name, query)
    local space = box.space[space_name]

    return local_document_delete(space, query)
end

local function remote_document_delete(candidate_space, query)
    local spaces = get_underlying_spaces(candidate_space)

    for _, space in ipairs(spaces) do
        local conn = space.connection
        local space_name = space.name

        local ret = conn:call('_document_remote_document_delete',
                              {space_name, query})
    end
end

local function document_delete(space, query)
    if space_type(space) == "space" then
        return local_document_delete(space, query)
    else
        return remote_document_delete(space, query)
    end
end


local function tuple_join(space1, space2, query, options)
    local left = {}
    local right = {}
    local both = {}
    options = options or {}
    local limit = options.limit

    for _, condition in ipairs(query) do
        validate_join_condition(condition)

        if condition_type(condition) == "left" then
            table.insert(left, condition)
        elseif condition_type(condition) == "right" then
            table.insert(right, {condition[3],
                                 invert_op(condition[2]),
                                 condition[1]})
        elseif condition_type(condition) == "both" then
            table.insert(both, {condition[3],
                                invert_op(condition[2]),
                                condition[1]})
        end
    end

    for _, condition in ipairs(right) do
        table.insert(both, condition)
    end


    local plan = {}
    local checks = {}

    for _, check in ipairs(both) do
        if type(check[3]) == "string" and startswith(check[3], "$") then
            local field = string.sub(check[3], 2, -1)
            local key = field_key(space1, field)

            table.insert(plan, {true, key})
            table.insert(checks, {check[1], check[2], nil})
        else
            table.insert(plan, {false})
            table.insert(checks, {check[1], check[2], check[3]})
        end
    end

    local left_iter = tuple_select(space1, left)
    local right_iter = nil
    local left_val = nil
    local count = 0

    local function gen()
        while true do
            if right_iter == nil then
                left_val = left_iter()

                if left_val == nil then
                    return nil
                end

                for i, p in ipairs(plan) do
                    if p[1] then
                        checks[i][3] = left_val[p[2]]
                    end
                end

                right_iter = tuple_select(space2, checks)
            else
                local right_val = right_iter()

                if right_val == nil then
                    right_iter = nil
                else
                    count = count + 1

                    if limit ~= nil and count > limit then
                        return nil
                    end

                    return {left_val, right_val}
                end
            end
        end
    end

    return gen
end

local function remote_document_join(space1, space2, query, options)
    local state = nil
    local res = nil
    options = options or {}
    local limit = options.limit
    local batch_size = options.batch_size or 1024

    local left_spaces = get_underlying_spaces(space1)
    local right_spaces = get_underlying_spaces(space2)
    local left_space_no = 0
    local right_space_no = 0
    local left_space = nil
    local right_space = nil

    local left_space_iterator = nil

    local batch = nil

    local select_next_left_space = nil
    local get_left_space_iterator = nil
    local select_next_left_batch = nil
    local select_next_right_space = nil
    local compile_checks = nil
    local select_tuples = nil
    local select_next_tuple = nil

    local tuples = nil
    local group_no = 0
    local tuple_no = 0
    local count = 0
    local leftover = nil

    local left_query = {}
    local right_query = {}

    local compiled_query = {}

    for _, condition in ipairs(query) do
        validate_join_condition(condition)

        if condition_type(condition) == "left" then
            table.insert(left_query, condition)
        else
            table.insert(right_query, {condition[3],
                                       invert_op(condition[2]),
                                       condition[1]})
        end
    end

    select_next_left_space = function()
        left_space_no = left_space_no + 1
        left_space = left_spaces[left_space_no]
        if left_space == nil then
            return nil
        end

        return select_left_space_iterator
    end

    select_left_space_iterator = function()
        if limit then
            leftover = limit - count

            if leftover <= 0 then
                return nil
            end
        end

        left_space_iterator = tuple_select(left_space, left_query,
                                           {limit=leftover, batch_size=batch_size})

        if left_space_iterator == nil then
            return nil
        end

        return select_next_left_batch
    end

    select_next_left_batch = function()
        if left_space_iterator == nil then
            return select_next_left_space
        end

        batch = take_batch(left_space_iterator, batch_size)

        if #batch < batch_size then
            left_space_iterator = nil
        end

        if #batch == 0 then
            return select_next_left_space
        end

        right_space_no = 0
        return select_next_right_space
    end

    select_next_right_space = function()
        right_space_no = right_space_no + 1
        right_space = right_spaces[right_space_no]
        if right_space == nil then
            return select_next_left_batch
        end

        return compile_checks
    end

    compile_checks = function()
        compiled_query = {}
        for i, tuple in ipairs(batch) do
            checks = {}
            compiled_query[i] = checks

            for j, condition in ipairs(right_query) do
                if type(condition[3]) == "string" and startswith(condition[3], "$") then
                    local field = string.sub(condition[3], 2, -1)
                    local key = field_key(left_space, field)

                    checks[j] = {condition[1], condition[2], tuple[key]}
                else
                    checks[j] = condition
                end
            end
        end

        return select_tuples
    end

    select_tuples = function()
        group_no = 1
        tuple_no = 0
        tuples = batch_tuple_select(right_space, compiled_query,
                                    {limit=leftover, batch_size=batch_size})

        if #tuples == 0 then
            return select_next_right_space
        end

        return select_next_tuple
    end

    select_next_tuple = function()
        local tuple = nil

        local group = tuples[group_no]

        if group == nil then
            return select_next_right_space
        end

        tuple_no = tuple_no + 1

        local tuple = group[tuple_no]

        if tuple == nil then
            group_no = group_no + 1
            tuple_no = 0
            return select_next_tuple
        end

        count = count + 1

        if limit and count > limit then
            return nil
        end

        local left = unflatten(left_space, batch[group_no])
        local right = unflatten(right_space, tuple)

        return select_next_tuple, {left, right}
    end

    state = select_next_left_space

    local function gen()
        while true do
            state, res = state()

            if state == nil then
                return nil
            end

            if res ~= nil then
                return res
            end
        end
    end

    return gen
end


local function local_document_join(space1, space2, query, options)
    local fun = tuple_join(space1, space2, query, options)

    local function gen()
        local res = fun()

        if res == nil then
            return nil
        end

        local tuple1 = res[1]
        local tuple2 = res[2]

        return {unflatten(space1, tuple1),
                unflatten(space2, tuple2)}
    end

    return gen
end

local function document_join(space1, space2, query, options)
    if space_type(space1) == "space" and space_type(space2) == "space" then
        return local_document_join(space1, space2, query, options)
    else
        return remote_document_join(space1, space2, query, options)
    end
end

return {flatten = flatten,
        unflatten = unflatten,
        create_index = create_index,
        field_key = field_key,
        get_schema = get_schema,
        set_schema = set_schema,
        extend_schema = extend_schema,
        insert = document_insert,
        select = document_select,
        delete = document_delete,
        join = document_join}
