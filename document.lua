#!/usr/bin/env tarantool

local math = require("math")
local prefix = "_doc_"
local schema_space = nil

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
        local t={} ; i=1
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


local function get_schema(space)
    local function schema_decode(path, schema)

    end

    local _space = box.space._space
    local format = _space:get(space.id)[7]
    return format or {}
end

local function set_schema(space, schema)
    local function schema_encode(path, schema)

    end

    local _space = box.space._space
    _space:update(space.id, {{'=', 7, schema}})
end

local function schema_get_max_index(schema)
    local max_index = 0

    for i, _ in ipairs(schema or {}) do
        max_index = math.max(max_index, i)
    end
    return max_index
end

local function extend_schema(tbl, schema)
    local function extend_schema_rec(tbl, schema, inv_schema, path, max_index)
        for k, v in pairs(tbl) do
            if path == nil then
                subpath = k
            else
                subpath = path .. "." .. k
            end

            if type(v) == "table" then
                local new_schema = nil
                max_index = extend_schema_rec(
                    v, schema, inv_schema, subpath, max_index)
            elseif inv_schema[subpath] == nil then
                max_index = max_index + 1
                schema[max_index] = {[subpath] = get_tarantool_type(v)}
            end
        end

        return max_index
    end

    schema = shallowcopy(schema)

    local inv_schema = {}

    for i, v in ipairs(schema) do
        for k, _ in pairs(v) do
            inv_schema[k] = i
        end
    end

    local max_index = schema_get_max_index(schema)

    extend_schema_rec(tbl, schema, inv_schema, nil, max_index)
    return schema
end

local function flatten_table(tbl, schema)
    local function flatten_rec(res, path, schema, tbl)
        for k,v in pairs(tbl) do

            if path == nil then
                subpath = k
            else
                subpath = path .. "." .. k
            end

            if type(v) == "table" then
                local status = flatten_rec(res, subpath, schema, v)
                if status == nil then
                    return nil
                end
            else
                local i = schema[subpath]
                if i == nil then
                    return nil
                end
                res[i] = v
            end
        end

        return res
    end

    local inv_schema = {}

    for i, v in ipairs(schema) do
        for k, _ in pairs(v) do
            inv_schema[k] = i
        end
    end

    return flatten_rec({}, nil, inv_schema, tbl)
end

local function unflatten_table(tbl, schema)
    local res = {}

    local function add_entry(res, key, entry)
        if string.find(key, '.', 1, true) == nil then
            res[key] = entry
        else
            local strend = nil
            local node = res
            local subkey = nil
            local i = 1
            while i < #key do

                strend = string.find(key, '.', i, true)
                if strend ~= nil then
                    subkey = string.sub(key, i, strend-1)
                else
                    subkey = string.sub(key, i, strend)
                end

                if strend == nil then
                    node[subkey] = entry
                    break
                else
                    if node[subkey] == nil then
                        node[subkey] = {}
                    end
                    node = node[subkey]
                    i = strend + 1
                end
            end
        end
    end

    for i, v in ipairs(schema) do
        for k, _ in pairs(v) do
            add_entry(res, k, tbl[i])
        end
    end
    return res

end

local function flatten(space, tbl)
    if space.id == nil then
        local schema = space
        return flatten_table(tbl, schema)
    end

    if tbl == nil then
        return nil
    end

    if type(tbl) ~= "table" then
        return tbl
    end

    if is_array(tbl) then
        return tbl
    end

    local schema = get_schema(space)

    local result = flatten_table(tbl, schema)

    if result ~= nil then
        return result
    end

    schema = extend_schema(tbl, schema)
    set_schema(space, schema)

    return flatten_table(tbl, schema)
end

local function unflatten(space, tbl)
    if tbl == nil then
        return nil
    end

    local schema = get_schema(space)

    return unflatten_table(tbl, schema)
end

local function schema_add_path(schema, path, path_type)

    for _, v in ipairs(schema) do
        for k, _ in pairs(v) do
            if k == path then
                return
            end
        end
    end

    local max_index = schema_get_max_index(schema)

    schema = shallowcopy(schema)

    schema[max_index+1] = { [path] = path_type }

    return schema
end

local function schema_get_field_key(schema, path)
    for i, v in ipairs(schema) do
        for k, _ in pairs(v) do
            if k == path then
                return i
            end
        end
    end

    return nil
end

local function field_key(space, path)
    local schema = get_schema(space)

    return schema_get_field_key(schema, path)
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

return {flatten = flatten,
        unflatten = unflatten,
        create_index = create_index,
        field_key = field_key}
