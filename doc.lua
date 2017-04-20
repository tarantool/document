#!/usr/bin/env tarantool

local math = require("math")
local prefix = "_doc_"
local schema_space = nil

local function init()
    schema_space = box.schema.create_space(prefix .. "schema",
                                           {if_not_exists = true})
    schema_space:create_index("primary", {
                                  parts = { 1, 'unsigned' },
                                  if_not_exists = true
    })
end

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
    local res = schema_space:get(space.id)

    if res == nil then
        return {}
    end

    return res[2]
end

local function set_schema(space, schema)
    schema_space:put({space.id, schema})
end

local function conforms_to_schema(tbl, schema)
    for k,v in pairs(tbl) do
        if type(v) == "table" then
            if schema[k] == nil or not conforms_to_schema(v, schema[k]) then
                return false
            end
        else
            if schema[k] == nil then
                return false
            end
        end
    end
    return true
end

local function schema_get_max_index(schema)
    local max_index = 0

    for _, v in pairs(schema or {}) do
        if type(v) == "table" then
            max_index = math.max(max_index, schema_get_max_index(v))
        else
            max_index = math.max(max_index, v)
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
                schema[k] = max_index
            end
        end

        return schema, max_index
    end

    local max_index = schema_get_max_index(schema)

    schema, _ = extend_schema_rec(tbl, schema or {}, max_index)
    return schema
end

local function flatten_table(tbl, schema)
    local function flatten_table_rec(res, tbl, schema)
        for k, v in pairs(tbl) do
            if type(v) == "table" then
                flatten_table_rec(res, v, schema[k])
            else
                res[schema[k]] = v
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
            if type(v) == "table" then
                local subres = unflatten_table_rec(tbl, v)
                if subres ~= nil then
                    res[k] = subres
                    is_empty = false
                end
            elseif tbl[v] ~= nil then
                res[k] = tbl[v]
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


local function flatten(space, tbl)
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

    if not conforms_to_schema(tbl, schema) then
        schema = extend_schema(tbl, schema)
        set_schema(space, schema)
    end

    return flatten_table(tbl, schema)
end

local function unflatten(space, tbl)
    if tbl == nil then
        return nil
    end

    local schema = get_schema(space)

    return unflatten_table(tbl, schema)
end


local function flatten_key(schema, key)
    if type(key) == table then
        tbl = {}
        for k, v in ipairs(key) do
            tbl[k] = flatten_key(v)
        end
        return tbl
    elseif type(key) == "string" then
        return schema_get_field_key(schema, key)
    else
        return key
    end
end

local function schema_add_path(schema, path)
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

            schema[v] = max_index + 1
        end
    end
    return root
end

local function schema_get_field_key(schema, path)
    local path_dict = split(path, ".")

    for k, v in ipairs(path_dict) do
        schema = schema[v]
        if schema == nil then
            return nil
        end
    end

    if type(schema) == "table" then
        return nil
    end

    return schema
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
                    schema = schema_add_path(schema, v)
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

return {init = init,
        flatten = flatten,
        unflatten = unflatten,
        create_index = create_index,
        field_key = field_key}
