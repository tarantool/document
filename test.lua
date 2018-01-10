#!/usr/bin/env tarantool
-- luacheck: globals box

local json = require('json')
local tap = require('tap')
local document = require('document')
local fun = require('fun')

local test = tap.test('document')

local function script_path()
   local str = debug.getinfo(2, "S").source:sub(2)
   return str:match("(.*/)") or '.'
end

local function get_format(space_name)
    if box.space[space_name] ~= nil then
        return box.space[space_name]:format()
    end

    return nil
end

local function drop_space(space_name)
    if box.space[space_name] ~= nil then
        box.space[space_name]:drop()
    end
end



local function init()
    local work_dir = script_path() .. '/.test'

    local cfg = {}

    if os.getenv('TARANTOOL_VERSION') == nil then
        cfg.memtx_dir = work_dir
        cfg.vilyl_dir = work_dir
        cfg.wal_dir = work_dir
    end


    os.execute(string.format("mkdir -p '%s'", work_dir))
    box.cfg(cfg)
end


local function run_test(test_fun)
    drop_space('test')
    drop_space('test2')

    box.schema.create_space('test')
    box.schema.create_space('test2')

    document.create_index(box.space.test, 'primary', {parts={'id', 'unsigned'}})
    document.create_index(box.space.test2, 'primary', {parts={'id', 'unsigned'}})

    test_fun()
end

local function basic_test()
    test:is(document.field_key(box.space.test, "id"), 1, "Field 'id' index is 1")

    local v = {id = 1, foo="bar", baz = {qux = 123, fff = {1,2,3}}}

    box.space.test:put(document.flatten(box.space.test, v))

    local ret = document.unflatten(box.space.test, box.space.test:get(1))

    test:is_deeply(v, ret, "Input and output values are equal")

    local s = document.get_schema(box.space.test)

    local expected_schema = {id = {1,"unsigned",false},
                             foo = {4,"string",true},
                             baz = {qux = {2,"scalar",true},
                                    fff = {3,"array",true}}}

    test:is_deeply(s, expected_schema, "Schemas equal")

    local flat_obj = document.flatten(s, v)

    test:is_deeply(flat_obj, {1, 123, {1,2,3}, "bar"}, "Flat object is correctly produced")

    local unflatten_obj = document.unflatten(s, flat_obj)

    test:is_deeply(v, unflatten_obj, "Unflattened objects are the same")
end

local function select_test()
    document.insert(box.space.test, {id=1, foo=2})
    document.insert(box.space.test, {id=2, foo=3})
    document.insert(box.space.test, {id=3, foo=4})
    document.insert(box.space.test, {id=4, foo=5})
    document.insert(box.space.test, {id=5, foo1=3})

    local res

    res = fun.totable(document.select(box.space.test, {{"$id", ">=", 1},
                                          {"$foo", ">", 1}}, {limit=2}))

    test:is_deeply({{id=1, foo=2}, {id=2, foo=3}},
        res, "select 1")

    res = fun.totable(document.select(box.space.test, {{"$id", ">=", 1},
                                          {"$foo", ">", 1}}, {limit=5}))

    test:is_deeply({{id=1, foo=2}, {id=2, foo=3},
            {id=3, foo=4}, {id=4, foo=5}},
        res, "select 2")

    res = fun.totable(document.select(box.space.test, {{"$foo", ">=", 0}}))

    test:is_deeply({{id=1, foo=2}, {id=2, foo=3},
            {id=3, foo=4}, {id=4, foo=5}},
        res, "select 3")

    res = fun.totable(document.select(box.space.test, nil, {limit=2}))

    test:is_deeply({{id=1, foo=2}, {id=2, foo=3}},
        res, "select 4")

    res = fun.totable(document.select(box.space.test, nil, {limit=2, offset=2}))

    test:is_deeply({{id=3, foo=4}, {id=4, foo=5}},
        res, "select 5")

    res = fun.totable(document.select(box.space.test, nil, {offset=3}))

    test:is_deeply({{id=4, foo=5}, {id=5, foo1=3}},
        res, "select 6")

    res = fun.totable(document.select(box.space.test, {{"$id", ">", 1},
                                          {"$id", "<", 4}}))

    test:is_deeply({{id=2, foo=3}, {id=3, foo=4}},
        res, "select 7")
end

local function count_test()
    document.insert(box.space.test, {id=1, foo=2})
    document.insert(box.space.test, {id=2, foo=3})
    document.insert(box.space.test, {id=3, foo=4})
    document.insert(box.space.test, {id=4, foo=5})
    document.insert(box.space.test, {id=5, foo1=3})

    local res

    res = document.count(box.space.test, {{"$id", ">=", 2},
                             {"$foo", ">", 1}}, {limit=2})

    test:is(2, res, 'count 1')

    res = document.count(box.space.test, nil)

    test:is(5, res, 'count 2')

    res = document.count(box.space.test, {{"$foo", ">=", 0}})

    test:is(4, res, 'count 3')
end

local function delete_test()
    document.insert(box.space.test, {id=1, foo=2})
    document.insert(box.space.test, {id=2, foo=3})

    local res

    res = fun.totable(document.select(box.space.test))

    test:is_deeply(res, {{id=1, foo=2}, {id=2, foo=3}}, "delete 1")

    document.delete(box.space.test, {{"$id", "==", 2}})

    res = fun.totable(document.select(box.space.test))

    test:is_deeply(res, {{id=1, foo=2}}, "delete 2")

    document.delete(box.space.test)

    res = fun.totable(document.select(box.space.test))

    test:is_deeply(res, {}, "delete 3")
end

local function join_test()
    document.insert(box.space.test, {id=1, foo=2})
    document.insert(box.space.test, {id=2, foo=3})
    document.insert(box.space.test, {id=3, foo=4})
    document.insert(box.space.test, {id=4, foo=5})
    document.insert(box.space.test, {id=5, foo1=3})

    document.insert(box.space.test2, {id=1, foo=6})
    document.insert(box.space.test2, {id=2, foo=7})
    document.insert(box.space.test2, {id=3, foo=8})
    document.insert(box.space.test2, {id=5, foo=9})

    local res

    res = fun.totable(document.join(box.space.test, box.space.test2,
                                    {{"$id", "==", "$id"}}, {limit=2}))

    test:is_deeply({{{id=1, foo=2}, {id=1, foo=6}},
            {{id=2, foo=3}, {id=2, foo=7}}}, res, "join 1")


    res = fun.totable(document.join(box.space.test, box.space.test2,
                                    {{"$id", "==", "$id"}}, {limit=5}))

    test:is_deeply({{{id=1, foo=2}, {id=1, foo=6}},
            {{id=2, foo=3}, {id=2, foo=7}},
            {{id=3, foo=4}, {id=3, foo=8}},
            {{id=5, foo1=3}, {id=5, foo=9}}},
        res, "join 2")

    res = fun.totable(document.join(box.space.test, box.space.test2,
                                    {{"$id", "==", "$id"}, {"$foo", ">=", 0}}))

    test:is_deeply({{{id=1, foo=2}, {id=1, foo=6}},
            {{id=2, foo=3}, {id=2, foo=7}},
            {{id=3, foo=4}, {id=3, foo=8}}},
        res, "join 3")

    res = fun.totable(document.join(box.space.test, box.space.test2,
                                    {{"$id", "==", "$id"}}))

    test:is_deeply({{{id=1, foo=2}, {id=1, foo=6}},
            {{id=2, foo=3}, {id=2, foo=7}},
            {{id=3, foo=4}, {id=3, foo=8}},
            {{id=5, foo1=3}, {id=5, foo=9}}},
        res, "join 4")

    res = fun.totable(document.join(box.space.test, box.space.test2,
                                    {{"$id", "==", "$id"}}, {limit=2, offset=2}))

    test:is_deeply({{{id=3, foo=4}, {id=3, foo=8}},
            {{id=5, foo1=3}, {id=5, foo=9}}},
        res, "join 5")
end

local function join_count_test()
    document.insert(box.space.test, {id=1, foo=2})
    document.insert(box.space.test, {id=2, foo=3})
    document.insert(box.space.test, {id=3, foo=4})
    document.insert(box.space.test, {id=4, foo=5})
    document.insert(box.space.test, {id=5, foo1=3})

    document.insert(box.space.test2, {id=1, foo=6})
    document.insert(box.space.test2, {id=2, foo=7})
    document.insert(box.space.test2, {id=3, foo=8})
    document.insert(box.space.test2, {id=5, foo=9})

    local res

    res = document.join_count(box.space.test, box.space.test2, {{"$id", "==", "$id"}})

    test:is(res, 4, 'join count 1')

    res = document.join_count(box.space.test, box.space.test2, {{"$id", "==", "$id"}, {"$foo", ">", 2}})

    test:is(res, 2, 'join count 2')
end

init()
run_test(basic_test)
run_test(select_test)
run_test(count_test)
run_test(delete_test)
run_test(join_test)
run_test(join_count_test)

os.exit(0)
