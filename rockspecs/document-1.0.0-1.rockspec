package = 'document'
version = '1.0.0-1'

source  = {
    url    = 'git://github.com/tarantool/document.git';
    tag = '1.0.0';
}

description = {
    summary  = "Effortless JSON storage for Tarantool";
    detailed = [[
Using this module you can store and retrieve dictionaries with very little overhead. It figures out document schema on the fly, and progressively updates it when new fields are received.
    ]];
    homepage = 'https://github.com/tarantool/document.git';
    maintainer = "Konstantin Nazarov <mail@kn.am>";
    license  = 'BSD2';
}

build = {
    type = 'builtin';
    modules = {
        ['document'] = 'document.lua';
    }
}
