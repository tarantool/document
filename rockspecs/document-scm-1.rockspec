package = 'document'
version = 'scm-1'

source  = {
    url    = 'git://github.com/tarantool/document.git';
    branch = 'master';
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
