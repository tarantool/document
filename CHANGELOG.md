# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2018-01-17
### Added
- A fix for missing variable declaration reported by the strict mode

## [1.0.0] - 2018-01-10
### Added
- Support for regular spaces, net.box spaces and shard spaces
- Storing schema in `space:format()`, both for local and remote operation
- Flattening (compression) on the client side to preserve space and bandwidth
- Simplistic query language with support for map/reduce across shards
- Support for iterators in `:select()`
- Support for limit/offset in `:select()`
- Luarock-based packaging
- Basic unit tests
