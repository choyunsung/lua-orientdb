package = 'orientdb'
version = '0.1-1'
source = {
  url = 'git://github.com/polymeris/lua-orientdb',
  --tag = 'v1.0'
}
description = {
  summary = 'Bindings for OrientDB.',
  detailed = [[
    Lua bindings to the OrientDB (http://orientdb.com) graph database through its binary interface.
    Works with luasocket and ngx_lua.socket on Lua 5.1, Lua 5.2 and LuaJIT 2.0.
  ]],
  homepage = 'https://github.com/polymeris/lua-orientdb',
  license = 'MPL-2.0'
}
dependencies = {
  'lua ~> 5.1',
  'luasocket >= 2.0',
  'struct >= 1.2',
  'luabitop >= 1.0'
}
build = {
  type = "builtin",
  modules = {
    orientdb = 'orientdb.lua',
    ['orientdb.connection'] = 'orientdb/connection.lua',
    ['orientdb.operations'] = 'orientdb/operations.lua',
    ['orientdb.util'] = 'orientdb/util.lua'
  }
}
