local _M = {}

local function find_socket_library(sockets_lib)
  if sockets_lib then return require(sockets_lib) end
  local ok, lib = pcall(require, 'ngx.socket')
  if ok then return lib end
  ok, lib = pcall(require, 'socket')
  if ok then return lib end
  error('no sockets library found')
end

function _M.connect(host, port, sockets_lib)
  local connection = {}
  local lib = find_socket_library(sockets_lib)
  
  connection.tcp = assert(lib.tcp())
  connection.tcp:settimeout(1)
  local ok, err_msg = connection.tcp:connect(host, port)
  if not ok then error('establishing connection '..err_msg) end
  
  function connection:close()
    self.tcp:close()
  end
  
  function connection:send(request)
    local line, err = self.tcp:send(request)
    if not line then error('on send connection '..err) end
    return line
  end
  
  function connection:receive(bytes)
    local line, err = self.tcp:receive(bytes)
    if not line then error('on receive connection '..err) end
    return line
  end
  
  local pn = assert(connection.tcp:receive(2))
  local struct = require 'struct'
  connection.PROTOCOL = struct.unpack('>h', pn)
  
  local odb = require 'orientdb'
  if connection.PROTOCOL < odb.MIN_PROTOCOL then
    connection:close()
    error('protocol '..tonumber(connection.PROTOCOL)..
          ' is too old to be supported by this library')
  end
  if connection.PROTOCOL > odb.MAX_PROTOCOL then
    connection:close()
    error('protocol '..tonumber(connection.PROTOCOL)..
          ' is not yet supported by this library')
  end
  
  return connection
end

return _M