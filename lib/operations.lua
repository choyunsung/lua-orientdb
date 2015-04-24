local _M = {}

local debug = require 'lib.util'.debug

local OP = {
  -- server operations
  SHUTDOWN = 1,
  CONNECT  = 2,
  DB_OPEN   = 3,
  DB_CREATE = 4,
  DB_EXIST  = 6,
  DB_DROP   = 7,
  CONFIG_GET  = 70,
  CONFIG_SET  = 71,
  CONFIG_LIST = 72,
  DB_LIST   = 74,
  -- db operations
  DB_CLOSE  = 5,
  DB_SIZE   = 8,
  DB_COUNTRECORDS = 9,
  DB_RELOAD = 73,
  DB_COPY   = 90,
  DB_TRANSFER = 93,
  DB_FREEZE   = 94,
  DB_RELEASE  = 95,
  DATACLUSTER_ADD       = 10,
  DATACLUSTER_DROP      = 11,
  DATACLUSTER_COUNT     = 12,
  DATACLUSTER_DATARANGE = 13,
  DATACLUSTER_COPY      = 14,
  DATACLUSTER_LH_CLUSTER_IS_USED = 16,
  DATACLUSTER_FREEZE    = 96,
  DATACLUSTER_RELEASE   = 97,
  RECORD_METADATA = 29,
  RECORD_LOAD     = 30,
  RECORD_CREATE   = 31,
  RECORD_UPDATE   = 32,
  RECORD_DELETE   = 33,
  RECORD_COPY     = 34,
  RECORD_CLEAN_OUT = 38,
  COMMAND = 41,
  POSITIONS_FLOOR   = 39,
  POSITIONS_CEILING = 42,
  TX_COMMIT = 60,
  PUSH_RECORD = 79,
  PUSH_DISTRIB_CONFIG = 80,
  REPLICATION = 91,
  CLUSTER = 92,
  CREATE_SBTREE_BONSAI    = 110,
  SBTREE_BONSAI_GET       = 111,
  SBTREE_BONSAI_FIRST_KEY = 112,
  SBTREE_BONSAI_GET_ENTRIES_MAJOR = 113,
  RIDBAG_GET_SIZE = 114
}

local TRUE, FALSE = 1, 0
local NULL_STRING = string.char(255)

local function create_request(client, operation)
  local struct = require 'struct'
  
  local req = {
    CLIENT = client,
    OPERATION = operation
  }
  
  function req:write(fmt, ...)
    fmt = fmt or ''
    local arg = {...}
    local argc = select('#', ...)
    
    debug('writing request OP ', operation)
    
    assert(#fmt == argc,
           ' format string ('..fmt..') length not equal to number of arguments ('..argc..')')
    local pack_args = {self.OPERATION, self.CLIENT.session}
    for i = 1, #fmt do
      local v = arg[i]
      local f = fmt:sub(i, i)
      if f == 's' then
        if type(v) ~= 'string' then error('expected a string value, got '..type(v)) end
        table.insert(pack_args, #v)
      end
      table.insert(pack_args, v)
    end
    local pack_fmt = '>bi4'..fmt:gsub('s', 'ic0'):gsub('i', 'i4')
    self.contents = struct.pack(pack_fmt, unpack(pack_args))
  end
  
  function req:send()
    if not self.contents then self:write() end
    self.CLIENT.connection:send(self.contents)

    local ok = self:read_byte() == 0
    local session = self:read_int()
    
    debug('sent. ok=', tostring(ok), ', session=', session)
    
    if not ok then
      local error_msg = 'Request error. '
      while self:read_byte() == 1 do  
        error_msg = error_msg..self:read_string()..'. '
        error_msg = error_msg..self:read_string()..'.\n'
      end
      debug(error_msg)
      error(error_msg)
    end
    
    assert(self.CLIENT.session == session,
           'sessions do not match, got '..session..', expected '..self.CLIENT.session)
  end
  
  function req:read_string()
    return self:read_bytes() or ''
  end
  
  function req:read_strings()
    local n = req:read_int()
    local ret = {}
    for i = 1, n do
      ret.insert(req:read_string())
    end
    return ret
  end
  
  function req:read_int(bytes)
    bytes = bytes or 4
    return struct.unpack('>i'..bytes, self:_recv(bytes))
  end
  
  function req:read_byte()
    return self:read_int(1)
  end
  
  function req:read_bool()
    return self:read_byte() == TRUE
  end
  
  function req:read_bytes()
    local n = self:read_int()
    if n == 0 then return nil end
    return struct.unpack('>c'..n, self:_recv(n))
  end
  
  function req:read_record()
    local header = req:read_int(2)
    assert(header == 0 or header == -2 or header == -3, 'bad record header '..header)
    debug('record header '..header)
    if header == -2 then return nil end
    if header == -3 then return req:read_int(2), req:read_int(8) end
    
    local record_type = req:read_int(1)
    local cluster_id = req:read_int(2)
    local cluster_pos = req:read_int(8)
    local record_version = req:read_int()
    local contents = req:read_bytes()
    return cluster_id, cluster_pos, record_type, contents, record_version
  end
  
  function req:_recv(bytes)
    return self.CLIENT.connection:receive(bytes)
  end
  
  return req
end

function _M.connect(client, user, password)
  local req = create_request(client, OP.CONNECT)
  req:write('sshssbss',
            'lua-orientdb',
            tostring(client.ODB.VERSION),
            client.connection.PROTOCOL,
            NULL_STRING,
            'ORecordSerializerBinary',
            FALSE,
            user,
            password
            )
  req:send()
  client.session = req:read_int()
  debug('session set to ', client.session)
  assert(req:read_bytes() == nil) --token, should be empty
end

function _M.db_open(client, db_name, db_type, user, password)
  local req = create_request(client, OP.DB_OPEN)
  req:write('sshssbssss',
            'lua-orientdb',
            tostring(client.ODB.VERSION),
            client.connection.PROTOCOL,
            NULL_STRING,
            'ORecordSerializerBinary',
            FALSE,
            db_name,
            db_type,
            user,
            password
            )
  req:send()
  client.session = req:read_int()
  debug('session set to ', client.session)
  assert(req:read_bytes() == nil) --token, should be empty
end

function _M.db_create(client, db_name, db_type, storage_type)
  local req = create_request(client, OP.DB_CREATE)
  req:write('sss', db_name, db_type, storage_type)
  req:send()
end

function _M.db_drop(client, db_name, storage_type)
  local req = create_request(client, OP.DB_DROP)
  req:write('ss', db_name, storage_type)
  req:send()
end

function _M.db_exist(client, db_name, storage_type)
  local req = create_request(client, OP.DB_EXIST)
  req:write('ss', db_name, storage_type)
  req:send()
  return req:read_bool()
end

function _M.db_list(client)
  local req = create_request(client, OP.DB_LIST)
  req:send()
  return req:read_record()
end

return _M