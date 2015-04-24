local _M = {}
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

local function send_request(client, operation, fmt, ...)
  fmt = fmt or ''
  local arg = {...}
  local argc = select('#', ...)
  
  assert(#fmt == argc,
         ' format string ('..fmt..') length not equal to number of arguments ('..argc..')')
  local pack_args = {operation, client.session}
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
  local request = struct.pack(pack_fmt, unpack(pack_args))

  client.connection:send(request)
    
  local ok = struct.unpack('>b', client.connection:receive(1)) == 0
  local session = struct.unpack('>i4', client.connection:receive(4))
  
  if not ok then
    local error_msg = 'Request error. '
    while struct.unpack('>b', client.connection:receive(1)) == 1 do
      local n = struct.unpack('>i4', client.connection:receive(4))
      error_msg = error_msg..struct.unpack('>c'..n, client.connection:receive(n))..'. '
      n = struct.unpack('>i4', client.connection:receive(4))
      error_msg = error_msg..struct.unpack('>c'..n, client.connection:receive(n))..'.\n'
    end
    error(error_msg)
  end
  
  assert(client.session == session,
         'sessions do not match, got '..session..', expected '..client.session)
  
  local response = nil
  if client.session == -1 then
    client.session = struct.unpack('>i4', client.connection:receive(4))
  elseif ok then
    local n_response = struct.unpack('>i4', client.connection:receive(4))
    if n_response > 0 then
      response = struct.unpack('>i4', client.connection:receive(n_response))
    end
  end
  return ok, response
end

function _M.connect(client, user, password)
  return send_request(client, OP.CONNECT, 'sshssbss',
                      'lua-orientdb',
                      tostring(client.ODB.VERSION),
                      client.connection.PROTOCOL,
                      NULL_STRING,
                      'ORecordSerializerBinary',
                      FALSE,
                      user,
                      password
  )
end

function _M.db_open(client, db_name, db_type, user, password)
  return send_request(client, OP.DB_OPEN, 'sshssbssss',
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
end

function _M.db_create(client, db_name, db_type, storage_type)
  return send_request(client, OP.DB_CREATE, 'sss', db_name, db_type, storage_type)
end

function _M.db_drop(client, db_name, storage_type)
  return send_request(client, OP.DB_DROP, 'ss', db_name, storage_type)
end

function _M.db_exist(client, db_name, storage_type)
  return send_request(client, OP.DB_EXIST, 'ss', db_name, storage_type)
end

function _M.db_list(client)
  return send_request(client, OP.DB_LIST)
end

return _M