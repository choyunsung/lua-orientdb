local _M = {
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
  DB_COPY	  = 90,
  DB_TRANSFER	= 93,
  DB_FREEZE	  = 94,
  DB_RELEASE	= 95,
  DATACLUSTER_ADD       = 10,
  DATACLUSTER_DROP      = 11,
  DATACLUSTER_COUNT     = 12,
  DATACLUSTER_DATARANGE = 13,
  DATACLUSTER_COPY      = 14,
  DATACLUSTER_LH_CLUSTER_IS_USED = 16,
  DATACLUSTER_FREEZE	  = 96,
  DATACLUSTER_RELEASE	  = 97,
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
  assert(#fmt == #arg,
         'format string length ('..#fmt..') not equal to number of arguments ('..#arg..')')
  local pack_args = {operation, client.session}
  for i = 1, #fmt do
    local v = arg[i]
    local f = fmt:sub(i, i)
    if f == 's' then
      table.insert(pack_args, #v)
    end
    table.insert(pack_args, v)
  end
  local pack_fmt = '>bi4'..fmt:gsub('s', 'ic0'):gsub('i', 'i4')
  local request = struct.pack(pack_fmt, unpack(pack_args))

  client.connection:send(request)
    
  local ok = struct.unpack('>b', client.connection:receive(1)) == 0
  local response = nil
  local session = struct.unpack('>i4', client.connection:receive(4))
  
  assert(client.session == session,
         'sessions do not match, got '..session..', expected '..client.session)
  
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
  return send_request(client, _M.CONNECT, 'sshssbss',
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
  return send_request(client, _M.DB_OPEN, 'sshssbssss',
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

function _M.db_list(client)
  return send_request(client, _M.DB_LIST)
end

return _M