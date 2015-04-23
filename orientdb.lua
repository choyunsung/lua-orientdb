local _M = {
  VERSION = 0.1,
  DB = {
    GRAPH = 'graph',
    DOCUMENT = 'document'
  },
  STORAGE = {
    MEMORY = 'memory',
    PHYSICAL = 'plocal'
  },
  MIN_PROTOCOL = 28,
  MAX_PROTOCOL = 28
}

function _M.new(host, port, sockets_lib)
  if type(host) ~= 'string' then error('hostname must be a string') end
  if port then
    port = tonumber(port) or error('port must be a number')
    if port < 0 or port > 0xffff or math.floor(port) ~= port then
      error('invalid port number')
    end
  end
  
  local connection = require 'lib.connection'
  local operations = require 'lib.operations'

  
  local client = {
    ODB = _M,
    connection = connection.connect(host, port, sockets_lib),
    session = -1
  }
  
  function client:connect(user, password)
    if not user or not password then error('missing credentials') end
    operations.connect(self, user, password)
  end
  
  function client:disconnect()
    self.connection:close()
  end
  
  function client:db_create(name, db_type, storage_type)
    if not name then error('db name missing') end
    if db_type ~= _M.DB.GRAPH and db_type ~= _M.DB.DOCUMENT then
      error('invalid db type')
    end
    storage_type = storage_type or _M.STORAGE.MEMORY
    if storage_type ~= _M.STORAGE.MEMORY and
       storage_type ~= _M.STORAGE.PHYSICAL then
       error('invalid storage type')
    end
    return assert(operations.db_create(self, name, db_type, storage_type))
  end
  
  function client:db_drop(db_name, storage_type)
    if not db_name then error('db name missing') end
    storage_type = storage_type or _M.STORAGE.MEMORY
    if storage_type ~= _M.STORAGE.MEMORY and
       storage_type ~= _M.STORAGE.PHYSICAL then
       error('invalid storage type')
    end
    return operations.db_drop(self, db_name, storage_type)
  end
  
  function client:db_exists(db_name, storage_type)
    if not db_name then error('db name missing') end
    storage_type = storage_type or _M.STORAGE.MEMORY
    if storage_type ~= _M.STORAGE.MEMORY and
       storage_type ~= _M.STORAGE.PHYSICAL then
       error('invalid storage type')
    end
    return operations.db_exist(self, db_name, storage_type)
  end
  
  function client:db_list()
    operations.db_list(self)
  end

  function client:db_open(db_name, db_type, user, password)
    operations.db_open(self, db_name, db_type, user, password)
    error('db functions not implemented')
    local db = {}
    
    function db:close()
    end
    
    function db:size()
    end
    
    function db:count_records()
    end
    
    function db:reload()
    end
    
    return db
  end
    
  function client:query()
  end
  
  function client:command()
  end
  
  function client:server_shutdown()
  end
  
  return client
end

return _M