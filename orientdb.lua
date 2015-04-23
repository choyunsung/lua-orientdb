local _M = {
  VERSION = 0.1,
  DB = {
    GRAPH = {},
    DOCUMENT = {}
  },
  STORAGE = {
    MEMORY = {},
    PHYSICAL = {}
  },
  MIN_PROTOCOL = 28,
  MAX_PROTOCOL = 28
}

function _M.new(host, port, sockets_lib)
  local client = {}
  
  function client:connect(user, password)
  end
  
  function client:disconnect()
  end
  
  function client:db_create(name, db_type, storage_type)
  end
  
  function client:db_drop(name)
  end
  
  function client:db_exists(name, storage_type)
  end
  
  function client:db_list()
  end

  function client:db_open(db_name, db_type, user, password)
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