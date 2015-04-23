odb = require 'orientdb'

local USER = 'root'
local PASSWORD = 'root'

for _, socket_lib in pairs({'socket'}) do
  describe('#connect over #'..socket_lib, function()
           
    it('check client parameters', function()
      assert.has.errors(function() odb.new('localhost', 2424, 'not_a_lib') end)
      assert.has.errors(function() odb.new('localhost', 'not a port', socket_lib) end)
      assert.has.errors(function() odb.new(nil, 2424, socket_lib) end)
    end)
    
    it('create client', function()
      assert.has_no.errors(function() odb.new('localhost', 2424, socket_lib) end)
    end)
    
    it('check credentials', function()
      local client = odb.new('localhost', 2424, socket_lib)
      assert.has.errors(function()
        return client:connect('no_pass')
      end)
      assert.has.errors(function()
        return client:connect('bad', 'creds')
      end)
    end)
    
    it('connect', function()
      local client = odb.new('localhost', 2424, socket_lib)
      assert.has_no.errors(function()
        client:connect(USER, PASSWORD)
      end)
    end)    
  end)
  
  describe('#operations on #'..socket_lib, function()
    local client = odb.new('localhost', 2424, socket_lib)
    local db = nil
    client:connect(USER, PASSWORD)
    
    it('list databases', function()
      local dbs = assert.has_no.errors(function() return client:db_list() end)
      assert.same(dbs, {'animals'})
    end)
    
    it('open graph database', function()
      db = assert.has_no.errors(function()
        return client:db_open('animals', 'graph', USER, PASSWORD)
      end)
      assert.truthy(db)
    end)
    
    pending('open document database')
    
    it('database #info', function()
      assert.truthy(function() return db:size() end)
      assert.truthy(function() return db:count_records() end)
    end)
    
    it('close db', function()
      assert.has_no.errors(function() db:close() end)
    end)
    
    it('create document database in memory', function()
      assert.has_no.errors(function() client:db_create('doc_db_mem', odb.DB.DOCUMENT, odb.STORAGE.MEMORY) end)
      assert.truthy(client:db_exists('doc_db_mem'))
    end)
    
    it('create document database on disk', function()
      assert.has_no.errors(function() client:db_create('doc_db_phys', odb.DB.DOCUMENT, odb.STORAGE.PHYSICAL) end)
      assert.truthy(client:db_exists('doc_db_phys'))
    end)
      
    it('create graph database in memory', function()
      assert.has_no.errors(function() client:db_create('graph_db_mem', odb.DB.GRAPH, odb.STORAGE.MEMORY) end)
      assert.truthy(client:db_exists('graph_db_mem'))
    end)
      
    it('create graph database on disk', function()
      assert.has_no.errors(function() client:db_create('graph_db_phys', odb.DB.GRAPH, odb.STORAGE.PHYSICAL) end)
      assert.truthy(client:db_exists('graph_db_phys'))
    end)
    
    it('drop databases', function()
      for _, db_name in pairs({'doc_db_mem', 'doc_db_phys', 'graph_db_mem', 'graph_db_phys'}) do
        assert.has_no.errors(function() client:db_drop(db_name) end)
        assert.falsy(client:db_exists(db_name))
      end
    end)
    
    pending('document db query')
    pending('document db command')
    pending('graph db query')
    pending('graph db command')
    
    client:disconnect()
  end)
end
