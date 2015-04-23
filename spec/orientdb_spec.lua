odb = require 'orientdb'

for _, socket_lib in pairs({'socket'}) do
  describe('connect over #'..socket_lib, function()
           
    it('check client parameters', function()
      assert.has.errors(function() odb.new('localhost', 2424, 'not_a_lib') end)
      assert.has.errors(function() odb.new('localhost', 'not a port', socket_lib) end)
      assert.has.errors(function() odb.new(nil, 2424, socket_lib) end)
    end)
    
    it('create client', function()
      assert.has_no.errors(function() odb.new('localhost', 2424, socket_lib) end)
    end)
    
    it('connect', function()
      assert.truthy(function()
        local client = odb.new('localhost', 2424, socket_lib)
        return client:connect('root', 'root')
      end)
    end)    
  end)
  
  describe('graph over #'..socket_lib, function()
    local client = odb.new('localhost', 2424, socket_lib)
    local db = nil
    client:connect('root', 'root')
    
    it('list databases', function()
      local dbs = assert.has_no.errors(function() return client:db_list() end)
      assert.same(dbs, {'animals'})
    end)
    
    it('open graph database', function()
      db = assert.has_no.errors(function()
        return client:db_open('animals', 'graph', 'root', 'root')
      end)
      assert.truthy(db)
    end)
    
    pending('open document database')
    
    it('database info', function()
      assert.truthy(function() return db:size() end)
      assert.truthy(function() return db:count_records() end)
    end)
    
    it('close db', function()
      assert.has_no.errors(function() db:close() end)
    end)
    
    it('create document database in memory', function()
      assert.has_no.errors(function() client:db_create('doc_db_mem', odb.DOCUMENT, odb.MEMORY) end)
      assert.truthy(client:db_exists('doc_db_mem'))
    end)
    
    it('create document database on disk', function()
      assert.has_no.errors(function() client:db_create('doc_db_phys', odb.DOCUMENT, odb.PHYSICAL) end)
      assert.truthy(client:db_exists('doc_db_phys'))
    end)
      
    it('create graph database in memory', function()
      assert.has_no.errors(function() client:db_create('graph_db_mem', odb.GRAPH, odb.MEMORY) end)
      assert.truthy(client:db_exists('graph_db_mem'))
    end)
      
    it('create graph database on disk', function()
      assert.has_no.errors(function() client:db_create('graph_db_phys', odb.GRAPH, odb.PHYSICAL) end)
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
