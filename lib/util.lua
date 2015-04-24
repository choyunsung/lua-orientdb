local _M = {}

if os.getenv('DEBUG') then
  function _M.debug(...)
    io.stderr:write(...)
  end
else
  function _M.debug(...) end
end

return _M