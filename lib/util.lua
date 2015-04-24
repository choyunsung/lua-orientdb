local _M = {}

if os.getenv('DEBUG') then
  function _M.debug(...)
    io.stderr:write('DEBUG\t', ...)
    io.stderr:write('\n')
  end
else
  function _M.debug(...) end
end

return _M