local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local oldest = now - tonumber(ARGV[1])
for _, member in ipairs(redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', oldest)) do
  redis.call('ZREM', KEYS[1], member)
  redis.call('HDEL', KEYS[2], member)
end
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', oldest)
local view = {now = now, executors = {}, owners = {}}
local alive = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
for i = 1, #alive, 2 do
  local member = alive[i]
  local declared = redis.call('HGET', KEYS[2], member)
  view.executors[member] = {tonumber(alive[i + 1]), declared or false}
end
local owners = redis.call('ZRANGE', KEYS[3], 0, -1, 'WITHSCORES')
for i = 1, #owners, 2 do
  view.owners[owners[i]] = tonumber(owners[i + 1])
end
return cjson.encode(view)
