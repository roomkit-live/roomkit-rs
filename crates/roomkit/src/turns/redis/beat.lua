local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local beat = cjson.decode(ARGV[2])
local generation = tonumber(redis.call('GET', KEYS[6]) or '0')
if beat.generation < generation then
  return now
end
redis.call('SET', KEYS[6], beat.generation, 'PX', ARGV[1])
for member, declared in pairs(beat.declared) do
  redis.call('ZADD', KEYS[1], now, member)
  redis.call('HSET', KEYS[2], member, declared)
end
for _, member in ipairs(beat.undeclared) do
  redis.call('ZADD', KEYS[1], now, member)
  redis.call('HDEL', KEYS[2], member)
end
for _, member in ipairs(beat.gone) do
  redis.call('ZREM', KEYS[1], member)
  redis.call('HDEL', KEYS[2], member)
end
if beat.owner then
  redis.call('ZADD', KEYS[3], now, beat.node)
end
redis.call('DEL', KEYS[5])
for _, turn in ipairs(beat.running) do
  redis.call('SADD', KEYS[5], turn)
end
redis.call('PEXPIRE', KEYS[5], ARGV[1])
redis.call('PEXPIRE', KEYS[4], ARGV[1])
return now
