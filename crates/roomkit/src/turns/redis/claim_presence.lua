-- ARGV[1] holds the new state or an empty string to forget it on detach.
if ARGV[1] == '' then
  redis.call('DEL', KEYS[1])
  return 0
end
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 1
