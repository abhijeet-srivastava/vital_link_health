-- Compare-and-set of the latest reading for one patient and reading type.
-- KEYS[1] latest:{patientId}:{TYPE}   ARGV[1] measured_at (epoch micros)   ARGV[2] reading JSON   ARGV[3] TTL seconds
-- Writes only when the incoming reading is strictly newer, so out-of-order arrivals never overwrite a newer value.
local current = redis.call('HGET', KEYS[1], 'm')
if current and tonumber(current) >= tonumber(ARGV[1]) then
  return 0
end
redis.call('HSET', KEYS[1], 'm', ARGV[1], 'v', ARGV[2])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
