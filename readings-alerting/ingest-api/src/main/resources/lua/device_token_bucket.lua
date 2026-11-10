-- Per-device token bucket. KEYS[1] = rate:{deviceId}; ARGV[1] = burst (bucket size), ARGV[2] = refill per minute.
-- Hash {t: tokens left, ts: last refill in ms}. Uses the Redis clock so every ingest pod agrees on time.
-- Returns {1, 0} when a token was taken, {0, ms until the next token} when the device is over its rate.
local burst = tonumber(ARGV[1])
local per_ms = tonumber(ARGV[2]) / 60000
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)

local state = redis.call('HMGET', KEYS[1], 't', 'ts')
local tokens = tonumber(state[1]) or burst
local ts = tonumber(state[2]) or now
tokens = math.min(burst, tokens + math.max(0, now - ts) * per_ms)

local allowed, wait = 0, 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  wait = math.ceil((1 - tokens) / per_ms)
end

redis.call('HSET', KEYS[1], 't', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / per_ms))  -- a full bucket needs no key
return {allowed, wait}
