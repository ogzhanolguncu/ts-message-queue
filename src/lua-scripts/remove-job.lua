--[[
key 1 -> [prefix]:test:succeeded
key 2 -> [prefix]:test:failed
key 3 -> [prefix]:test:waiting
key 4 -> [prefix]:test:active
key 5 -> [prefix]:test:jobs
arg 1 -> jobId
]]

local jobId = ARGV[1]

if (redis.call("sismember", KEYS[1], jobId) + redis.call("sismember", KEYS[2], jobId)) == 0 then
  redis.call("lrem", KEYS[3], 0, jobId)
  redis.call("lrem", KEYS[4], 0, jobId)
end

redis.call("srem", KEYS[1], jobId)
redis.call("srem", KEYS[2], jobId)
redis.call("hdel", KEYS[5], jobId)
