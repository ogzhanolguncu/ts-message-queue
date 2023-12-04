--[[
key 1 -> [prefix]:name:id (job ID counter)
key 2 -> [prefix]:name:jobs
key 3 -> [prefix]:name:waiting
arg 1 -> job id
arg 2 -> job data
]]


local jobId = ARGV[1]
local payload = ARGV[2]

if redis.call("hexists", KEYS[1], jobId) == 1 then return nil end
redis.call("hset", KEYS[1], jobId, payload)
redis.call("lpush", KEYS[2], jobId)

return jobId
