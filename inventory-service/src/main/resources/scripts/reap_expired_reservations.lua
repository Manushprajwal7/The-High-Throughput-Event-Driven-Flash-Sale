-- reap_expired_reservations.lua
-- Atomically reaps expired active reservations from the ZSET and restores stock
-- KEYS[1]: stock key (flashsale:stock:{saleId})
-- KEYS[2]: expirations zset key (flashsale:res_expirations:{saleId})
-- ARGV[1]: nowMillis (integer)
-- ARGV[2]: saleId (string)
-- ARGV[3]: limit (integer)

local nowMillis = tonumber(ARGV[1])
local saleId = ARGV[2]
local limit = tonumber(ARGV[3]) or 100

-- 1. Find members whose expiration timestamp <= nowMillis
local expiredMembers = redis.call('ZRANGEBYSCORE', KEYS[2], 0, nowMillis, 'LIMIT', 0, limit)
if not expiredMembers or #expiredMembers == 0 then
    return 0
end

local reapedCount = 0
local totalRestoredQty = 0

for _, member in ipairs(expiredMembers) do
    local sepIdx = string.find(member, ':')
    local resId = member
    local qty = 0
    if sepIdx then
        resId = string.sub(member, 1, sepIdx - 1)
        qty = tonumber(string.sub(member, sepIdx + 1)) or 0
    end

    local resKey = "flashsale:res:" .. saleId .. ":" .. resId
    local status = redis.call('HGET', resKey, 'status')

    if status == 'COMMITTED' then
        -- Already committed, remove from ZSET
        redis.call('ZREM', KEYS[2], member)
    elseif status == 'RELEASED' then
        -- Already released, remove from ZSET
        redis.call('ZREM', KEYS[2], member)
    else
        -- ACTIVE or orphaned reservation (hash evicted because process crashed before DB save)
        if qty <= 0 and redis.call('EXISTS', resKey) == 1 then
            qty = tonumber(redis.call('HGET', resKey, 'quantity')) or 0
        end

        if qty > 0 then
            totalRestoredQty = totalRestoredQty + qty
            reapedCount = reapedCount + 1
        end

        if redis.call('EXISTS', resKey) == 1 then
            redis.call('HSET', resKey, 'status', 'EXPIRED', 'releaseReason', 'REAPED_TTL')
            redis.call('EXPIRE', resKey, 300)
        end
        redis.call('ZREM', KEYS[2], member)
    end
end

-- Restore total reaped stock atomically
if totalRestoredQty > 0 then
    redis.call('INCRBY', KEYS[1], totalRestoredQty)
end

return reapedCount
