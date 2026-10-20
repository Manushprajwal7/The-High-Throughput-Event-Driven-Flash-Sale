-- release_stock.lua
-- Idempotent stock release script
-- KEYS[1]: stock key (flashsale:stock:{saleId})
-- KEYS[2]: reservation key (flashsale:res:{saleId}:{reservationId})
-- KEYS[3]: optional expirations zset key (flashsale:res_expirations:{saleId})
-- ARGV[1]: release reason (string)
-- ARGV[2]: optional reservationId (string)

-- 1. Check reservation exists
if redis.call('EXISTS', KEYS[2]) == 0 then
    if KEYS[3] and KEYS[3] ~= '' and ARGV[2] then
        redis.call('ZREM', KEYS[3], ARGV[2])
    end
    return 0 -- Reservation does not exist or already cleaned up
end

-- 2. Verify status is ACTIVE
local status = redis.call('HGET', KEYS[2], 'status')
if status ~= 'ACTIVE' then
    if KEYS[3] and KEYS[3] ~= '' and ARGV[2] then
        local qty = redis.call('HGET', KEYS[2], 'quantity') or '0'
        redis.call('ZREM', KEYS[3], ARGV[2])
        redis.call('ZREM', KEYS[3], ARGV[2] .. ':' .. qty)
    end
    return 0 -- Already RELEASED, COMMITTED, or EXPIRED. Do not double-restore stock!
end

-- 3. Read reserved quantity
local qty = tonumber(redis.call('HGET', KEYS[2], 'quantity'))
if not qty or qty <= 0 then
    return 0
end

-- 4. Mark reservation as RELEASED and retain for 300s deduplication tombstone
redis.call('HSET', KEYS[2], 'status', 'RELEASED', 'releaseReason', ARGV[1])
redis.call('EXPIRE', KEYS[2], 300)

-- 5. Restore stock
redis.call('INCRBY', KEYS[1], qty)

-- 6. Clean up from expiration ZSET
if KEYS[3] and KEYS[3] ~= '' and ARGV[2] then
    redis.call('ZREM', KEYS[3], ARGV[2])
    redis.call('ZREM', KEYS[3], ARGV[2] .. ':' .. tostring(qty))
end

return 1 -- SUCCESS: Stock restored
