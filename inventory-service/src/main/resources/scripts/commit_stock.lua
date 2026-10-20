-- commit_stock.lua
-- Idempotent stock commit script
-- KEYS[1]: reservation key (flashsale:res:{saleId}:{reservationId})
-- KEYS[2]: optional expirations zset key (flashsale:res_expirations:{saleId})
-- ARGV[1]: optional reservationId (string)

if redis.call('EXISTS', KEYS[1]) == 0 then
    if KEYS[2] and KEYS[2] ~= '' and ARGV[1] then
        redis.call('ZREM', KEYS[2], ARGV[1])
    end
    return 0 -- Reservation not found
end

local status = redis.call('HGET', KEYS[1], 'status')
if status == 'COMMITTED' then
    if KEYS[2] and KEYS[2] ~= '' and ARGV[1] then
        local qty = redis.call('HGET', KEYS[1], 'quantity') or '0'
        redis.call('ZREM', KEYS[2], ARGV[1])
        redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. qty)
    end
    return 1 -- Idempotent success
end

if status ~= 'ACTIVE' then
    return 0 -- Cannot commit released or expired reservation
end

local qty = redis.call('HGET', KEYS[1], 'quantity') or '0'
redis.call('HSET', KEYS[1], 'status', 'COMMITTED')
redis.call('EXPIRE', KEYS[1], 86400) -- Keep 24 hours for audit/reconciliation

if KEYS[2] and KEYS[2] ~= '' and ARGV[1] then
    redis.call('ZREM', KEYS[2], ARGV[1])
    redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. qty)
end

return 1
