-- reserve_stock.lua
-- Atomic stock reservation script
-- KEYS[1]: stock key (flashsale:stock:{saleId})
-- KEYS[2]: reservation key (flashsale:res:{saleId}:{reservationId})
-- KEYS[3]: sale key (flashsale:sale:{saleId})
-- KEYS[4]: optional expirations zset key (flashsale:res_expirations:{saleId})
-- KEYS[5]: optional idempotency key (flashsale:res_idempotency:{saleId}:{idempotencyKey})
-- ARGV[1]: requested quantity (integer)
-- ARGV[2]: TTL in seconds (integer)
-- ARGV[3]: shopperId (string)
-- ARGV[4]: expiresAt (ISO string)
-- ARGV[5]: optional expiresAt epoch millis (integer)
-- ARGV[6]: optional reservationId (string)

-- 1. Check idempotency if idempotency key passed
if KEYS[5] and KEYS[5] ~= '' and redis.call('EXISTS', KEYS[5]) == 1 then
    return 1 -- Idempotent success: reservation already recorded
end

-- 2. Check sale status if sale key exists
if redis.call('EXISTS', KEYS[3]) == 1 then
    local status = redis.call('HGET', KEYS[3], 'status')
    if status ~= 'OPEN' then
        return -2 -- SALE_NOT_OPEN
    end
end

-- 3. Read available stock
local currentStock = redis.call('GET', KEYS[1])
if not currentStock then
    return -1 -- STOCK_KEY_NOT_FOUND
end

local stockNum = tonumber(currentStock)
local requestedQty = tonumber(ARGV[1])

if stockNum < requestedQty then
    return 0 -- SOLD_OUT / INSUFFICIENT_STOCK
end

-- 4. Atomic decrement
redis.call('DECRBY', KEYS[1], requestedQty)

-- 5. Store active reservation hash
redis.call('HSET', KEYS[2], 
    'shopperId', ARGV[3],
    'quantity', requestedQty,
    'status', 'ACTIVE',
    'expiresAt', ARGV[4]
)
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))

-- 6. Add to expiration ZSET for active lease crash recovery
if KEYS[4] and KEYS[4] ~= '' and ARGV[5] and ARGV[6] then
    local zmember = ARGV[6] .. ':' .. requestedQty
    redis.call('ZADD', KEYS[4], tonumber(ARGV[5]), zmember)
end

-- 7. Record idempotency key if provided
if KEYS[5] and KEYS[5] ~= '' and ARGV[6] then
    redis.call('SET', KEYS[5], ARGV[6], 'EX', tonumber(ARGV[2]))
end

return 1 -- SUCCESS
