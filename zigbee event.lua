--[[
Event-based, execute during ramping, name: "ZIGBEE"

Pushes events to resident script via socket.

Tag required objects with the "ZIGBEE" keyword and this script will run whenever one of those objects changes.

Add a "z=<zigbee address from zigbee2mqtt" to the keyword as well, or populate "Local Addresses" below
--]]

logging = true
zPort = 0xBEEF1

-- Define the Zigbee address for each group we want to send
local addresses = {
    ["0/56/200"] = "0xa4c1389bf2e3ae50",
}

server = require('socket').udp()

-- Get Zigbee address for the group from local addresses or tags
function getZigbeeAddress(group)
    local local_address = addresses[group]
    if local_address then
        log("Using local address for group "..group..": "..local_address)
        return local_address
    end

    local tags = db:getall("SELECT ot.tag FROM objects AS o JOIN objecttags AS ot ON o.id=ot.object WHERE o.id="..group.." AND ot.tag like 'z=%'")
    if tags and #tags > 0 then
        local zigbee_address = string.match(tags[1]["tag"], 'z=(%x+)')
        if zigbee_address then
            log("Zigbee address found for group "..group..": "..zigbee_address)
            return zigbee_address
        end
    end
    
    log("No Zigbee address found for group "..group)
    return nil
end

-- Main event processing
val = event.getvalue()
parts = string.split(event.dst, '/')
net = tonumber(parts[1])
app = tonumber(parts[2])
group = tonumber(parts[3])
ramp = GetCBusRampRate(net, app, group)

zigbee_address = getZigbeeAddress(event.dstraw)

if not zigbee_address then
    log("Keyword triggered but don't have a Zigbee address for "..event.dst.." - please update the table in the ZIGBEE script or add a z= tag to the group")
    return
end
  
-- Send an event to zigbee2mqtt
local output = zigbee_address.."/"..val.."/"..ramp
if logging then 
    log('Sending '..output)
end

server:sendto(output, '127.0.0.1', zPort)