--[[
Event-based, execute during ramping, name: "ZIGBEE"

Pushes events to resident script via socket.

Tag required objects with the "ZIGBEE" keyword and this script will run whenever one of those objects change.

Add a "z=<zigbee address from zigbee2mqtt" to the keyword as well, or populate "Local Addresses" below
--]]


logging = false
zPort = 0xBEEF1

-- Define the zigbee address for each group we want to send

local addresses = 
  {
   ["0/56/200"]="0xa4c1389bf2e3ae50",
  }


server = require('socket').udp()

val = event.getvalue()
parts = string.split(event.dst, '/')
net = tonumber(parts[1]); app = tonumber(parts[2]); group = tonumber(parts[3])
ramp = GetCBusRampRate(net, app, group)


-- Do a database lookup, with no error handling, to find our z= tag
local tags = db:getall("SELECT ot.tag FROM objects AS o JOIN objecttags AS ot ON o.id=ot.object WHERE o.id="..event.dstraw.." AND ot.tag like 'z=%'")
local zigbee_address = string.split(tags[1]["tag"],"=")[2]

-- Try our local LUT if that didn't work out
if zigbee_address == nil then
	local zigbee_address = addresses[event.dst]
end

if zigbee_address == nil then
  log("Keyword triggered but dont have a zigbee address for "..event.dst.." - please update the table in the ZIGBEE script or add a z= tag to the group")
  do return end
end
  
-- Send an event to zigbee2mqtt
local output = zigbee_address.."/"..val.."/"..ramp
if logging then log('Sending '..output) end

server:sendto(output, '127.0.0.1', zPort)

