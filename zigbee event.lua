--[[
Event-based, execute during ramping, name: "ZIGBEE"

Pushes events to resident script via socket.

Tag required objects with the "ZIGBEE" keyword and this script will run whenever one of those objects changes.

Add a "z=<zigbee address from zigbee2mqtt" to the keyword as well
--]]

local logging = true
local zPort = 0xBEEF1
local parts = string.split(event.dst, '/') local net = tonumber(parts[1]) local app = tonumber(parts[2]) local group = tonumber(parts[3])
if app ~= 228 and app ~= 250 then
  ramp = GetCBusRampRate(net, app, group)
else
  ramp = 0
end

-- Send an event to zigbee2mqtt
local output = event.dst.."/"..event.getvalue().."/"..ramp
if logging then log('Sending '..output) end
require('socket').udp():sendto(output, '127.0.0.1', zPort)