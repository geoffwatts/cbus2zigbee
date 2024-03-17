--[[
Resident script

Actually does the talking to zigbee2mqtt via MQTT.

Update your MQTT credentials below:
--]]

mqtt_broker = '192.168.1.1'
mqtt_username = ''
mqtt_password = ''
mqtt_clientid = 'cbuszigbee'

zPort = 0xBEEF1

logging = true

mqtt_topic = "zigbee2mqtt"

-- load mqtt module
mqtt = require("mosquitto")

-- create new mqtt client
client = mqtt.new(mqtt_clientid)

client:will_set('cbus2zigbee/status', 'offline', 2, true)
client:login_set(mqtt_username, mqtt_password)

client.ON_CONNECT = function()
  log("MQTT connected - ready to send commands to zigbee2mqtt")
  client:publish('cbus2zigbee/status', 'online', 2, true)
end

client.ON_DISCONNECT = function()
  log("MQTT disconnected - attempting recovery and client reconnect")
end

client:connect(mqtt_broker)
client:loop_start()

-- C-Bus events to MQTT local listener
server = require('socket').udp()
server:settimeout(1)
server:setsockname('127.0.0.1', zPort)

-- Main loop: Connect to MQTT broker and process commands
while true do

  -- Receive commands from C-Bus and publish to MQTT
    cmd = server:receive()
    if cmd then
        parts = string.split(cmd, "/")
        zigbee_address = parts[1]
        level = tonumber(parts[2])
        ramp = tonumber(parts[3]) -- Ignoring ramp for now
        state = (level ~= 0) and "ON" or "OFF"
        client:publish(mqtt_topic .. "/" .. zigbee_address .. "/set", '{"state":"'..state..
            '","brightness":"'..level..'"}', 2, true) -- QoS 2 for send exactly once
    end

    ::continue_loop:: -- Continue loop label
end