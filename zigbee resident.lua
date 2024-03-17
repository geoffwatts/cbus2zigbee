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

-- Set MQTT LWT
client:will_set('cbus2zigbee/status', 'offline', 2, true)

log("created MQTT client", client)

-- C-Bus events to MQTT local listener
server = require('socket').udp()
server:settimeout(1)
server:setsockname('127.0.0.1', zPort)

client.ON_CONNECT = function()
  log("MQTT connected - ready to send commands to zigbee2mqtt")
  client:publish('cbus2zigbee/status', 'online', 2, true)
end

client.ON_DISCONNECT = function()
  log("MQTT disconnected - attempting recovery and client reconnect")
  -- Handle recovery and client reconnect here
end

client:login_set(mqtt_username, mqtt_password)

while true do
  -- Attempt to connect to MQTT broker
  connected, err = client:connect(mqtt_broker)
  if not connected then
    log("Failed to connect to MQTT broker: " .. err)
    -- Handle failed connection attempt here, wait and retry
    socket.sleep(5) -- Wait for 5 seconds before retrying
    goto continue_loop
  end

  -- Main loop: Receive commands from C-Bus and publish to MQTT
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