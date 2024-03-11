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

log("created MQTT client", client)

-- C-Bus events to MQTT local listener
server = require('socket').udp()
server:settimeout(1)
server:setsockname('127.0.0.1', zPort)

client.ON_CONNECT = function()
  log("MQTT connected - ready to send commands to zigbee2mqtt")
end

client:login_set(mqtt_username, mqtt_password)
client:connect(mqtt_broker)
client:loop_start()

while true do
	cmd = server:receive()
	if cmd then
    parts = string.split(cmd, "/")
    zigbee_address = parts[1]
    level = tonumber(parts[2])
    ramp = tonumber(parts[3]) -- We will ignore this, because cbus repeatedly calls this during ramps - maybe we figure out transitions later
  	state = (level ~= 0) and "ON" or "OFF"
    client:publish(mqtt_topic .. "/" .. zigbee_address .. "/set", '{"state":"'..state..
      '","brightness":"'..level..'"}', 1, true)   -- ,"transition":"'..ramp..'"
	end
end
