--[[
Resident script

Actually does the talking to zigbee2mqtt via MQTT.

Update your MQTT credentials below:
--]]

mqtt_broker = '192.168.1.1'
mqtt_username = ''
mqtt_password = ''
mqtt_clientid = 'cbus2zigbee'
eventName = 'zigbee' -- The name of the ZigBee event script
local checkChanges = 5 -- Interval in seconds to check for changes to object keywords (set to nil to disable change checks, recommended once configuration is stable)

local logging = true

local socketTimeout = 0.1
local mqttTimeout = 0

local zPort = 0xBEEF1
local mqttTopic = "zigbee2mqtt/"
local mqttStatus = 2
local QoS = 2 -- send exactly once
local zigbee = {}
local cbusMessages = {} -- message queue
local mqttMessages = {} -- message queue

local cudRaw = { -- All possible keywords for ZIGBEE objects, used in CUD function to exclude unrelated keywords for change detection
  'ZIGBEE', 'z=',
}
local cudAll = {} local param for _, param in ipairs(cudRaw) do cudAll[param] = true end cudRaw = nil

local function removeIrrelevant(keywords)
  local curr = {}
  for _, k in ipairs(keywords:split(',')) do
    local parts = k:split('=') if parts[2] ~= nil then parts[1] = parts[1]:trim()..'=' end
    if cudAll[parts[1]] then table.insert(curr, k) end
  end
  return table.concat(curr, ',')
end


--[[
Check for presence of the event script
--]]
local eventScripts = db:getall("SELECT name FROM scripting WHERE type = 'event'")
found = false
for _, s in ipairs(eventScripts) do
  if s.name:lower() == eventName:lower() then found = true eventName = s.name end
end
if not found then
  log('Error: Event-based script \''..eventName..'\' not found. Halting')
  while true do socket.select(nil, nil, 1) end
end


--[[
Create new mqtt client and callbacks
--]]
local client = require("mosquitto").new(mqttClientId)
if mqttUsername and mqttUsername ~= '' then client:login_set(mqttUsername, mqttPassword) end

client.ON_CONNECT = function(success)
  if success then
    log('Connected to Mosquitto broker')
    mqttStatus = 1
  end
end

client.ON_DISCONNECT = function()
  log("Mosquitto broker disconnected - attempting recovery and client reconnect")
end

client.ON_MESSAGE = function(mid, topic, payload)
  mqttMessages[#mqttMessages + 1] = { topic=topic, payload=payload } -- Queue the message
end


--[[
C-Bus events to MQTT local listener
--]]
pcall(function () server:close() end) -- force close the socket on re-entry due to script error
server = require('socket').udp()
server:settimeout(socketTimeout)
server:setsockname('127.0.0.1', zPort)


--[[
Get applicable groups and keywords
--]]
local function getGroups(kw)
  local t
  local grps = {}
  for _, t in ipairs(db:getall("SELECT o.address, o.tagcache, o.name FROM objects AS o JOIN objecttags AS ot ON o.id=ot.object WHERE ot.tag='"..kw.."'")) do
    local alias = knxlib.decodega(t.address)
    local parts = load("return {"..alias:gsub('/',',').."}")()
    grps[alias] = { tags = {}, name = t.name, keywords = t.tagcache:gsub(', ',','), net = parts[1], app = parts[2], group = parts[3], channel = parts[4], }
    if parts[2] == 228 then -- Get the measurement app channel name from the CBus tag map
      local resp = db:getall('SELECT tag FROM cbus_tag_map WHERE tagtype="S" AND net='..parts[1]..' AND app='..parts[2]..' AND grp='..parts[3]..' AND tagid='..parts[4])
      if resp ~= nil and resp[1] ~= nil and resp[1]['tag'] ~= nil then grps[alias].name = resp[1]['tag'] else grps[alias].name = alias end
    end
    local tags = {}
    local tg
    for _, tg in ipairs(grps[alias].keywords:split(',')) do
      parts = tg:split('=')
      if parts[2] then tags[parts[1]:trim()] = parts[2]:trim() else tags[parts[1]:trim()] = -1 end
    end
    grps[alias].tags = tags
  end
  return grps
end


--[[
Get key/value pairs. Returns a keyword if found in 'allow'. (allow, synonym and special parameters are optional).
--]]
local function getKeyValue(alias, tags, _L, synonym, special, allow)
  if synonym == nil then synonym = {} end
  if special == nil then special = {} end
  if allow == nil then allow = {} end
  local dType = nil
  for k, t in pairs(tags) do
    k = k:trim()
    if t ~= -1 then
      if special[k] ~= nil then special[k] = true end
      local v = t:trim()
      if _L[k] then
        if type(_L[k]) == 'number' then _L[k] = tonumber(v) if _L[k] == nil then error('Error: Bad numeric value for '..alias..', keyword "'..k..'="') end
        elseif type(_L[k]) == 'table' then
          _L[k] = string.split(v, '/')
          local i, tv for i, tv in ipairs(_L[k]) do _L[k][i] = tv:trim() end
        else _L[k] = v end
      end
    else
      if synonym[k] then k = synonym[k] end
      if special[k] ~= nil then special[k] = true end
      if allow[k] then
        if dType == nil then dType = k else error('Error: More than one "type" keyword used for '..alias) end
      end
    end
  end
  return dType
end


--[[
Create / update / delete ZIGBEE devices
--]]
local function cudZig()
  local grps = getGroups('ZIGBEE')
  local found = {}
  local addition = false
  local modification = false
  local addCount = 0
  local modCount = 0
  local remCount = 0
  local alias, k, v

  for alias, v in pairs(grps) do
    found[alias] = true
    local curr = removeIrrelevant(v.keywords)

    if zigbee[alias] and zigbee[alias].keywords ~= curr then modification = true end
    if not zigbee[alias] or modification then
      local _L = {
        z = '',
      }
      getKeyValue(alias, v.tags, _L)
      if _L.z:find('^0[xX]%x*$') then
        if not modification then addition = true addCount = addCount + 1 else modCount = modCount + 1 end
        
        zigbee[alias] = {name=v.name, address=_L.z, net=v.net, app=v.app, group=v.group, keywords=curr, }
      else
        log('Error: Invalid or no z= hexadecimal address specified for ZigBee object '..alias)
        zigbee[alias] = {name=v.name, net=v.net, app=v.app, group=v.group, keywords=curr, } 
      end
    end
  end

  -- Handle deletions
  for k, v in pairs(zigbee) do
    if not found[k] then
      log('Removing '..k..' ZigBee '..v.name) zigbee[k] = nil remCount = remCount + 1
    end
  end
  if remCount > 0 then
    log('Removed '..remCount..' ZigBee object'..(remCount ~= 1 and 's' or '')..', event script \''..eventName..'\' restarted')
  end
  -- Handle additions/modifications
  if addition or modification then
    if addition then log('Added '..addCount..' ZigBee object'..(addCount ~= 1 and 's' or '')..(addCount > 0 and ', event script \''..eventName..'\' restarted' or '')) end
    if modification then log('Modified '..modCount..' ZigBee object'..(modCount ~= 1 and 's' or '')..(modCount > 0 and ', event script \''..eventName..'\' restarted' or '')) end
  end
  if addCount > 0 or modCount > 0 or remCount > 0 then
    script.disable(eventName) script.enable(eventName) -- Ensure that newly changed keyworded groups send updates
  end
end


--[[
Publish to MQTT
--]]
function publish(alias, level)
  local msg = {
    brightness = level,
    state = (level ~= 0) and "ON" or "OFF",
  }
  client:publish(mqttTopic..zigbee[alias].address..'/set', json.encode(msg), QoS, true)
end


--[[
Publish current level and state to MQTT
--]]
function publishCurrent()
  local alias, v
  for alias, v in pairs(zigbee) do
    publish(alias, grp.getvalue(alias))
  end
end


--[[
Receive commands from C-Bus and publish to MQTT
--]]
function outstandingCbusMessage()
  for _, cmd in ipairs(cbusMessages) do
    parts = string.split(cmd, "/")
    alias = parts[1]..'/'..parts[2]..'/'..parts[3]
    local level = tonumber(parts[4])
    local ramp = tonumber(parts[5]) -- Ignoring ramp for now
    publish(alias, level)
  end
  cbusMessages = {}
end


--[[
Send commands subscribed from MQTT to C-Bus
--]]
function outstandingMqttMessage()
  for _, cmd in ipairs(mqttMessages) do
    
  end
  mqttMessages = {}
end

-- Reconnect variables
local warningTimeout = 30
local timeout = 1
local init = true
local reconnect = false
local timeoutStart, connectStart, mqttConnected

local changesChecked = socket.gettime()

-- Main loop: process commands
while true do
  -- Check for new messages from CBus. The entire socket buffer is collected each iteration for efficiency.
  local stat, err
  local more = false
  stat, err = pcall(function ()
    ::checkAgain::
    local cmd = nil
	  cmd = server:receive()
    if cmd and type(cmd) == 'string' then
      cbusMessages[#cbusMessages + 1] = cmd -- Queue the new message
      server:settimeout(0); more = true; goto checkAgain -- Immediately check for more buffered inbound messages to queue
    else
      if more then server:settimeout(socketTimeout) end
    end
  end)
  if not stat then log('Socket receive error: '..err) end

  if mqttStatus == 1 then
    -- Process MQTT message buffers synchronously - sends and receives
    client:loop(mqttTimeout)

    if #cbusMessages > 0 then
      -- Send outstanding messages to MQTT
      stat, err = pcall(outstandingCbusMessage)
      if not stat then log('Error processing outstanding CBus messages: '..err) cbusMessages = {} end -- Log error and clear the queue, continue
    end

    if #mqttMessages > 0 then
      -- Send outstanding messages to CBus
      stat, err = pcall(outstandingMqttMessage)
      if not stat then log('Error processing outstanding MQTT messages: '..err) mqttMessages = {} end -- Log error and clear the queue
    end
  elseif mqttStatus == 2 or not mqttStatus then
    -- MQTT is disconnected, so attempt a connection, waiting. If fail to connect then retry.
    if init then
      log('Connecting to Mosquitto broker')
      timeoutStart = socket.gettime()
      connectStart = timeoutStart
      init = false
    end
    stat, err = pcall(function (b, p, k) client:connect(b, p, k) end, mqttBroker, 1883, 25) -- Requested keep-alive 25 seconds, broker at port 1883
    if not stat then -- Log and abort
      log('Error calling connect to broker: '..err)
      pcall(function () server:close() end)
      do return end
    end
    while mqttStatus ~= 1 do
      client:loop(1) -- Service the client with a generous timeout
      if socket.gettime() - connectStart > timeout then
        if socket.gettime() - timeoutStart > warningTimeout then
          log('Failed to connect to the Mosquitto broker, retrying continuously')
          timeoutStart = socket.gettime()
        end
        connectStart = socket.gettime()
        goto next -- Exit to the main loop to keep socket messages monitored
      end
    end
    mqttConnected = socket.gettime()
    -- Subscribe to relevant topics
    client:subscribe(mqttTopic..'#', mqttQoS)
    -- Connected... Now loop briefly to allow retained value retrieval for subscribed topics because synchronous
    while socket.gettime() - mqttConnected < 0.5 do client:loop(0) end
    if not reconnect then -- Full publish topics
      stat, err = pcall(cudZig) if not stat then log(err) log('Halting') while true do socket.select(nil, nil, 1) end end
      stat, err = pcall(publishCurrent) if not stat then log('Error publishing current values: '..err) end -- Log and continue
    end
  else
    log('Error: Invalid mqttStatus: '..mqttStatus)
    pcall(function () server:close() end)
    do return end
  end

  local t = socket.gettime()
  if checkChanges and t > changesChecked then
    changesChecked = t
    stat, err = pcall(cudZig) if not stat then log(err) log('Halting') while true do socket.select(nil, nil, 1) end end
  end

  ::next::
end