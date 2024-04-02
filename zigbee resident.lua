--[[
C-Bus integration for Zigbee2MQTT.

Update your Mosquitto broker address and credentials below.

Required keywords for automation controller objects are described in the readme at https://github.com/geoffwatts/cbus2zigbee.

Copyright (c) 2024, Geoff Watts. Subject to BSD 3-Clause License.
--]]

mqttBroker = '192.168.1.1'
mqttUsername = ''
mqttPassword = ''
local checkChanges = 15 -- Interval in seconds to check for changes to object keywords (set to nil to disable change checks, recommended once configuration is stable)
local lighting = { ['56'] = true, } -- Array of applications that are used for lighting
local keepMessagesForOfflineQueued = true -- When a Zigbee device is offline, queue outstanding C-Bus to zigbee messages

local logging = true

local busTimeout = 0.1
local mqttTimeout = 0

local mqttTopic = 'zigbee2mqtt/'
local mqttClientId = 'cbus2zigbee'
local mqttStatus = 2        -- Initially disconnected from broker
local init = true           -- Broker initialise
local reconnect = false     -- Reconnecting to broker
local QoS = 2               -- Send exactly once
local subscribed = {}       -- Topics that have been subscribed to
local zigbee = {}           -- Key is C-Bus alias, contains { class, address, name, net, app, group, channel, keywords, exposed, datatype, value }
local zigbeeAddress = {}    -- Key is IEEE-address, contains { alias, net, app, group, channel }
local zigbeeDevices = {}    -- Key is IEEE-address, contains { class, friendly, max, exposesRaw, exposes, exposed={ {expose, type, alias, net, app, group, channel}, ... } }
local zigbeeName = {}       -- Key is friendly name, contains IEEE-address
local zigbeeGroups = {}     -- TO DO
local cbusMessages = {}     -- Message queue, inbound from C-Bus, contains an array of { alias, level, origin, ramp }
local mqttMessages = {}     -- Message queue, inbound from Mosquitto, contains an array of { topic, payload }
local ignoreMqtt = {}       -- When sending from C-Bus to MQTT any status update for C-Bus will be ignored, avoids message loops
local ignoreCbus = {}       -- When receiving from MQTT to C-Bus any status update for MQTT will be ignored, avoids message loops
local suppressMqttUpdates = {} -- Suppress status updates to C-Bus during transitions, key is alias, contains expected completion timestamp

local cbusMeasurementUnits = { temperature=0, humidity=0x1a, current=1, frequency=7, voltage=0x24, power=0x26, energy=0x25, }

local cudRaw = { -- All possible keywords for ZIGBEE objects. cudAll is used in create/update/delete function to exclude unrelated keywords for change detection
  'ZIGBEE', 'light', 'switch', 'sensor', 'name=', 'n=', 'addr=', 'z=', 'type=', 'exposed=', 'property=', 
}
local cudAll = {} local param for _, param in ipairs(cudRaw) do cudAll[param] = true end cudRaw = nil

-- Runtime global variable checking. Globals must be explicitly declared, which will catch variable name typos
local declaredNames = { vprint = true, vprinthex = true, maxgroup = true, mosquitto = true, rr = true, _ = true, }
local function declare(name, initval) rawset(_G, name, initval) declaredNames[name] = true end
local exclude = { ngx = true, }
setmetatable(_G, {
  __newindex = function (t, n, v) if not declaredNames[n] then log('Warning: Write to undeclared global variable "'..n..'"') end rawset(t, n, v) end,
  __index = function (_, n) if not exclude[n] and not declaredNames[n] then log('Warning: Read undeclared global variable "'..n..'"') end return nil end,
})

local function removeIrrelevant(keywords)
  local curr = {}
  for _, k in ipairs(keywords:split(',')) do
    local parts = k:split('=') if parts[2] ~= nil then parts[1] = parts[1]:trim()..'=' end
    if cudAll[parts[1]] then table.insert(curr, k) end
  end
  return table.concat(curr, ',')
end

local function hasMembers(tbl) for _, _ in pairs(tbl) do return true end return false end -- Get whether a table has any members


--[[
Create Mosquitto client and callbacks
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
  mqttStatus = 2
  init = true
  reconnect = true
end

client.ON_MESSAGE = function(mid, topic, payload)
  local p if payload:sub(1,1) == '{' or payload:sub(1,1) == '[' then p = json.decode(payload) else p = payload end -- If not JSON then pass the string (for legacy availability flag)
  mqttMessages[#mqttMessages + 1] = { topic=topic, payload=p } -- Queue the message
end


--[[
C-Bus events (queues transitions at the start of a ramp, with status queued also at the target level)
--]]
local function eventCallback(event)
  if not zigbee[event.dst] then return end
  -- local value = dpt.decode(event.datahex, zigbee[event.dst].datatype) -- This will return nil, as I don't think decode works for AC use
  local value
  local origin = zigbee[event.dst].value
  local ramp = 0
  local parts = string.split(event.dst, '/')
  if lighting[parts[2]] then
    value = tonumber(string.sub(event.datahex,1,2),16)
    local target = tonumber(string.sub(event.datahex,3,4),16)
    if event.meta == 'admin' then -- A ramp always begins with an admin message, so queue a transition
      ramp = tonumber(string.sub(event.datahex,5,8),16)
      if ramp > 0 then
        cbusMessages[#cbusMessages + 1] = { alias=event.dst, level=target, origin=origin, ramp=ramp, } -- Queue the event
        return
      end
    end
    if value ~= target then return end -- Ignore level changes during a ramp
  else
    value = grp.getvalue(event.dst)
  end
  if value == zigbee[event.dst].value then return end -- Don't publish if already at the level, avoids publishing twice when a ramp occurs
  zigbee[event.dst].value = value
  cbusMessages[#cbusMessages + 1] = { alias=event.dst, level=value, origin=origin, ramp=ramp, } -- Queue the event
  suppressMqttUpdates[event.dst] = nil
end

local localbus = require('localbus').new(busTimeout) -- Set up the localbus
localbus:sethandler('groupwrite', eventCallback)


--[[
Get applicable groups and keywords
--]]
local function getGroups(kw)
  local t
  local grps = {}
  for _, t in ipairs(db:getall("SELECT o.address, o.tagcache, o.name FROM objects AS o JOIN objecttags AS ot ON o.id=ot.object WHERE ot.tag='"..kw.."'")) do
    if t.tagcache == nil then goto skip end
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
    ::skip::
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
      if synonym[k] then k = synonym[k] end
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
  local addCount = 0
  local modCount = 0
  local remCount = 0
  local alias, k, v
  local synonym = { addr = 'z', name = 'n' } -- Synonyms for some keywords, allowing variations
  local special = {} -- special use keywords, e.g. { noavailqueue=true, }

  for alias, v in pairs(grps) do
    local modification = false
    found[alias] = true
    local curr = removeIrrelevant(v.keywords)
    if zigbee[alias] and zigbee[alias].keywords ~= curr then modification = true end
    if not zigbee[alias] or modification then
      zigbee[alias] = { name=v.name, net=v.net, app=v.app, group=v.group, channel=v.channel, keywords=curr, datatype=grp.find(alias).datatype } 
      local _L = {
        n = '',
        z = '',
        sensor = '',
        type = '',
        exposed = '', 
        property = 'property',
      }

      local function getExposes()
        local exposes = {}
        local function getProps(tbl)
          local val
          for _, val in pairs(tbl) do if type(val) == "table" then getProps(val) if val[_L.property] then exposes[val[_L.property]] = true end end end
        end
        getProps(zigbeeDevices[_L.z].exposesRaw)
        return exposes
      end

      local function setupExposed()
        if zigbeeDevices[_L.z].exposes == nil then
          if zigbeeDevices[_L.z].exposesRaw ~= nil and hasMembers(zigbeeDevices[_L.z].exposesRaw) then
            zigbeeDevices[_L.z].exposes = getExposes()
          else
            log('Error, device '..alias..', '.._L.z..' has nothing exposed')
            return false
          end
        end

        if zigbeeDevices[_L.z].exposes[_L.exposed] ~= nil then
          zigbee[alias].exposed = _L.exposed
        else
          log('Keyword error, device '..alias..', '.._L.z..' has no '.._L.exposed..' exposed')
          return false
        end
        return true
      end

      local allow = {
        light = {setup = function ()
            zigbee[alias].address = _L.z
            return true
          end
        },
        switch = {setup = function ()
            zigbee[alias].address = _L.z
            if setupExposed() then
              if not zigbeeDevices[_L.z].exposed then zigbeeDevices[_L.z].exposed = {} end
              local found = false for _, e in ipairs(zigbeeDevices[_L.z].exposed) do if e.expose == _L.exposed then found = true break end end
              if not found then zigbeeDevices[_L.z].exposed[#zigbeeDevices[_L.z].exposed + 1] = { expose=_L.exposed, type=_L.type, alias=alias, net=v.net, app=v.app, group=v.group, } end
              zigbee[alias].exposed = _L.exposed
            else
              return false
            end
            return true
          end
        },
        sensor = {setup = function ()
            zigbee[alias].address = _L.z
            if setupExposed() then
              if not zigbeeDevices[_L.z].exposed then zigbeeDevices[_L.z].exposed = {} end
              local found = false for _, e in ipairs(zigbeeDevices[_L.z].exposed) do if e.expose == _L.exposed then found = true break end end
              if not found then zigbeeDevices[_L.z].exposed[#zigbeeDevices[_L.z].exposed + 1] = { expose=_L.exposed, type=_L.type, alias=alias, net=v.net, app=v.app, group=v.group, channel=v.channel, } end
              zigbee[alias].exposed = _L.exposed
            else
              return false
            end
            return true
          end
        },
      }

      local dType = 'light'
      local dt = getKeyValue(alias, v.tags, _L, synonym, special, allow)
      if dt ~= nil then dType = dt end
      zigbee[alias].class = dType

      if _L.n ~= '' then
        _L.z = zigbeeName[_L.n]
        if _L.z == nil then log('Error: Zigbee device with friendly name of '.._L.n..' does not exist, skipping') _L.z = '' end
      end

      if not _L.z:find('^0[xX]%x*$') then
        log('Error: Invalid or no z= hexadecimal address specified for Zigbee object '..alias)
      else
        if zigbeeDevices[_L.z] ~= nil then
          if not allow[dType].setup() then goto next end
        else
          log('Error, device '..alias..', '.._L.z..' does not exist')
          goto next
        end

        log('Adding '..dType..' '..alias..' Zigbee '.._L.z..((zigbee[alias].exposed and ', exposed '..zigbee[alias].exposed) or ''))
        zigbeeDevices[_L.z].class = dType

        zigbee[alias].value = grp.getvalue(alias)
        local friendly = zigbeeDevices[_L.z].friendly
        if not subscribed[friendly] or dType == 'sensor' then
          if dType ~= 'sensor' then ignoreMqtt[alias] = true end
          client:subscribe(mqttTopic..friendly..'/#', QoS)
          subscribed[friendly] = true
          if logging then log('Subscribed '..mqttTopic..friendly..'/#') end
        end

      end
      ::next::
      zigbeeAddress[_L.z] = { alias=alias, net=v.net, app=v.app, group=v.group, channel=v.channel, }
    end
  end

  -- Handle deletions
  for k, v in pairs(zigbee) do
    if not found[k] then
      local exposed = v.exposed
      local address = v.address
      local unsub = false
      if exposed then
        local i, exp
        for i, exp in pairs(zigbeeDevices[address].exposed) do
          if exposed == exp.expose then
            log('Removing '..k..' Zigbee '..address..', exposed '..exposed)
            zigbeeDevices[address].exposed[i] = nil
            zigbee[k] = nil remCount = remCount + 1
          end
        end
        if #zigbeeDevices[address].exposed == 0 then -- If there is nothing more exposed for the device then unsubscribe and remove it
          unsub = true
        end
      else
        if address ~= nil then
          log('Removing '..k..' Zigbee '..address)
          zigbee[k] = nil remCount = remCount + 1
          unsub = true
        end
      end
      if unsub then
        zigbeeAddress[address] = nil
        client:unsubscribe(mqttTopic..address..'/#')
        subscribed[zigbeeDevices[address].friendly] = nil
        if logging then log('Unsubscribed '..mqttTopic..zigbeeDevices[address].friendly..'/#') end
      end
    end
  end
  -- Log the summary
  if remCount > 0 then log('Removed '..remCount..' Zigbee object'..(remCount ~= 1 and 's' or '')) end
  if addCount > 0 then log('Added '..addCount..' Zigbee object'..(addCount ~= 1 and 's' or '')) end
  if modCount > 0 then log('Modified '..modCount..' Zigbee object'..(modCount ~= 1 and 's' or '')) end
end


--[[
Publish to MQTT
--]]
local function publish(alias, level, origin, ramp)
  if not zigbee[alias].address then return 2 end
  if zigbee[alias].class == 'sensor' then return 2 end
  if not zigbeeDevices[zigbee[alias].address].available then if keepMessagesForOfflineQueued then return 1 else return 2 end end -- Device is currently unavailable, so keep queued if keepMessagesForOfflineQueued is true

  local msg
  if zigbee[alias].class == 'switch' then
    msg = {
      [zigbee[alias].exposed] = (level ~= 0) and 'ON' or 'OFF',
    }
  else
    local duration
    if hasMembers(zigbeeDevices) and zigbeeDevices[zigbee[alias].address] then -- Some zigbee devices have a max brightness of less than 255
      local max = zigbeeDevices[zigbee[alias].address].max
      if max ~= nil then
        if level > max then level = max end
      end
    end
    if ramp > 0 then
      duration = math.abs(level - origin) / 256 * ramp -- Translate ramp rate to transition time
      suppressMqttUpdates[alias] = socket.gettime() + duration + 1 -- Expected completion time with a small buffer
    else
      duration = nil
    end
    local state = (level ~= 0) and 'ON' or 'OFF'
    if duration ~= nil and duration > 0 and level == 0 then state = nil end
    msg = {
      brightness = level,
      state = state,
      transition = duration,
    }
  end
  ignoreMqtt[alias] = true
  local topic = mqttTopic..zigbeeDevices[zigbee[alias].address].friendly..'/set'
  if logging then log('Publish '..alias..' '..topic..', '..json.encode(msg)) end
  client:publish(topic, json.encode(msg), QoS, false)
  return 0
end


--[[
Publish current level and state to MQTT
--]]
local function publishCurrent()
  local alias, v
  for alias, v in pairs(zigbee) do
    local level = grp.getvalue(alias)
    publish(alias, level, level, 0)
  end
end


--[[
Receive commands from C-Bus and publish to MQTT
--]]
local function outstandingCbusMessage()
  local keep = {}
  for _, cmd in ipairs(cbusMessages) do
    if ignoreCbus[cmd.alias] then
      ignoreCbus[cmd.alias] = nil
    else
      if publish(cmd.alias, cmd.level, cmd.origin, cmd.ramp) == 1 then keep[#keep+1] = cmd end -- Device unavailable, so keep trying
    end
  end
  cbusMessages = keep
end


--[[
Available devices has changed
--]]
local function updateDevices(payload)
  local d, e, f, new, modified
  local found = {}
  local kill = {}
  for _, d in ipairs(payload) do
    new = false
    modified = false
    local friendly = nil if d.friendly_name and type(d.friendly_name) ~= 'userdata' then friendly = d.friendly_name end

    if friendly == 'Coordinator' then goto skip end
    found[d.ieee_address] = true
    if zigbeeDevices[d.ieee_address] == nil then
      new = true
      zigbeeDevices[d.ieee_address] = { available=true } -- Initially available, updated on device topic subscribe if availability is configured
      if logging then log('Found a device '..d.ieee_address..(d.friendly_name ~= nil and ' (friendly name: '..d.friendly_name..')' or '')) end
    end
    if zigbeeDevices[d.ieee_address].friendly ~= friendly then zigbeeDevices[d.ieee_address].friendly = friendly modified = true end
    zigbeeName[friendly] = d.ieee_address
    if type(d.definition) ~= 'userdata' and d.definition.exposes then
      zigbeeDevices[d.ieee_address].exposesRaw = d.definition.exposes
      for _, e in pairs(d.definition.exposes) do
        if e.type == 'light' then
          if e.features and type(d.features) ~= 'userdata' then
            for _, f in ipairs(e.features) do
              if f.name == 'brightness' then
                zigbeeDevices[d.ieee_address].max = value_max
              end
            end
          end
        end
      end
      if not new and modified then
        if logging then log('Update for a device '..d.ieee_address..(d.friendly_name ~= nil and ', '..d.friendly_name or '')) end
      end
    end
    ::skip::
  end
  for d, _ in pairs(zigbeeDevices) do if not found[d] then kill[d] = true if logging then log('Removed a device '..d) end end end
  for d, _ in pairs(kill) do zigbeeDevices[d] = nil end
end


--[[
Available groups has changed
--]]
local function updateGroups(payload)
end


--[[
A device has updated status, so send to C-Bus
--]]
local function statusUpdate(friendly, payload)
  local device
  if hasMembers(zigbeeDevices) then device = zigbeeDevices[zigbeeName[friendly]] else return end
  if not device then return end

  if device.class == 'switch' then
    local s
    for _, s in ipairs(device.exposed) do
      local value = payload[s.expose]
      value = (value == 'ON') and 255 or 0
      if value then
        if grp.getvalue(s.alias) ~= value then
          if logging then log('Set '..s.alias..', '..s.expose..'='..value) end
          ignoreCbus[s.alias] = true
          SetCBusLevel(s.net, s.app, s.group, value, 0)
          zigbee[s.alias].value = value
        end
      else
        log('Error: Nil value for sensor '..salias)
      end
    end
  elseif device.class == 'sensor' then
    local s
    for _, s in ipairs(device.exposed) do
      local value = payload[s.expose]
      if s.type == 'boolean' then value = value and 1 or 0 end
      if value then
        if string.format('%.5f', grp.getvalue(s.alias)) ~= string.format('%.5f', value) then
          if logging then log('Set '..s.alias..', '..s.expose..'='..value) end
          ignoreCbus[s.alias] = true
          if lighting[tostring(s.app)] then -- Lighting group sensor
            SetCBusLevel(s.net, s.app, s.group, value, 0)
          elseif s.app == 250 then -- User param
            SetUserParam(s.net, s.group, value)
          elseif s.app == 228 then -- Measurement
            SetCBusMeasurement(s.net, s.group, s.channel, value, cbusMeasurementUnits[s.expose] or 0)
          end
          zigbee[s.alias].value = value
        end
      else
        log('Error: Nil value for sensor '..salias)
      end
    end
  else
    local net = zigbeeAddress[zigbeeName[friendly]].net
    local app = zigbeeAddress[zigbeeName[friendly]].app
    local group = zigbeeAddress[zigbeeName[friendly]].group
    local alias = zigbeeAddress[zigbeeName[friendly]].alias

    if payload.brightness then
      if payload.state == 'ON' then
        local level = payload.brightness
        local max = device.max
        if max ~= nil then
         if level == max then level = 255 end
        end
        if grp.getvalue(alias) ~= level then
          if logging then log('Set '..alias..' to '..level) end
          ignoreCbus[alias] = true
          SetCBusLevel(net, app, group, level, 0)
          zigbee[alias].value = level
        end
      else
        if grp.getvalue(alias) ~= 0 then
          if logging then log('Set '..alias..' to OFF') end
          ignoreCbus[alias] = true
          SetCBusLevel(net, app, group, 0, 0)
          zigbee[alias].value = 0
        end
      end
    end
  end
end


--[[
Send commands subscribed from MQTT to C-Bus
--]]
local function outstandingMqttMessage()
  for _, msg in ipairs(mqttMessages) do
    local parts = msg.topic:split('/')
    if parts[2] == 'bridge' then
      if parts[3] == 'devices' then updateDevices(msg.payload)
      elseif parts[3] == 'groups' then updateGroups(msg.payload)
      end
    else
      -- Find the friendly name
      local i
      local friendly = {}
      local s = 2
      if parts[#parts] == 'availability' then
        local avail = false
        local e = #parts - 1
        for i = s, e do table.insert(friendly, parts[i]) end friendly = table.concat(friendly, '/')
        if type(msg.payload) == 'string' then -- Legacy mode
          avail = msg.payload == 'online'
        else
          avail = msg.payload.state == 'online'
        end
        zigbeeDevices[zigbeeName[friendly]].available = avail
        if logging then log('Device '..friendly..(avail and ' is available' or ' is NOT available')) end
      elseif parts[#parts] == 'set' then -- Do nothing
      elseif parts[#parts] == 'get' then -- Do nothing
      else -- Status update
        local e = #parts
        for i = s, e do table.insert(friendly, parts[i]) end friendly = table.concat(friendly, '/')
        local alias = zigbeeAddress[zigbeeName[friendly]].alias
        if not ignoreMqtt[alias] then
          if not suppressMqttUpdates[alias] then statusUpdate(friendly, msg.payload) end
        else
          ignoreMqtt[alias] = nil
        end
      end
    end
  end
  mqttMessages = {}
end

    
-- Reconnect variables
local warningTimeout = 30
local timeout = 1
local timeoutStart, connectStart, mqttConnected
local onReconnect = {}
local bridgeSubscribed = false

local changesChecked = socket.gettime()

-- Main loop: Process both C-Bus and Mosquitto broker messages
while true do
  local stat, err

  localbus:step()

  -- Clean up expired transition suppressions
  local status, finish
  for alias, finish in pairs(suppressMqttUpdates) do if socket.gettime() > finish then suppressMqttUpdates[alias] = nil if logging then log('Expired transition for '..alias) end end end
  
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
    -- Broker is disconnected, so attempt a connection, waiting. If fail to connect then retry.
    if bridgeSubscribed then
      client:unsubscribe(mqttTopic..'bridge/#', QoS)
      bridgeSubscribed = false
    if logging then log('Unsubscribed '..mqttTopic..'bridge/#') end
    end
    for friendly, _ in pairs(subscribed) do
      client:unsubscribe(mqttTopic..friendly..'/#', QoS)
      if logging then log('Unubscribed '..mqttTopic..friendly..'/#') end
      table.insert(onReconnect, friendly)
      subscribed[friendly] = nil
    end
    if init then
      log('Connecting to Mosquitto broker')
      timeoutStart = socket.gettime()
      connectStart = timeoutStart
      init = false
    end
    stat, err = pcall(function (b, p, k) client:connect(b, p, k) end, mqttBroker, 1883, 25) -- Requested keep-alive 25 seconds, broker at port 1883
    if not stat then -- Log and abort
      log('Error calling connect to broker: '..err)
      do return end
    end
    while mqttStatus ~= 1 do
      client:loop(1) -- Service the client on startup with a generous timeout
      if socket.gettime() - connectStart > timeout then
        if socket.gettime() - timeoutStart > warningTimeout then
          log('Failed to connect to the Mosquitto broker, retrying continuously')
          timeoutStart = socket.gettime()
        end
        connectStart = socket.gettime()
        goto next -- Exit to the main loop to keep localbus messages monitored
      end
    end
    mqttConnected = socket.gettime()
    -- Subscribe to bridge topics
    client:subscribe(mqttTopic..'bridge/#', QoS)
    if logging then log('Subscribed '..mqttTopic..'bridge/#') end
    bridgeSubscribed = true
    -- Connected... Now loop briefly to allow retained value retrieval for the bridge first (because synchronous), which will ensure all mqttDevices get created before device topics are processed
    while socket.gettime() - mqttConnected < 0.5 do
      client:loop(0)
      if #mqttMessages > 0 then
        stat, err = pcall(outstandingMqttMessage) -- Process outstanding bridge messages
        if not stat then log('Error processing outstanding MQTT messages: '..err) mqttMessages = {} end -- Log error and clear the queue
      end
    end
    if not reconnect then -- Full publish topics
      stat, err = pcall(cudZig) if not stat then log('Error in cudZig(): '..err) end
      stat, err = pcall(publishCurrent) if not stat then log('Error publishing current values: '..err) end -- Log and continue
    else -- Resubscribe
      local friendly
      for _, friendly in ipairs(onReconnect) do
        ignoreMqtt[zigbeeAddress[zigbeeName[friendly]].alias] = true
        client:subscribe(mqttTopic..friendly..'/#', QoS)
        subscribed[friendly] = true
        if logging then log('Subscribed '..mqttTopic..friendly..'/#') end
      end
      onReconnect = {}
    end
  else
    log('Error: Invalid mqttStatus: '..mqttStatus)
    do return end
  end

  local t = socket.gettime()
  if checkChanges and t > changesChecked + checkChanges then
    changesChecked = t
    stat, err = pcall(cudZig) if not stat then log('Error in cudZig(): '..err) end
  end

  ::next::
end