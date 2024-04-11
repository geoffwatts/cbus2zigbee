# Clipsal C-Bus Automation Controller Integration with Zigbee2MQTT

## Overview

This project provides integration between Clipsal C-Bus automation controllers and Zigbee2MQTT.

The integration consists of a single resident script, which listens for both C-Bus level changes and Mosquitto broker messages, sending messages bidirectionally. Changes to C-Bus will set the Zigbee devices, and Zigbee status changes will set C-Bus objects.

Lighting group, measurement app and user parameters are implemented.

## Setup

### Prerequisites

- Clipsal C-Bus automation controller (SHAC, NAC, AC2, NAC2).
- Zigbee2mqtt instance running on a compatible device (e.g., Raspberry Pi, Home Assistant add-in) in the same network as the C-Bus controller.
- Mosquitto broker (such as Mosquitto, Home Assistant add-in) accessible to both the C-Bus controller and Zigbee2MQTT.

### Installation Steps

1. **Install Lua Script**: Create a resident script, call it what you want with a sensible sleep interval (zero is fine) and paste "zigbee resident.lua".  

2. **Configure Zigbee2MQTT**: Ensure that Zigbee2MQTT is configured correctly.

3. **Update Configuration**: Modify the Lua scripts to include the Mosquitto broker details and any other configuration specific to your setup.

4. **Configure Automation Controller keywords**: Tag C-Bus devices with the "ZIGBEE" keyword and other keywords as described below.

### Keywords

Along with the keyword ZIGBEE, the available keywords to use are as follows, with either z= or n= being required.

* n=My Device (name= is an alias), to use the device friendly name, which may include '/'. The IEEE address will be looked up.

... or
* z=0x12345678abcdef123 (addr= is an alias), to use the IEEE address of the Zigbee device, which should be used where a friendly name is not defined, or if one is defined and z= is used then the friendly name will be looked up.

Add the following:

* light, to indicate a lighting object (the default type, so optional to specify for lights)
* switch, to indicate a switching object
* sensor, to indicate a sensor object
* group, to indicate a lighting group
* exposed=exposes_name, to tie a C-Bus object to an exposed Zigbee value (sensor/switch only, exposes= is an alias)
* type=number|boolean, to specify the data type (sensor only, "number" is the default)
* parameter=altparameter, to specify an alternate for a 'parameter' value in the 'exposes' value of a Zigbee object (sensor/switch only, the default is "parameter")

### Keyword Examples

* ZIGBEE, light, z=0xa4c1389bf2e3ae5f, 
* ZIGBEE, light, name=Office/Office desk strip, 
* ZIGBEE, group, name=Kitchen lights group, 
* ZIGBEE, sensor, addr=0x00169a00022256da, exposed=humidity, 
* ZIGBEE, switch, name=3way, exposes=state_l1, 

### About groups

Zigbee goups are implemented, but only uni-derectionally from C-Bus to Zigbee2Mqtt at this stage, so group object updates will not be seen on C-Bus. The reason is twofold. Firstly, any status change of a Zigbee group member will result in the whole group being updated to match that individual group member. This is somewhat misleading. Secondly, transitioning a Zigbee group to off can result in a blunt "I am off" message immediately being being sent for the Zigbee group, which again is somewhet misleading. Instead, the C-Bus object will not change, rather acting as a conduit for sent Zigbee group commands, and retaining the last command sent as its value.

## Contributing

Contributions to this project are welcome! If you encounter any issues, have feature requests, or would like to contribute improvements, please open an issue or submit a pull request. I have also started talking about this on the C-Bus Forums at https://www.cbusforums.com/threads/cbus2zigbee.11245/

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments

Special thanks to [contributors](CONTRIBUTORS.md) who have helped improve this project.