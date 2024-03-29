# Clipsal C-Bus Automation Controller Integration with Zigbee2mqtt

## Overview

This project provides integration between Clipsal C-Bus automation controllers and Zigbee2MQTT.

The integration consists of a single resident script, which listens for both C-Bus level changes and Mosquitto broker messages, sending messages bidirectionally. Changes to C-Bus will set the Zigbee devices, and Zigbee status changes will set C-Bus objects. Lighting group and user parameters are implemented.

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

* n=My Device, which will use the device friendly name, which may include '/'. (name= is an alias.) The IEEE address will be looked up.

... or
* z=0x12345678abcdef123, which is the IEEE address of the Zigbee device, which should be used where a friendly name is not defined, or if one is defined and z= is used then the friendly name will be looked up. (addr= is an alias.)

For Zigbee sensors, add the following:

* sensor=exposes_name, to tie a C-Bus user parameter or lighting object to an exposed Zigbee value
* type=number|boolean, to specify the data type

## Contributing

Contributions to this project are welcome! If you encounter any issues, have feature requests, or would like to contribute improvements, please open an issue or submit a pull request. I have also started talking about this on the C-Bus Forums at https://www.cbusforums.com/threads/cbus2zigbee.11245/

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments

Special thanks to [contributors](CONTRIBUTORS.md) who have helped improve this project.