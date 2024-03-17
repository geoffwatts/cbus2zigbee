# Clipsal C-Bus Automation Controller Integration with Zigbee2mqtt

## Overview

This project provides integration between Clipsal C-Bus automation controllers and Zigbee2mqtt, allowing changes in C-Bus groups (lights) to instantly turn on, off or dim zigbee devices.

The integration consists of two main components:

1. **Event Script**: A Lua script running on the C-Bus automation controller triggers events during C-Bus device state changes. These events are then relayed to Zigbee2mqtt for further processing.

2. **Resident Script**: Another Lua script running on the C-Bus automation controller listens for events from the event script and publishes corresponding commands to Zigbee2mqtt via MQTT, controlling Zigbee devices accordingly.

## Setup

### Prerequisites

- Clipsal C-Bus automation controller with Lua scripting support.
- Zigbee2mqtt instance running on a compatible device (e.g., Raspberry Pi) in the same network as the C-Bus controller.
- MQTT broker (such as Mosquitto) accessible to both the C-Bus controller and Zigbee2mqtt.

### Installation Steps

1. **Install Lua Scripts**: Create an event script called ZIGBEE, and paste "zigbee event.lua" in that script.  Mark that script "execute during ramping" so that the controller will send updates to dim the lights during a ramp.  Create a resident script and paste "zigbee resident.lua".  

2. **Configure Zigbee2mqtt**: Ensure that Zigbee2mqtt is configured correctly and is running on a device reachable from the C-Bus controller.

3. **Update Configuration**: Modify the Lua scripts to include the MQTT broker details and any other configuration specific to your setup.

4. **Assign Zigbee Addresses**: Tag C-Bus devices with the `ZIGBEE` keyword and include the Zigbee device's address using the `z=<zigbee address>` tag. Alternatively, populate the `addresses` table in the Lua scripts with mappings between C-Bus group addresses and Zigbee device addresses, which will perform better (but I may remove in future versions, particularly as I update this to bidirectional)

## Contributing

Contributions to this project are welcome! If you encounter any issues, have feature requests, or would like to contribute improvements, please open an issue or submit a pull request.  I have also started talking about this on the C-Bus Forums at https://www.cbusforums.com/threads/cbus2zigbee.11245/

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments

Special thanks to [contributors](CONTRIBUTORS.md) who have helped improve this project.