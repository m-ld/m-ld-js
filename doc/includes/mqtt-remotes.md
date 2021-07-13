### MQTT Remotes
[MQTT](http://mqtt.org/) is a machine-to-machine (M2M)/"Internet of Things"
connectivity protocol. It is convenient to use it for local development or if
the deployment environment has an MQTT broker available. See below for specific
broker requirements.

The `MqttRemotes` class and its companion configuration class `MeldMqttConfig`
can be imported or required from `'@m-ld/m-ld/dist/mqtt'`. You must also 
install the [`async-mqtt`](https://www.npmjs.com/package/async-mqtt) package 
as a peer of `@m-ld/m-ld`.

The configuration interface adds an `mqtt` key to the base
[`MeldConfig`](interfaces/meldconfig.html). The content of this key is a client
options object for [MQTT.js](https://www.npmjs.com/package/mqtt#client). It must
not include the `will` and `clientId` options, as these are set internally. It
must include a `hostname` _or_ a `host` and `port`.

### MQTT Broker Requirements
`MqttRemotes` requires broker support for:
1. MQTT 3.1
1. QoS 0 and 1
1. Retained messages
1. Last Will and Testament (LWT) messages

A good choice for local development is [Aedes](https://github.com/moscajs/aedes).