## MQTT Remotes
[MQTT](http://mqtt.org/) is a machine-to-machine (M2M)/"Internet of Things"
connectivity protocol. It is convenient to use it for local development or if
the deployment environment has an MQTT broker available. See below for specific
broker requirements.

The `MqttRemotes` class and its companion configuration class `MeldMqttConfig`
can be imported or required from `'@m-ld/m-ld/dist/mqtt'`.

The configuration interface adds an `mqtt` key to the base
[`MeldConfig`](interfaces/meldconfig.html). The content of this key is a client
options object from [MQTT.js](https://www.npmjs.com/package/mqtt#client). It
must not include the `will` and `clientId` options as these are set internally.
It must include a `hostname` _or_ a `host` and `port`.

### MQTT Broker Requirements
1. MQTT 3.1 compliant
1. QoS 0 and 1 must be supported
1. Retained messages must be supported
1. Last Will and Testament (LWT) messages must be supported

A good choice for local development is [Aedes](https://github.com/moscajs/aedes).
