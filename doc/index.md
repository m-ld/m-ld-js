# **m-ld** Javascript clone engine
The Javascript engine can be used in a modern browser or a server engine like
[Node.js](https://nodejs.org/).

## Install
1. Download the [package here](media://m-ld.tgz)
1. `npm install <path/to>/m-ld.tgz`

## Data Persistence
**m-ld** uses [levelup](https://github.com/level/levelup) to interface with a
LevelDB-compatible storage backend.
- In the browser, use [level-js](https://github.com/Level/level-js).
- In a service, use [leveldown](https://github.com/level/leveldown/).
- If you can be sure there is another clone which is storing the data, for the
  fastest responses use [memdown](https://github.com/level/memdown).

## Connecting to Other Clones
A **m-ld** clone uses a 'remotes' object to communicate with other clones.
- If you have an MQTT broker available, use [`MqttRemotes`](#mqtt-remotes).
- For a scalable global managed service, use [`AblyRemotes`](#ably-remotes).

## Basic usage
The [clone](#clone) function initialises the m-ld engine with a leveldb back-end
and the clone [configuration](interfaces/meldconfig.html).
```js
import { clone, uuid } from '@m-ld/m-ld';
import { MqttRemotes, MeldMqttConfig } from '@m-ld/m-ld/dist/mqtt';

const config: MeldMqttConfig = {
  '@id': uuid(),
  '@domain': 'test.example.org',
  genesis: true,
  mqtt: { hostname: 'mqtt.example.org' }
};

const meld = await clone(new MemDown, MqttRemotes, config);
await meld.status.becomes({ online: true, outdated: false });
```
[[include:mqtt-remotes.md]]
[[include:ably-remotes.md]]
