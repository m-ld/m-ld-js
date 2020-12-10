🚧 *This documentation is for the [developer preview](http://m-ld.org/#developer-preview) of **m-ld**.*

[![github](https://img.shields.io/badge/m--ld-m--ld--js-red?logo=github)](https://github.com/m-ld/m-ld-js)
[![licence](https://img.shields.io/github/license/m-ld/m-ld-js)](https://github.com/m-ld/m-ld-js/blob/master/LICENSE)

# **m-ld** Javascript clone engine
The Javascript engine can be used in a modern browser or a server engine like
[Node.js](https://nodejs.org/).

> The Javascript clone engine conforms to the **m-ld**
> [specification](http://spec.m-ld.org/). Its support for transaction pattern
> complexity is detailed [below](#transactions). Its [concurrency](#concurrency)
> model is based on immutable states.

## Getting Started
`npm install @m-ld/m-ld`

To see some executable code, have a look at the Node.js
[starter&nbsp;project](https://github.com/m-ld/m-ld-nodejs-starter).

### Data Persistence
**m-ld** uses [levelup](https://github.com/level/levelup) to interface with a
LevelDB-compatible storage backend.
- For the fastest responses use [memdown](https://github.com/level/memdown)
  (memory storage).
- In the browser, use [level-js](https://github.com/Level/level-js)
  (browser-local storage).
- In a service, use [leveldown](https://github.com/level/leveldown/)
  (file system storage).

### Connecting to Other Clones
A **m-ld** clone uses a 'remotes' object to communicate with other clones.
- If you have an MQTT broker available, use [`MqttRemotes`](#mqtt-remotes).
- For a scalable global managed service, use [`AblyRemotes`](#ably-remotes).

> 🚧 *If your architecture includes some other publish/subscribe service like
> AMQP, or you would like to use a fully peer-to-peer protocol, please
> [contact&nbsp;us](mailto:info@m-ld.io) to discuss your use-case.*

### Initialisation
The [clone](#clone) function initialises the m-ld engine with a leveldb back-end
and the clone [configuration](interfaces/meldconfig.html).
```typescript
import MemDown from 'memdown';
import { clone, uuid } from '@m-ld/m-ld';
import { MqttRemotes, MeldMqttConfig } from '@m-ld/m-ld/dist/mqtt';

const config: MeldMqttConfig = {
  '@id': uuid(),
  '@domain': 'test.example.org',
  genesis: true,
  mqtt: { hostname: 'mqtt.example.org' }
};

const meld = await clone(new MemDown, MqttRemotes, config);
```

The `clone` function returns control as soon as it is safe to start making data
transactions against the domain. If this clone has has been re-started from
persisted state, it may still be receiving updates from the domain. This can
cause a UI to start showing these updates. If instead, you want to wait until
the clone has the most recent data, you can add:
```typescript
await meld.status.becomes({ online: true, outdated: false });
```

[[include:mqtt-remotes.md]]

[[include:ably-remotes.md]]

[[include:transactions.md]]

[[include:concurrency.md]]
