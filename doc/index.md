[[include:live-code-setup.script.html]]

🚧 *This documentation is for
the [developer preview](http://m-ld.org/#developer-preview) of **m-ld**.*

[![github](https://img.shields.io/badge/m--ld-m--ld--js-red?logo=github)](https://github.com/m-ld/m-ld-js)
[![licence](https://img.shields.io/github/license/m-ld/m-ld-js)](https://github.com/m-ld/m-ld-js/blob/master/LICENSE)
[![npm (tag)](https://img.shields.io/npm/v/@m-ld/m-ld)](https://www.npmjs.com/package/@m-ld/m-ld)
[![Gitter](https://img.shields.io/gitter/room/m-ld/community)](https://gitter.im/m-ld/community)
[![GitHub Discussions](https://img.shields.io/github/discussions/m-ld/m-ld-spec)](https://github.com/m-ld/m-ld-spec/discussions)

# **m-ld** Javascript clone engine

The Javascript engine can be used in a modern **browser** or a server engine like
[**Node.js**](https://nodejs.org/).

> The Javascript clone engine conforms to the **m-ld**
[specification](http://spec.m-ld.org/). Its support for transaction patterns
> is detailed [below](#transactions). Its API [concurrency](#concurrency) model
> is based on serialised consistent states.

[[include:quick-start.md]]

There are more live code examples for you to try in the [How-To](#how-to) section below.

## Introduction

### Packages

In Node.js, or using a bundler, use:

`npm install @m-ld/m-ld`

Browser bundles (as used above) are served on:
- `js.m-ld.org/ext` – latest stable prerelease
- `edge.js.m-ld.org/ext` – bleeding edge

They include the core (`index.mjs`), bundled [remotes](#remotes) (`mqtt.mjs`, `ably.mjs` and `socket.io.mjs`), in-memory backend (`memory-level.mjs`) and [other extensions](#extensions).

Some example starter projects available:

- The [TodoMVC App](https://github.com/m-ld/m-ld-web-starter) shows one way to build a multi-collaborator application for browsers.
- The [Node.js Starter Project](https://github.com/m-ld/m-ld-nodejs-starter) uses Node processes to initialise two clones, and an MQTT broker for messaging.

### Data Persistence

**m-ld** uses [abstract-level](https://github.com/Level/abstract-level) to interface with a LevelDB-compatible storage backend.

- For the fastest in-memory responses, use [memory-level](https://github.com/Level/memory-level).
- In a service or native application, use [classic-level](https://github.com/Level/classic-level) (file system storage).
- In a browser, you can use [browser-level](https://github.com/Level/browser-level) (browser-local storage).

### Connecting to Other Clones

A **m-ld** clone uses a 'remotes' object to communicate with other clones.

- With an MQTT broker, use [`MqttRemotes`](#mqtt-remotes).
- For a scalable global managed service, use [`AblyRemotes`](#ably-remotes).
- If you have a live web server (not just CDN or serverless), you can use
  [`IoRemotes`](#socketio-remotes).

> 💡 If your architecture includes some other publish/subscribe service like AMQP or Apache Kafka, or you would like to use a fully peer-to-peer protocol, please [contact&nbsp;us](https://m-ld.org/hello/) to discuss your use-case. Remotes can even utilise multiple transport protocols, for example WebRTC with a suitable signalling service.

### Initialisation

The [clone](#clone) function initialises the m-ld engine with a leveldb back-end
and the clone [configuration](interfaces/meldconfig.html).

```typescript
import { clone, uuid } from '@m-ld/m-ld';
import { MemoryLevel } from 'memory-level';
import { MqttRemotes, MeldMqttConfig } from '@m-ld/m-ld/ext/mqtt';

const config: MeldMqttConfig = {
  '@id': uuid(),
  '@domain': 'test.example.org',
  genesis: true,
  mqtt: { hostname: 'mqtt.example.org' }
};

const meld = await clone(new MemoryLevel, MqttRemotes, config);
```

The `clone` function returns control as soon as it is safe to start making data transactions against the domain. If this clone has been re-started from persisted state, it may still be receiving updates from the domain. This can cause a UI to start showing these updates. If instead, you want to wait until the clone has the most recent data, you can add:

```typescript
await meld.status.becomes({ online: true, outdated: false });
```

## Remotes

[[include:ext/mqtt-remotes.md]]

[[include:ext/ably-remotes.md]]

[[include:ext/socketio-remotes.md]]

[[include:transactions.md]]

[[include:subjects.md]]

[[include:concurrency.md]]

## How-To

Here is a roll-up of links to usage docs and live coding examples to help get started with common patterns.

[[include:how-to/domain-setup.md]]

### Reading, Following and Writing

These examples show simple patterns for getting started with an app's code structure. In principle, **m-ld** acts as a "model", replacing (or being proxied by) the local in-memory data model. Because **m-ld** information is fundamentally _live_ – it can change due to a remote edit as well as a local one – it's valuable for the local app code to react to changes that it may not have initiated itself.

[[include:how-to/kb-setup.md]]
[[include:how-to/kb-reload-all.md]]
[[include:how-to/kb-handle-updates.md]]
[[include:how-to/kb-subject-updater.md]]

### [Using Shape Constraints](classes/shapeconstrained.html)

One of the most common questions asked about live information models is, what happens if there is a "conflict"? Here, we handle one particular kind of conflict using declarative constraints.

[[include:how-to/shapes.md]]

### [Using Lists](interfaces/List.html)

By default, information in **m-ld** is an unordered _graph_ (just like in a relational database). This example shows how ordered lists can be embedded in the graph.

[[include:how-to/lists.md]]

### [Using Collaborative Text](classes/tseqtext.html)

Text embedded in a structured graph of information might need to be editable by multiple users at the same time, like an online document. This example shows the use of an embedded data type and a supporting HTML control, to enable multi-player text editing.

[[include:how-to/text.md]]

### Using the RDF/JS API

The information in **m-ld** is stored using the W3C standard data representation RDF (Resource Description Framework). For RDF-native apps, the Javascript engine API supports direct access to the RDF graph.

[[include:how-to/rdfjs.md]]

[[include:security.md]]

[[include:ext/index.md]]
