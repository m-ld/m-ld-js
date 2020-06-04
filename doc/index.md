# **m-ld** Javascript clone engine
The Javascript engine can be used in any modern browser or in
[Node.js](https://nodejs.org/).

## install
`npm install @m-ld/m-ld -S`

## basic usage
```js
import { clone } from '@m-ld/m-ld';
```
**m-ld** uses
[levelup](https://github.com/level/levelup) to interface with a
LevelDB-compatible storage backend, such as
[leveldown](https://github.com/level/leveldown/),
[level-js](https://github.com/Level/level-js) or
[memdown](https://github.com/level/memdown).

The [clone](#clone) function initialises the m-ld engine with a leveldb back-end
and the clone [configuration](interfaces/meldconfig.html).
```js
const meld = await clone(new MemDown, config);
```
