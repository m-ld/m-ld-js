# **m-ld** Javascript clone engine
The Javascript engine can be used in any modern browser or in [Node.js](https://nodejs.org/).

## install
`npm install @m-ld/m-ld -S`

## basic usage
```js
import { clone } from '@m-ld/m-ld';
```
The [clone](#clone) function initialises the m-ld engine with a leveldb
back-end and the clone [configuration](interfaces/meldconfig.html).
```js
const meld = await clone(ldb, config);
```
The `ldb` argument must be an instance of a leveldb backend.