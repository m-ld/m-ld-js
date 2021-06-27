### Socket.io Remotes

[Socket.IO](https://socket.io/) enables real-time, bidirectional and event-based
communication. It works on every platform, browser or device, focusing equally
on reliability and speed. It is convenient to use when the app architecture has
a live web server or app server, using HTTP.

The `IoRemotes` class and its companion configuration class `MeldIoConfig`
can be imported or required from `'@m-ld/m-ld/dist/socket.io'`. You must also
install the [`socket.io-client`](https://www.npmjs.com/package/socket.io-client)
package as a peer of `@m-ld/m-ld`.

The configuration interface adds an `io` key to the base
[`MeldConfig`](interfaces/meldconfig.html). The value is an optional object
having:

- `uri`: The server URL (defaults to the browser URL with no path)
- `opts`: A
  Socket.io [factory&nbsp;options](https://socket.io/docs/v4/client-initialization/#IO-factory-options)
  object, which can be used to customise the server connection

### Socket.io Server

When using Socket.io, the server must correctly route **m-ld** protocol
operations to their intended recipients. The Javascript engine package bundles a
class for Node.js servers,  `IoRemotesService`, which can be imported
from `'@m-ld/m-ld/dist/socket.io/server'`.

To use, [initialise](https://socket.io/docs/v4/server-initialization/) the
Socket.io server as normal, and then construct an `IoRemotesService`, passing
the namespace you want to make available to **m-ld**. To use the global
namespace, pass the `sockets` member of the `Server` class. For example:

```js
const socket = require('socket.io');
const httpServer = require('http').createServer();
// Start the Socket.io server, and attach the m-ld message-passing service
const io = new socket.Server(httpServer);
new IoRemotesService(io.sockets);
```

For a complete example, see
the [web starter project](https://github.com/m-ld/m-ld-web-starter/blob/main/index.js)
.

For other server types, [contact&nbsp;us](https://m-ld.org/hello/).