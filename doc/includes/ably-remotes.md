### Ably Remotes
[Ably](https://www.ably.io/) provides infrastructure and APIs to power realtime
experiences at scale. It is a managed service, and includes pay-as-you-go
[developer pricing](https://www.ably.io/pricing). It is also convenient to use
for global deployments without the need to self-manage a broker.

The `AblyRemotes` class and its companion configuration class `MeldAblyConfig`
can be imported or required from `'@m-ld/m-ld/ext/ably'`. You must also 
install the [ably](https://www.npmjs.com/package/ably) package
as a peer of `@m-ld/m-ld`.

The configuration interface adds an `ably` key to the base
[`MeldConfig`](interfaces/meldconfig.html). The content of this key is an Ably
[client options
object](https://www.ably.io/documentation/realtime/usage#client-options). It
must not include the `echoMessages` and `clientId` options, as these are set
internally.

If using [token
authentication](https://www.ably.io/documentation/core-features/authentication#token-authentication),
ensure that the `clientId` the token is generated for corresponds to the `@id`
given in the [`MeldConfig`](interfaces/meldconfig.html).