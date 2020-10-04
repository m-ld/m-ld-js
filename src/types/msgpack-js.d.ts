declare module '@ably/msgpack-js' {
  function encode(value: any, sparse?: boolean): Uint8Array;
  function decode(buffer: Uint8Array): any;
}