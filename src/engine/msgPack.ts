import { decode as rawDecode, encode as rawEncode } from '@ably/msgpack-js';

export const encode = (value: any) => Buffer.from(rawEncode(value).buffer);
export const decode = (buffer: Uint8Array) => rawDecode(Buffer.from(buffer));
