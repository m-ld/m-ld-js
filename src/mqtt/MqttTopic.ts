import { exec, TopicParams } from 'mqtt-pattern';
import { flatten } from '../engine/util';
import { SendParams, ReplyParams } from '../engine/PubsubRemotes';
import { array } from '../util';

type TopicPart<P> = string | { '+': keyof P; } | { '#': keyof P; };

export class MqttTopic<P extends TopicParams = TopicParams> {
  private readonly parts: TopicPart<P>[];

  constructor(...parts: TopicPart<P>[]) {
    this.parts = parts;
  }

  match(topic: string): P | null;
  match(topic: string, handler: (params: P) => void): void;
  match(topic: string, handler?: (params: P) => void): P | null | void {
    const params = (exec(this.pattern, topic) as P | null);
    if (params && handler)
      handler.call(this, params);
    else
      return params;
  }

  with(params: Partial<P>) {
    return new MqttTopic<P>(...flatten<TopicPart<P>>(this.parts.map(part => typeof part === 'string' ?
      [part] : '+' in part ? array<TopicPart<P>>(params[part['+']] || part) :
        array<TopicPart<P>>(params[part['#']] || part))));
  }

  get address(): string {
    return this.path.join('/');
  }

  get path(): string[] {
    return this.parts.map(part => typeof part === 'string' ?
      part : Object.keys(part)[0]);
  }

  private get pattern(): string {
    return this.parts.map(part => typeof part === 'string' ?
      part : '+' in part ? '+' + part['+'] : '#' + part['#']).join('/');
  }
}

export interface SendAddressParams extends SendParams {
  address: string[];
}

export const SEND_TOPIC = new MqttTopic<SendAddressParams & TopicParams>
  ('__send', { '+': 'toId' }, { '+': 'fromId' }, { '+': 'messageId' }, { '#': 'address' });

export const REPLY_TOPIC = new MqttTopic<ReplyParams & TopicParams>
  ('__reply', { '+': 'toId' }, { '+': 'fromId' }, { '+': 'messageId' }, { '+': 'sentMessageId' });

