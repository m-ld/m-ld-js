import { exec, TopicParams } from 'mqtt-pattern';
import { flatten, toArray } from './util';

export class MqttTopic<P extends TopicParams = TopicParams> {
  constructor(
    private readonly parts: Array<string | { '+': keyof P; } | { '#': keyof P; }>) {
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
    return new MqttTopic<P>(flatten(this.parts.map(part => typeof part === 'string' ?
      [part] : '+' in part ? toArray(params[part['+']] || '+') : toArray(params[part['#']] || '#'))));
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

export interface DirectParams extends TopicParams {
  toId: string;
  fromId: string;
  messageId: string;
}

export interface SentParams extends DirectParams {
  address: string[];
}

export interface ReplyParams extends DirectParams {
  sentMessageId: string;
}

export const SENT_TOPIC = new MqttTopic<SentParams>
  (['__send', { '+': 'toId' }, { '+': 'fromId' }, { '+': 'messageId' }, { '#': 'address' }]);

export const REPLY_TOPIC = new MqttTopic<ReplyParams>
  (['__reply', { '+': 'toId' }, { '+': 'fromId' }, { '+': 'messageId' }, { '+': 'sentMessageId' }]);

