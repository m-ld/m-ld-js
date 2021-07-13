/**
 * @module PeerParams
 * CAUTION: this module may be imported in server and routing modules
 */

// @see org.m_ld.json.MeldJacksonModule.NotificationDeserializer
export interface JsonNotification {
  next?: any;
  complete?: true;
  error?: any;
}

/** Parameters for sending, receiving and notifying a peer */
export interface PeerParams {
  toId: string;
  fromId: string;
}

/** Parameters for sending a single message */
export interface SendParams extends PeerParams {
  messageId: string;
}

/** Parameters for replying to a message */
export interface ReplyParams extends SendParams {
  sentMessageId: string;
}

/** Parameters for multiple messages on some channel */
export interface NotifyParams extends PeerParams {
  channelId: string;
}