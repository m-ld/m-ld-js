import { MeldLocal, MeldRemotes, OperationMessage, Revup, Snapshot } from '../index';
import {
  BehaviorSubject, concatWith, defer, EMPTY, firstValueFrom, from, identity, NEVER, Observable,
  Observer, of, onErrorResumeNext, race, Subject as Source, Subscription, switchMap, throwError
} from 'rxjs';
import { TreeClock } from '../clocks';
import { generate as uuid } from 'short-uuid';
import {
  ControlMessage, NewClockRequest, NewClockResponse, RejectedResponse, Request, Response,
  RevupRequest, RevupResponse, SnapshotRequest, SnapshotResponse
} from './ControlMessage';
import { Future, MsgPack, Stopwatch, toJSON } from '../util';
import { delay, first, ignoreElements, map, reduce, tap, timeout, toArray } from 'rxjs/operators';
import { MeldError, MeldErrorStatus } from '../MeldError';
import { AbstractMeld } from '../AbstractMeld';
import { MeldExtensions, MeldReadState, noTransportSecurity, shortId } from '../../index';
import { JsonNotification, NotifyParams, ReplyParams, SendParams } from './PubsubParams';
import { consume } from 'rx-flowable/consume';
import { MeldMessageType } from '../../ns/m-ld';
import { MeldConfig } from '../../config';

/**
 * A sub-publisher, used to temporarily address unicast messages to one peer clone. Sub-publishers
 * are created on the demand of the {@link PubsubRemotes} base class, and are closed when no longer
 * required. During its lifetime, the sub-publisher will only address one other clone.
 * @see PubsubRemotes.sender
 * @see PubsubRemotes.replier
 * @see PubsubRemotes.notifier
 */
export interface SubPub {
  /**
   * Context-dependent identity of this sub-publisher, see usages for more information.
   */
  readonly id: string;
  /**
   * Unicast a message to the peer addressed by this sub-publisher.
   * @param msg the message payload to send
   */
  publish(msg: Buffer): Promise<unknown>;
  /**
   * Optional tidy-up of this sub-publisher.
   */
  close?(): void;
}

/** A m-ld ack is a reply to a reply with a null body */
const ACK = null;
type ACK = typeof ACK;
const ACK_PAYLOAD = MsgPack.encode(ACK);

/**
 * An abstract implementation of {@link MeldRemotes}, providing a common base for publish/subscribe
 * message transports able to broadcast moderate-sized binary payloads between nodes.
 *
 * In addition to message broadcast, the subclass must also be able to provide:
 * - A set of peer clone identities which are currently reachable ('present')
 * - Unicasting of a single message or a stream of messages to _one_ other identified peer clone
 *
 * These functions may make use of the publish/subscribe service if it has such capability, or by
 * any other means, as required.
 *
 * A subclass must:
 * 1. Implement the abstract methods to broadcast, unicast, and obtain peer presence
 * 1. Call the protected methods prefixed with "`on`" (`onConnect`, `onDisconnect`, `onNotify`,
 * `onPresenceChange`, `onOperation`, `onReply`, `onSent`) when the relevant transport state
 * change happens, or a message arrives
 */
export abstract class PubsubRemotes extends AbstractMeld implements MeldRemotes {
  private clone: MeldLocal | null = null;
  private cloneSubs = new Subscription;
  private readonly replyResolvers: {
    [messageId: string]: {
      received: Future<Response | ACK>,
      state: MeldReadState | null,
      readyToAck?: PromiseLike<void>
    }
  } = {};
  private readonly recentlySentTo: Set<string> = new Set;
  private readonly consuming: { [subPubId: string]: Observer<Buffer> } = {};
  private readonly sendTimeout: number;
  private readonly active = new BehaviorSubject<Promise<unknown> | undefined>(undefined);
  /**
   * This is separate to liveness because decided liveness requires presence,
   * which is discovered after connection. Connectedness is not equivalent to
   * decided-liveness.
   */
  private connected = new BehaviorSubject<boolean>(false);

  protected constructor(
    config: MeldConfig,
    private readonly extensions: () => Promise<MeldExtensions>
  ) {
    super(config);
    this.sendTimeout = config.networkTimeout ?? 5000;
  }

  private get transportSecurity() {
    return this.extensions().then(ext => ext.transportSecurity ?? noTransportSecurity);
  }

  setLocal(clone: MeldLocal | null) {
    if (clone == null) {
      if (this.clone != null)
        this.closeSafely();
      // Otherwise ignore re-set to null
    } else if (this.clone == null) {
      this.clone = clone;
      // Start sending updates from the local clone to the remotes
      this.cloneSubs.add(clone.operations.subscribe({
        next: async msg => {
          // If we are not connected, we just ignore updates.
          // They will be replayed from the clone's journal on re-connection.
          try {
            if (this.connected.value) {
              // Operation received from the local clone. Relay to the domain
              await this.publishOperation(msg.toBuffer());
            }
          } catch (err) {
            // Failed to send an update while (probably) connected
            this.log.warn('Failed to send an update while connected', err);
            // Operation delivery is guaranteed at-least-once. So, if it
            // fails, something catastrophic must have happened. Signal
            // failure of this service and allow the clone to deal with it.
            this.closeSafely(err);
          } finally {
            msg.delivered.resolve();
          }
        },
        // Local m-ld clone has stopped. It will no longer accept messages.
        complete: () => this.closeSafely(),
        // Local m-ld clone has stopped unexpectedly.
        // The application will already know, so just shut down gracefully.
        error: err => this.closeSafely(err)
      }));
      // When the clone comes live, join the presence on this domain if we can
      this.cloneSubs.add(clone.live.subscribe(live => {
        if (live != null)
          this.cloneLive(live).catch(err => this.log.warn('Failed to handle liveness', err));
      }));
    } else if (clone != this.clone) {
      throw new Error(`${this.id}: Local clone cannot change`);
    }
  }

  /**
   * Broadcasts a operation message to all clones on the domain, not including the local one
   * (messages **must not** be echoed).
   */
  protected abstract publishOperation(msg: Buffer): Promise<unknown>;

  /**
   * Retrieve a set of peer clone identities which are 'live' for recovery collaboration. A
   * returned identity may be used to establish a unicast channel via {@link sender} or
   * {@link notifier}.
   *
   * The subclass can choose whether to return the identities of _all_ other 'present' clones (who
   * have called {@link setPresent} locally), or some subset. To not include the identity of some
   * clone can prevent unwanted load, for example if the clone has restricted compute resources;
   * but this symmetrically increases the potential for load on clones that _are_ included. It may
   * also be most efficient to route all sent messages to some central clones, in a hub-and-spoke
   * architecture.
   *
   * Note that while the return is an observable, the full set should be available quickly,
   * comfortably with a single network timeout.
   * @see MeldConfig
   * @see Meld.live
   */
  protected abstract present(): Observable<string>;

  /**
   * Called to indicate whether the local clone is 'live' for recovery collaboration.
   *
   * The implementation can choose whether to actually make this clone available in other peers'
   * {@link present} sets.
   * @param present whether the local clone is available for recovery collaboration
   * @see Meld.live
   */
  protected abstract setPresent(present: boolean): Promise<unknown>;

  /**
   * Allocate a new {@link SubPub sub-publisher} to address another clone with a stream of
   * messages, via a notification 'channel'. Multiple channels may be allocated at the same time to
   * the same recipient.
   *
   * The {@link SubPub.id} of the returned object **must** equal the {@link NotifyParams.channelId}
   * of the parameter. TODO: refactor, make this warning unnecessary
   * @param params details of the sender, intended recipient, and channel
   */
  protected abstract notifier(params: NotifyParams): SubPub | Promise<SubPub>;

  /**
   * Allocate a new {@link SubPub sub-publisher} to address another clone with a single message,
   * which may incur a {@link replier reply}.
   *
   * The {@link SubPub.id} of the returned object **must** equal the {@link SendParams.toId}
   * of the parameter. TODO: refactor, make this warning unnecessary
   * @param params details of the sender, intended recipient and message identity
   */
  protected abstract sender(params: SendParams): SubPub | Promise<SubPub>;

  /**
   * Allocate a new {@link SubPub sub-publisher} to reply to a {@link sender sent} message.
   *
   * The {@link SubPub.id} of the returned object **must** equal the {@link SendParams.toId}
   * of the parameter. TODO: refactor, make this warning unnecessary
   * @param params details of the replier, sender, sent message identity and reply message identity
   */
  protected abstract replier(params: ReplyParams): SubPub | Promise<SubPub>;

  async close(err?: any) {
    if (err)
      this.log.info('Shutting down due to', err);
    else
      this.log.info('Shutting down normally');
    // This finalises the #updates, thereby notifying the clone (if present)
    super.close(err);
    // Wait until all activities have finalised
    try { // TODO unit test this
      this.active.complete();
      await this.active.value;
    } catch (e) {
      this.log.warn('Error while closing', e);
    }
  }

  private closeSafely(err?: any) {
    this.clone = null;
    this.cloneSubs.unsubscribe();
    this.cloneLive(false)
      .finally(() => this.close(err))
      .catch(err => this.log.warn('Error while closing safely', err));
  }

  async newClock(): Promise<TreeClock> {
    const sw = new Stopwatch('clock', shortId(4));
    const req = new NewClockRequest;
    const { res } = await this.send<NewClockResponse>(
      await this.wireRequest(req, null),
      { sw, state: null, logRequest: req });
    sw.stop();
    return res.clock;
  }

  async snapshot(state: MeldReadState): Promise<Snapshot> {
    const readyToAck = new Future;
    const sw = new Stopwatch('snapshot', shortId(4));
    const req = new SnapshotRequest;
    const { res, fromId } = await this.send<SnapshotResponse>(
      await this.wireRequest(req, state),
      { readyToAck, state, sw, logRequest: req });
    sw.next('consume');
    // Subscribe in parallel (subscription can be slow)
    const [data, updates] = await Promise.all([
      this.consume(fromId, res.dataAddress, MsgPack.decode, 'failIfSlow'),
      this.consume(fromId, res.updatesAddress, OperationMessage.fromBuffer)
    ]);
    sw.stop();
    return {
      gwc: res.gwc,
      agreed: res.agreed,
      data: defer(() => {
        // Ack the response to start the streams
        readyToAck.resolve();
        return data;
      }), updates
    };
  }

  async revupFrom(time: TreeClock, state: MeldReadState): Promise<Revup | undefined> {
    const readyToAck = new Future;
    const sw = new Stopwatch('revup', shortId(4));
    const req = new RevupRequest(time);
    const { res, fromId: from } = await this.send<RevupResponse>(
      await this.wireRequest(req, state), {
        // Try everyone until we find someone who can revup
        readyToAck, state, check: res => res.gwc != null, sw, logRequest: req
      });
    if (res.gwc != null) {
      sw.next('consume');
      const updates = await this.consume(
        from, res.updatesAddress, OperationMessage.fromBuffer, 'failIfSlow');
      sw.stop();
      return {
        gwc: res.gwc, updates: defer(() => {
          // Ack the response to start the streams
          readyToAck.resolve();
          return updates;
        })
      };
    } // else return undefined
    sw.stop();
  }

  /**
   * Call from the subclass when the transport is connected. Generally this indicates that the
   * network is available and the publish/subscribe service has been reached.
   */
  protected async onConnect() {
    this.connected.next(true);
    if (this.clone != null && this.clone.live.value === true)
      return this.cloneLive(true);
  }

  /**
   * Call from the subclass when the transport is disconnected. Generally this indicates that the
   * network is unavailable or the publish/subscribe service is not reachable.
   */
  protected onDisconnect() {
    this.connected.next(false);
    this.setLive(null);
  }

  /**
   * Call from the subclass when the set of present peer clone identities has changed, for example
   * because a peer has become present. This **must** also be called after connection, when the
   * presence set becomes available and {@link present} is callable without error.
   */
  protected onPresenceChange() {
    // Don't process a presence change until connected emits true
    this.connected.pipe(first(identity), tap(() => {
      // If there is more than just me present, we are live
      this.present()
        .pipe(reduce((live, id) => live || id !== this.id, false))
        .subscribe(live => this.setLive(live));
    })).subscribe();
  }

  /**
   * Call from the subclass when an operation message has arrived.
   * @param payload the operation message payload
   */
  protected onOperation(payload: Buffer) {
    const update = OperationMessage.fromBuffer(payload);
    if (update)
      this.nextOperation(update, 'remote');
    else
      // This is extremely bad - may indicate a bad actor
      this.log.error(new MeldError('Bad update'));
  }

  /**
   * Call from the subclass when a message has been sent intended for the local clone.
   * @param payload the sent message payload
   * @param sentParams details of the sender, recipient (always the local clone
   *   identity), and message identity
   * @see sender
   */
  protected async onSent(payload: Buffer, sentParams: SendParams) {
    // Ignore control messages before we have a clone
    if (this.clone) {
      // Keep track of long-running activity so that we can shut down cleanly
      const done = new Future, clone = this.clone;
      this.active.next(Promise.all([this.active.value, done]));
      const sw = new Stopwatch('reply', shortId(4));
      try {
        // The local state is required to prepare the response and to send it
        const replied = await clone.latch(async state => {
          try {
            // Unsecure the message if required
            const transportSecurity = await this.transportSecurity;
            const unwired = await transportSecurity.wire(
              payload, MeldMessageType.request, 'in', state);
            const req = Request.fromBuffer(unwired);
            // Verify the message if necessary
            await transportSecurity.verify?.(req.enc, req.attr, state);
            // Generate a suitable response
            if (req instanceof NewClockRequest) {
              sw.next('clock');
              const clock = await clone.newClock();
              sw.lap.next('send');
              return this.replyClock(sentParams, state, clock);
            } else if (req instanceof SnapshotRequest) {
              sw.next('snapshot');
              const snapshot = await clone.snapshot(state);
              sw.lap.next('send');
              return this.replySnapshot(sentParams, state, snapshot);
            } else {
              sw.next('revup');
              const revup = await clone.revupFrom(req.time, state);
              sw.lap.next('send');
              return this.replyRevup(sentParams, state, revup);
            }
          } catch (err) {
            // This will only catch from the work methods, not the returned replies
            this.reply(sentParams, state, new RejectedResponse(asMeldErrorStatus(err)))
              .catch(err => this.log.warn('Failed to handle sent message', err));
            throw err;
          }
        });
        if (typeof replied == 'object')
          await replied.after;
      } catch (err) {
        if (this.closed)
          this.log.info('Clone closed: Not responding to sent message');
        else
          // Rejection will already have been sent
          this.log.warn('Failed to respond to sent message', err);
      } finally {
        sw.stop();
        done.resolve();
      }
    }
  }

  /**
   * Call from the subclass when a reply arrives to a message {@link sender sent} by us.
   * @param payload the reply message payload
   * @param replyParams details of the sender, recipient (always the local clone
   *   identity), original sent message identity and reply message identity
   * @see replier
   */
  protected async onReply(payload: Buffer, replyParams: ReplyParams) {
    if (replyParams.sentMessageId in this.replyResolvers && this.clone != null) {
      const { received, state, readyToAck } = this.replyResolvers[replyParams.sentMessageId];
      try {
        const transportSecurity = await this.transportSecurity;
        const unwired = await transportSecurity.wire(
          payload, MeldMessageType.response, 'in', state);
        if (ACK_PAYLOAD.equals(unwired)) {
          received.resolve(ACK);
        } else {
          const res = Response.fromBuffer(unwired);
          // TODO: State for a request may be very old or non-existent.
          // Therefore we cannot verify a response here.
          // await this.transportSecurity.verify?.(res.enc, res.attr, state);
          received.resolve(res);
          if (readyToAck != null) {
            await readyToAck;
            await this.reply(replyParams, state, ACK);
          }
        }
      } catch (err) {
        received.reject(err);
      }
    }
  }

  /**
   * Call from the subclass when a single message arrives from a notification 'channel'.
   * @param channelId the notification channel identifier
   * @param payload the notified message payload
   * @see notifier
   */
  protected onNotify(channelId: string, payload: Buffer) {
    if (channelId in this.consuming) {
      const json = MsgPack.decode(payload);
      this.log.debug(`Notified ${Object.keys(json)[0]} on channel ${channelId}`);
      if (json.next)
        this.consuming[channelId].next(json.next);
      else if (json.complete)
        this.consuming[channelId].complete();
      else if (json.error)
        this.consuming[channelId].error(MeldError.from(json.error));
    }
  }

  private async cloneLive(live: boolean): Promise<unknown> {
    if (this.connected.value)
      return this.setPresent(live);
  }

  private wireRequest = async (ctrlMsg: ControlMessage, state: MeldReadState | null) => {
    // Sign the enclosed request
    const transportSecurity = await this.transportSecurity;
    ctrlMsg.attr = await transportSecurity.sign?.(ctrlMsg.enc, state) ?? null;
    return transportSecurity.wire(
      ctrlMsg.toBuffer(), MeldMessageType.request, 'out', state);
  };

  // Note this method is recursive and can be long-running, waiting for timeout
  @PubsubRemotes.checkNotClosed.async
  private async send<T extends Response>(
    wireRequest: Buffer,
    params: {
      logRequest: any;
      readyToAck?: PromiseLike<void>;
      state: MeldReadState | null;
      check?: (res: T) => boolean;
      sw: Stopwatch;
    },
    tried: { [address: string]: Promise<{ res: T, fromId: string }> } = {},
    messageId = uuid()
  ): Promise<{ res: T, fromId: string }> {
    const { logRequest, readyToAck, state, check, sw } = params;
    sw.next('sender');
    const sender = await this.nextSender(messageId);
    if (sender == null) {
      throw new MeldError('No visible clones',
        `No-one present on ${this.domain} to send message to`);
    } else if (sender.id in tried) {
      // If we have already tried this address, we've tried everyone; return
      // whatever the last response was.
      return tried[sender.id];
    }
    this.log.debug('Sending request', messageId, logRequest, sender.id);
    sw.next('send');
    const sent = sender.publish(wireRequest).finally(() => sender.close?.());
    tried[sender.id] = this.getResponse<T>(
      sent,
      messageId,
      { readyToAck, state }
    ).then(res => ({ res, fromId: sender.id }));
    // If the publish fails, don't keep trying other addresses
    await sent;
    // If the caller doesn't like this response, try again
    return tried[sender.id].then(rtn => check == null || check(rtn.res) ? rtn :
        this.send(wireRequest, params, tried, messageId),
      () => this.send(wireRequest, params, tried, messageId));
  }

  private getResponse<T extends Response | ACK>(sent: Promise<unknown>, messageId: string,
    { readyToAck, state, allowTimeFor }: {
      readyToAck?: PromiseLike<void>,
      state: MeldReadState | null,
      allowTimeFor?: Promise<unknown>
    }
  ): Promise<T> {
    return firstValueFrom(race(
      // Four possible outcomes:
      // 1. Response times out after allowing time for prior work
      from(allowTimeFor ?? of(0)).pipe(delay(this.sendTimeout),
        switchMap(() => throwError(() => {
          this.log.debug(`Message ${messageId} timed out.`);
          return new Error('Send timeout exceeded.');
        }))),
      // 2. Send fails - abandon the response if it rejects
      from(sent).pipe(switchMap(() => NEVER)),
      // 3. Remotes have closed
      this.active.pipe(ignoreElements(),
        concatWith(throwError(() => new MeldError('Clone has closed')))),
      // 4. Response received
      new Observable<T>(subs => {
        const received = new Future<Response | ACK>();
        received.then(res => {
          if (res instanceof RejectedResponse)
            subs.error(new MeldError(res.status));
          else
            subs.next(res as T);
        }, err => subs.error(err));
        this.replyResolvers[messageId] = { received, state, readyToAck };
        return () => { delete this.replyResolvers[messageId]; };
      })));
  }

  private async replyClock(sentParams: SendParams, state: MeldReadState, clock: TreeClock) {
    await this.reply(sentParams, state, new NewClockResponse(clock));
  }

  private async replySnapshot(sentParams: SendParams, state: MeldReadState, snapshot: Snapshot) {
    const { gwc, agreed, data, updates } = snapshot;
    const dataAddress = uuid(), updatesAddress = uuid();
    // Send the reply in parallel with establishing notifiers
    const replyId = uuid();
    const replied = this.reply(sentParams, state,
      new SnapshotResponse(gwc, agreed, dataAddress, updatesAddress), replyId);
    // Allow time for the notifiers to resolve while waiting for a reply
    const [dataNotifier, updatesNotifier] = await this.getAck(replied, replyId, state, Promise.all([
      this.notifier({ toId: sentParams.fromId, fromId: this.id, channelId: dataAddress }),
      this.notifier({ toId: sentParams.fromId, fromId: this.id, channelId: updatesAddress })
    ]));
    // Ack has been sent, start streaming the data and updates concurrently
    return {
      after: Promise.all([
        this.produce(data, dataNotifier, MsgPack.encode, 'snapshot'),
        this.produce(updates, updatesNotifier, msg => msg.toBuffer(), 'updates')
      ])
    };
  }

  private async replyRevup(sentParams: SendParams, state: MeldReadState, revup: Revup | undefined) {
    if (revup) {
      const updatesAddress = uuid();
      const replyId = uuid();
      const replied = this.reply(sentParams, state,
        new RevupResponse(revup.gwc, updatesAddress), replyId);
      const notifier = await this.getAck(replied, replyId, state, Promise.resolve(this.notifier({
        toId: sentParams.fromId, fromId: this.id, channelId: updatesAddress
      })));
      // Ack has been sent, start streaming the updates
      return {
        after: this.produce(
          revup.updates,
          notifier,
            msg => msg.toBuffer(), 'updates')
      };
    } else if (this.clone) {
      await this.reply(sentParams, state, new RevupResponse(null, this.id));
    }
  }

  private async consume<T>(
    fromId: string,
    channelId: string,
    datumFromPayload: (payload: Buffer) => T,
    failIfSlow?: 'failIfSlow'
  ): Promise<Observable<T>> {
    const notifier = await this.notifier({ fromId, toId: this.id, channelId });
    const src = this.consuming[channelId] = new Source;
    // src is multicast; subscribing to close the notifier
    onErrorResumeNext(src, EMPTY).subscribe({
      complete: () => {
        // TODO unit test this
        notifier.close?.();
        delete this.consuming[channelId];
      }
    });
    const consumed = src.pipe(map(datumFromPayload));
    // Rev-up and snapshot update channels are expected to be very fast, as they
    // are streamed directly from the dataset. So if there is a pause in the
    // delivery this probably indicates a failure e.g. the collaborator is dead.
    // TODO unit test this
    return failIfSlow ? consumed.pipe(timeout(this.sendTimeout)) : consumed;
  }

  private async produce<T>(
    data: Observable<T>,
    notifier: SubPub,
    datumToPayload: (datum: T) => Buffer,
    type: string
  ) {
    const notifyError = (error: any) => {
      this.log.warn('Notifying error on', notifier.id, error);
      return notify({ error: toJSON(error) });
    };
    const notifyComplete = () => {
      this.log.debug(`Completed production of ${type} on ${notifier.id}`);
      return notify({ complete: true });
    };
    const notify: (notification: JsonNotification) => Promise<unknown> = notification =>
      notifier.publish(MsgPack.encode(notification))
        // If notifications fail due to channel death, the recipient will find
        // out from the network so here we make best efforts to notify an error
        // and then give up.
        .catch(err => notifyError(err));
    return new Promise<void>((resolve, reject) => {
      // Convert the input into a consumable to get backpressure, if available
      consume(data).subscribe({
        next: ({ value, next }) =>
          notify({ next: datumToPayload(value) }).then(next),
        error: err => notifyError(err).then(() => reject(err)),
        complete: () => notifyComplete().then(resolve)
      });
    }).finally(() => notifier.close?.());
  }

  private async reply(
    { fromId: toId, messageId: sentMessageId }: SendParams,
    state: MeldReadState | null,
    res: Response | ACK, messageId = uuid()
  ): Promise<unknown> {
    const replier = await this.replier({ fromId: this.id, toId, messageId, sentMessageId });
    this.log.debug('Replying response', messageId, 'to', sentMessageId, res, replier.id);
    const transportSecurity = await this.transportSecurity;
    let payload: Buffer;
    if (res == ACK) {
      payload = ACK_PAYLOAD;
    } else {
      // Sign the enclosed request
      res.attr = await transportSecurity.sign?.(res.enc, state) ?? null;
      payload = res.toBuffer();
    }
    const wire = await transportSecurity.wire(
      payload, MeldMessageType.response, 'out', state);
    return replier.publish(wire).finally(() => replier.close?.());
  }

  private async getAck<T>(
    replied: Promise<unknown>,
    messageId: string,
    state: MeldReadState | null,
    allowTimeFor: Promise<T>
  ): Promise<T> {
    return this.getResponse<ACK>(
      replied, messageId, { allowTimeFor, state })
      .then(() => allowTimeFor); // This just gets the return value
  }

  private async nextSender(messageId: string): Promise<SubPub | null> {
    const present = await firstValueFrom(this.present().pipe(toArray()));
    if (present.every(id => this.recentlySentTo.has(id)))
      this.recentlySentTo.clear();

    this.recentlySentTo.add(this.id);
    const toId = present.filter(id => !this.recentlySentTo.has(id))[0];
    if (toId != null) {
      this.recentlySentTo.add(toId);
      return this.sender({ fromId: this.id, toId, messageId });
    }
    return null;
  }
}

function asMeldErrorStatus(err: any): MeldErrorStatus {
  return err instanceof MeldError ? err.status : MeldErrorStatus['Request rejected'];
}