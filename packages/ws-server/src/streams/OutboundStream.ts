import {
  RejectChanges,
  bytesToHex,
  tags,
  uintArraysEqual,
} from "@vlcn.io/ws-common";
import type { IDB } from "../DB.js";
import type Transport from "../Trasnport.js";
import { logger } from "@vlcn.io/logger-provider";

/**
 * Listens to the local db and sends out a stream
 * of changes over the transport.
 */
export default class OutboundStream {
  readonly #db;
  readonly #transport;
  readonly #to: Uint8Array;
  #disposer: (() => void) | null = null;
  #closed = false;
  #lastSent: readonly [bigint, number];
  #bufferFullBackoff = 50;
  #timeoutHandle: ReturnType<typeof setTimeout> | null = null;

  constructor(
    transport: Transport,
    db: IDB,
    lastSeenByClient: readonly [Uint8Array, [bigint, number]][],
    clientDbId: Uint8Array
  ) {
    logger.info(
      `Starting outbound stream from ${bytesToHex(db.siteId)} to ${bytesToHex(
        clientDbId
      )}`
    );
    this.#transport = transport;
    this.#db = db;
    this.#to = clientDbId;
    const lastSent = lastSeenByClient.find((v) =>
      uintArraysEqual(v[0], db.siteId)
    );
    this.#lastSent = (lastSent && lastSent[1]) || [0n, 0];
    if (lastSent == null) {
      logger.info(
        `Unable to find existing last sent for ${bytesToHex(db.siteId)}`
      );
    }
  }

  start() {
    if (this.#closed) {
      throw new Error(`Illegal state -- OutboundStream has been closed`);
    }
    this.#disposer = this.#db.onChange(this.#dbChanged);
    // Initial kickoff. Deliberately NOT via #dbChanged: ConnectionBroker wraps
    // start() and reports a failed handshake to the client, so a fault here must
    // still propagate. Only the callback paths (change listener, buffer-full
    // retry) are isolated, because those have no caller to catch them.
    this.#pumpChanges();
  }

  reset(msg: RejectChanges) {
    // The peer rejected our changes: they detected a gap because the `since`
    // we sent was ahead of what they have actually applied (e.g. a previous
    // batch failed to apply or was lost mid-connection). Rewind our cursor to
    // the version they report and re-send from there. Without this, the gap is
    // never re-delivered and the peer silently diverges forever.
    this.#lastSent = msg.since;
    this.#dbChanged();
  }

  // db change notifications are already throttled for us in `DB.ts`
  // but we also apply some backpressure if the outbound buffer is full.
  //
  // This runs as a callback: from the db's change listener, and from our own
  // buffer-full setTimeout. A throw here therefore has no caller to catch it and
  // takes down the entire process -- every room and every other client -- for
  // what is a fault in ONE stream. So the body is wrapped and a failure stops
  // just this stream; the peer sees the connection drop and reconnects.
  #dbChanged = () => {
    try {
      this.#pumpChanges();
    } catch (e) {
      const reason = e instanceof Error ? e.message : String(e);
      logger.error(
        `OutboundStream to ${bytesToHex(
          this.#to
        )} failed (${reason}); stopping this stream instead of crashing the server`
      );
      this.stop();
    }
  };

  #pumpChanges() {
    logger.info(`OutboundStream got a db change event`);
    // A disposed listener can still be invoked: the db's change notification is
    // delivered via setTimeout, so a callback scheduled before stop() still runs
    // afterwards -- against a db the cache may already have closed.
    if (this.#closed) {
      return;
    }
    if (this.#timeoutHandle != null) {
      clearTimeout(this.#timeoutHandle);
      this.#timeoutHandle = null;
    }
    // #to to ignore changes from self.
    const changes = this.#db.pullChangeset(this.#lastSent, this.#to);
    if (changes.length == 0) {
      return;
    }

    const lastChange = changes[changes.length - 1];
    const since = this.#lastSent;
    this.#lastSent = [lastChange[5], 0] as const;

    try {
      const didSend = this.#transport.sendChanges({
        _tag: tags.Changes,
        changes,
        sender: this.#db.siteId,
        since,
      });
      switch (didSend) {
        case "sent":
          this.#bufferFullBackoff = 50;
          break;
        case "buffer-full":
          this.#lastSent = since;
          this.#timeoutHandle = setTimeout(
            this.#dbChanged,
            (this.#bufferFullBackoff = Math.max(
              this.#bufferFullBackoff * 2,
              1000
            ))
          );
          break;
      }
    } catch (e) {
      this.#lastSent = since;
      throw e;
    }
  }

  stop() {
    if (this.#disposer) {
      this.#disposer();
      this.#disposer = null;
    }
    // Without this, a pending buffer-full retry fires after the stream is gone.
    if (this.#timeoutHandle != null) {
      clearTimeout(this.#timeoutHandle);
      this.#timeoutHandle = null;
    }
    this.#closed = true;
  }
}
