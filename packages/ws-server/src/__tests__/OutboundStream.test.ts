import { test, expect } from "vitest";
import { tags } from "@vlcn.io/ws-common";
import type { RejectChanges } from "@vlcn.io/ws-common";
import OutboundStream from "../streams/OutboundStream.js";
import type { IDB } from "../DB.js";
import type Transport from "../Trasnport.js";

// A crsql_changes row tuple: [table, pk, cid, val, col_version, db_version, site_id, cl, seq].
// OutboundStream only cares about index 5 (db_version).
function change(dbVersion: bigint): any {
  return ["item", new Uint8Array([1]), "v", null, 1n, dbVersion, null, 1n, 0];
}

// Minimal IDB: OutboundStream only uses siteId, onChange, pullChangeset.
function fakeDb(allChanges: any[]): IDB {
  return {
    siteId: new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]),
    onChange: (_cb: () => void) => () => {},
    pullChangeset: (since: readonly [bigint, number], _excludeSite: Uint8Array) =>
      allChanges.filter((c) => (c[5] as bigint) > since[0]),
  } as unknown as IDB;
}

function recordingTransport(): { transport: Transport; sent: any[] } {
  const sent: any[] = [];
  const transport = {
    sendChanges: (msg: any) => {
      sent.push(msg);
      return "sent" as const;
    },
    rejectChanges: () => {},
    startStreaming: () => {},
  } as unknown as Transport;
  return { transport, sent };
}

const clientId = new Uint8Array([0x01, 0x02, 0x03, 0x04]);

test("reset() rewinds the cursor and re-sends after a peer rejects a gap", () => {
  const db = fakeDb([change(1n), change(2n), change(3n)]);
  const { transport, sent } = recordingTransport();

  const stream = new OutboundStream(transport, db, [], clientId);
  stream.start(); // initial kickoff sends everything, advancing the cursor to db_version 3

  expect(sent).toHaveLength(1);
  expect(sent[0].since).toEqual([0n, 0]);
  expect(sent[0].changes.map((c: any) => c[5])).toEqual([1n, 2n, 3n]);

  // The peer only managed to apply up to db_version 1 (batch 2/3 failed or was lost),
  // so it rejects and asks us to resume from [1, 0].
  const rejection: RejectChanges = {
    _tag: tags.RejectChanges,
    whose: db.siteId,
    since: [1n, 0],
  };
  stream.reset(rejection);

  // The server must rewind to [1, 0] and re-send the changes the peer is missing (2 and 3).
  // Before the fix, reset() was a no-op and this second send never happened — the peer
  // silently lost db_version 2 and 3 forever.
  expect(sent).toHaveLength(2);
  expect(sent[1].since).toEqual([1n, 0]);
  expect(sent[1].changes.map((c: any) => c[5])).toEqual([2n, 3n]);
});

// A db whose change-listener we can fire by hand, and whose pullChangeset can be
// made to fail on demand.
function controllableDb(): {
  db: IDB;
  fireChange: () => void;
  isSubscribed: () => boolean;
  failWith: (err: Error | null) => void;
  pullCount: () => number;
} {
  // `captured` deliberately survives disposal: the real FSNotify delivers change
  // events through setTimeout, so a callback scheduled before stop() still fires
  // after the listener has been removed. That is the case we need to exercise.
  let captured: (() => void) | null = null;
  let subscribed = false;
  let err: Error | null = null;
  let pulls = 0;
  const db = {
    siteId: new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]),
    onChange: (fn: () => void) => {
      captured = fn;
      subscribed = true;
      return () => {
        subscribed = false;
      };
    },
    pullChangeset: () => {
      pulls++;
      if (err) throw err;
      return [];
    },
  } as unknown as IDB;
  return {
    db,
    fireChange: () => captured?.(),
    isSubscribed: () => subscribed,
    failWith: (e: Error | null) => {
      err = e;
    },
    pullCount: () => pulls,
  };
}

test("a failing pullChangeset in the change callback does not escape (would kill the process)", () => {
  const { db, fireChange, failWith, isSubscribed } = controllableDb();
  const { transport } = recordingTransport();

  const stream = new OutboundStream(transport, db, [], clientId);
  stream.start();

  // The db goes bad after the handshake, as it does when the cache closes it
  // underneath a live stream.
  failWith(new Error("SQL logic error"));

  // Before this fix the throw propagated out of the listener/timer callback,
  // where nothing catches it, and took the whole sync server down.
  expect(() => fireChange()).not.toThrow();
  // and it takes itself out of service rather than failing on every future event
  expect(isSubscribed()).toBe(false);
});

test("start() still propagates, so the broker can report a failed handshake", () => {
  const { db, failWith } = controllableDb();
  const { transport } = recordingTransport();
  failWith(new Error("SQL logic error"));

  const stream = new OutboundStream(transport, db, [], clientId);
  expect(() => stream.start()).toThrow(/SQL logic error/);
});

test("a change callback that arrives after stop() does not touch the db", () => {
  const { db, fireChange, pullCount } = controllableDb();
  const { transport } = recordingTransport();

  const stream = new OutboundStream(transport, db, [], clientId);
  stream.start();
  const afterStart = pullCount();

  stream.stop();
  // The db delivers change notifications via setTimeout, so one can still land
  // after the stream was disposed.
  fireChange();

  expect(pullCount()).toBe(afterStart);
});
