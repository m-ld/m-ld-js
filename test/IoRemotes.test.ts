import { IoRemotes } from '../src/socket.io';
import { IoRemotesService } from '../src/socket.io/server';
import { createServer } from 'http';
import { Server as ServerIo } from 'socket.io';
import { AddressInfo } from 'net';
import { comesAlive } from '../src/engine/AbstractMeld';
import { mockLocal, MockProcess } from './testClones';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { lastValueFrom, of } from 'rxjs';
import { mock } from 'jest-mock-extended';

describe('Socket.io Remotes', () => {
  let serverIo: ServerIo;
  let remoteRemotes: IoRemotes;
  let localRemotes: IoRemotes;
  let domain: string;
  let port: number;
  const extensions = () => Promise.resolve({});

  beforeAll(done => {
    const server = createServer();
    serverIo = new ServerIo(server);
    new IoRemotesService(serverIo.sockets)
      .on('error', console.error)
      .on('debug', console.debug);

    server.listen(() => {
      port = (<AddressInfo>server.address()).port;
      done();
    });
  });

  beforeEach(() => {
    domain = `${(expect.getState().currentTestName?.replace(/\W/g, ''))}.m-ld.org`;
    remoteRemotes = new IoRemotes({
      '@id': 'remote-remotes', '@domain': domain, genesis: true,
      io: { uri: `http://localhost:${port}` }
    }, extensions);
  });

  test('remote connects', async () => {
    // live-ness of false means connected but no-one else around
    await expect(comesAlive(remoteRemotes, false)).resolves.toBe(false);
    // And check disconnection
    serverIo.disconnectSockets();
    await expect(comesAlive(remoteRemotes, null)).resolves.toBe(null);
  });

  test('comes alive with remote clone', async () => {
    // This tests presence
    localRemotes = new IoRemotes({
      '@id': 'local-remotes', '@domain': domain, genesis: false,
      io: { uri: `http://localhost:${port}` }
    }, extensions);
    const remoteClone = mockLocal();
    remoteRemotes.setLocal(remoteClone);
    await expect(comesAlive(localRemotes)).resolves.toBe(true);
    // Shut down the remote clone to check presence leaves
    remoteClone.liveSource.next(false);
    await expect(comesAlive(localRemotes, false)).resolves.toBe(false);
  });

  test('can get clock', async () => {
    // This tests sending and replying
    localRemotes = new IoRemotes({
      '@id': 'local-remotes', '@domain': domain, genesis: false,
      io: { uri: `http://localhost:${port}` }
    }, extensions);
    localRemotes.setLocal(mockLocal());
    const clock = TreeClock.GENESIS.forked().left;
    remoteRemotes.setLocal(mockLocal({
      newClock: async () => clock
    }));
    await comesAlive(localRemotes);
    const newClock = await localRemotes.newClock();
    expect(newClock.equals(clock)).toBe(true);
  });

  test('can rev-up', async () => {
    // This tests notification channels
    localRemotes = new IoRemotes({
      '@id': 'local-remotes', '@domain': domain, genesis: false,
      io: { uri: `http://localhost:${port}` }
    }, extensions);
    localRemotes.setLocal(mockLocal());
    const remote = new MockProcess(TreeClock.GENESIS.forked().right);
    remoteRemotes.setLocal(mockLocal({
      revupFrom: async () => ({
        gwc: GlobalClock.GENESIS,
        updates: of(remote.sentOperation({}, {}))
      })
    }));
    await comesAlive(localRemotes); // Indicates that the remote is present
    const revup = await localRemotes.revupFrom(TreeClock.GENESIS.forked().right, mock());
    const op = await lastValueFrom(revup!.updates);
    expect(op.time.equals(remote.time)).toBe(true);
  });

  afterEach(async () => {
    await localRemotes?.close();
    await remoteRemotes.close();
  });

  afterAll(done => {
    serverIo.close(done);
  });
});