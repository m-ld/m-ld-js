import { IoRemotes } from '../src/socket.io';
import { IoRemotesService } from '../src/socket.io/server';
import { createServer } from 'http';
import { Server as ServerIo } from 'socket.io';
import { AddressInfo } from 'net';
import { comesAlive } from '../src/engine/AbstractMeld';
import { mockLocal, MockProcess } from './testClones';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { lastValueFrom, of } from 'rxjs';

describe('Socket.io Remotes', () => {
  let serverIo: ServerIo;
  let remoteRemotes: IoRemotes;
  let domain: string;
  let port: number;

  beforeAll((done) => {
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
    domain = `${(expect.getState().currentTestName.replace(/[^\w]/g, ''))}.m-ld.org`;
    remoteRemotes = new IoRemotes({
      '@id': 'remote-remotes', '@domain': domain, genesis: true,
      io: { uri: `http://localhost:${port}` }
    });
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
    const localRemotes = new IoRemotes({
      '@id': 'local-remotes', '@domain': domain, genesis: false,
      io: { uri: `http://localhost:${port}` }
    });
    const remoteClone = mockLocal();
    remoteRemotes.setLocal(remoteClone);
    await expect(comesAlive(localRemotes)).resolves.toBe(true);
    // Shut down the remote clone to check presence leaves
    remoteClone.liveSource.next(false);
    await expect(comesAlive(localRemotes, false)).resolves.toBe(false);
  });

  test('can get clock', async () => {
    // This tests sending and replying
    const localRemotes = new IoRemotes({
      '@id': 'local-remotes', '@domain': domain, genesis: false,
      io: { uri: `http://localhost:${port}` }
    });
    const clock = TreeClock.GENESIS.forked().left;
    remoteRemotes.setLocal(mockLocal({
      newClock: async () => clock
    }));
    const newClock = await localRemotes.newClock();
    expect(newClock.equals(clock)).toBe(true);
  });

  test('can rev-up', async () => {
    // This tests notification channels
    const localRemotes = new IoRemotes({
      '@id': 'local-remotes', '@domain': domain, genesis: false,
      io: { uri: `http://localhost:${port}` }
    });
    const remote = new MockProcess(TreeClock.GENESIS.forked().right);
    remoteRemotes.setLocal(mockLocal({
      revupFrom: async () => ({
        gwc: GlobalClock.GENESIS,
        updates: of(remote.sentOperation('{}', '{}'))
      })
    }));
    const revup = await localRemotes.revupFrom(TreeClock.GENESIS.forked().right);
    const op = await lastValueFrom(revup!.updates);
    expect(op.time.equals(remote.time)).toBe(true);
  });

  afterEach(async () => {
    await remoteRemotes.close();
  });

  afterAll(() => {
    serverIo.close();
  });

});