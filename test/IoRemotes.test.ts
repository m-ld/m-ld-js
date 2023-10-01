import { IoRemotes } from '../src/socket.io';
import { MeldExtensions, noTransportSecurity } from '../src';
import { IoRemotesService } from '../src/socket.io/server';
import { createServer } from 'http';
import { Server as ServerIo } from 'socket.io';
import { AddressInfo } from 'net';
import { mockLocal, MockProcess } from './testClones';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { lastValueFrom, of } from 'rxjs';
import { mock } from 'jest-mock-extended';
import { Snapshot } from '../src/engine/index';

describe('Socket.io Remotes', () => {
  let serverIo: ServerIo;
  let localRemotes: IoRemotes;
  let domain: string;
  let port: number;
  const extensions = () => Promise.resolve(mock<MeldExtensions>({
    transportSecurity: noTransportSecurity
  }));

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
  });

  afterEach(async () => {
    await localRemotes?.close();
  });

  afterAll(done => {
    serverIo.close(done);
  });

  test('closes on middleware error', async () => {
    serverIo.use((socket, next) => {
      if (socket.handshake.auth?.bork)
        next(new Error('bork'));
      else
        next();
    });
    localRemotes = new IoRemotes({
      '@id': 'local-remotes', '@domain': domain, genesis: false,
      io: { uri: `http://localhost:${port}`, opts: { auth: { bork: true } } }
    }, extensions);
    await Promise.all([
      expect(lastValueFrom(localRemotes.operations))
        .rejects.toThrowError('bork'),
      // Also check outstanding new clock requests waiting for connection
      expect(localRemotes.snapshot(false, mock()))
        .rejects.toThrowError('bork')
    ]);
  });

  test('closes on explicit disconnect', async () => {
    localRemotes = new IoRemotes({
      '@id': 'local-remotes', '@domain': domain, genesis: false,
      io: { uri: `http://localhost:${port}` }
    }, extensions);
    await localRemotes.comesAlive(false);
    serverIo.disconnectSockets();
    await expect(lastValueFrom(localRemotes.operations))
      .rejects.toBe('io server disconnect');
  });

  test('connects after initial transport error', async () => {
    await new Promise(resolve => serverIo.close(resolve));
    await new Promise<void>(resolve => {
      localRemotes = new class extends IoRemotes {
        onDisconnect() {
          super.onDisconnect();
          resolve();
        }
      }({
        '@id': 'local-remotes', '@domain': domain, genesis: false,
        io: { uri: `http://localhost:${port}` }
      }, extensions);
    });
    serverIo.attach(port);
    await expect(localRemotes.comesAlive(false)).resolves.toBe(false);
  });

  test('reconnects after transport error', async () => {
    localRemotes = new IoRemotes({
      '@id': 'local-remotes', '@domain': domain, genesis: false,
      io: { uri: `http://localhost:${port}` }
    }, extensions);
    await expect(localRemotes.comesAlive(false)).resolves.toBe(false);
    serverIo.close();
    await expect(localRemotes.comesAlive(null)).resolves.toBeNull();
    serverIo.attach(port);
    await expect(localRemotes.comesAlive(false)).resolves.toBe(false);
  });

  describe('with presence', () => {
    let remoteRemotes: IoRemotes;

    beforeEach(() => {
      remoteRemotes = new IoRemotes({
        '@id': 'remote-remotes', '@domain': domain, genesis: true,
        io: { uri: `http://localhost:${port}` }
      }, extensions);
    });

    afterEach(async () => {
      await remoteRemotes.close();
    });

    test('remote connects', async () => {
      // live-ness of false means connected but no-one else around
      await expect(remoteRemotes.comesAlive(false)).resolves.toBe(false);
      // And check disconnection
      serverIo.disconnectSockets();
      await expect(remoteRemotes.comesAlive(null)).resolves.toBe(null);
    });

    test('comes alive with remote clone', async () => {
      // This tests presence
      localRemotes = new IoRemotes({
        '@id': 'local-remotes', '@domain': domain, genesis: false,
        io: { uri: `http://localhost:${port}` }
      }, extensions);
      const remoteClone = mockLocal();
      remoteRemotes.setLocal(remoteClone);
      await expect(localRemotes.comesAlive()).resolves.toBe(true);
      // Shut down the remote clone to check presence leaves
      remoteClone.liveSource.next(false);
      await expect(localRemotes.comesAlive(false)).resolves.toBe(false);
    });

    test('can get snapshot', async () => {
      // This tests sending and replying
      localRemotes = new IoRemotes({
        '@id': 'local-remotes', '@domain': domain, genesis: false,
        io: { uri: `http://localhost:${port}` }
      }, extensions);
      localRemotes.setLocal(mockLocal());
      const clock = TreeClock.GENESIS.forked().left;
      remoteRemotes.setLocal(mockLocal({
        snapshot: async () => mock<Snapshot>({
          clock, gwc: GlobalClock.GENESIS, agreed: TreeClock.GENESIS
        })
      }));
      await localRemotes.comesAlive();
      const { clock: newClock } = await localRemotes.snapshot(true, mock());
      expect(newClock!.equals(clock)).toBe(true);
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
          updates: of(remote.sentOperation({}, {})),
          cancel() {}
        })
      }));
      await localRemotes.comesAlive(); // Indicates that the remote is present
      const revup = await localRemotes.revupFrom(TreeClock.GENESIS.forked().right, mock());
      const op = await lastValueFrom(revup!.updates);
      expect(op.time.equals(remote.time)).toBe(true);
    });
  });
});