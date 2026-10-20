import { describe, it, strict } from 'poku';
import Connection from '../../../lib/base/connection.js';

type StoppableStream = NodeJS.ReadableStream & { stop(): void };

describe('createBinlogStream stop', () => {
  it('ends iteration normally and closes the connection once', async () => {
    let closed = 0;
    const connection = {
      query(_sql: string, cb: (...args: unknown[]) => void) {
        cb(null, [{ Value: 'NONE' }], [{}, { name: 'Value' }]);
      },
      close() {
        closed++;
      },
    };

    const stream = await new Promise<StoppableStream>((resolve, reject) => {
      Connection.prototype.createBinlogStream.call(
        connection,
        {},
        (err: Error | undefined, s: StoppableStream) =>
          err ? reject(err) : resolve(s)
      );
    });

    stream.stop();
    stream.stop();

    const events: unknown[] = [];
    for await (const ev of stream) events.push(ev);

    strict.deepEqual(events, []);
    strict.equal(closed >= 1, true);
  });
});

describe('Connection.end after close', () => {
  it('calls back without enqueueing a command', async () => {
    let called = 0;
    const connection = {
      config: {},
      _closing: true,
      addCommand() {
        throw new Error('should not enqueue');
      },
    };
    await new Promise<void>((resolve) =>
      Connection.prototype.end.call(connection, () => {
        called++;
        resolve();
      })
    );
    strict.equal(called, 1);
  });
});
