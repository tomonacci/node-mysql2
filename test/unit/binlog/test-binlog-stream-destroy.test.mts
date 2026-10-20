import { describe, it, strict } from 'poku';
import Connection from '../../../lib/base/connection.js';

describe('createBinlogStream', () => {
  it('closes the connection when the stream is destroyed', async () => {
    let closed = 0;
    const connection = {
      query(_sql: string, cb: (...args: unknown[]) => void) {
        cb(null, [{ Value: 'NONE' }], [{}, { name: 'Value' }]);
      },
      close() {
        closed++;
      },
    };

    const stream = await new Promise<NodeJS.ReadableStream>(
      (resolve, reject) => {
        Connection.prototype.createBinlogStream.call(
          connection,
          {},
          (err: Error | undefined, s: NodeJS.ReadableStream) =>
            err ? reject(err) : resolve(s)
        );
      }
    );

    // @ts-expect-error: internal access
    stream.destroy();

    strict.equal(closed, 1);
  });
});
