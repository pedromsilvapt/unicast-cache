import test from 'tape';
import sinon from 'sinon';
import { Readable } from 'stream';
import { IRange } from 'data-pieces';
import { ConsoleBackend, Logger } from 'clui-logger';
import { TIMESTAMP_SHORT } from 'clui-logger/lib/Backends/ConsoleBackend';
import { MemoryRAS } from './RandomAccessStorage';
import { InputTransmission, OutputTransmission, ReplicatedResource, TransmissionKind } from './ReplicatedResource';

// Configure Logger (can be enabled in the tests to aid debugging)
const logger = new Logger(new ConsoleBackend(TIMESTAMP_SHORT));

test('ReplicatedResource', t => {
    // 5 As, 5 Bs, ..., 5 Es, 4 Fs
    const source = 'AAAAABBBBBCCCCCDDDDDEEEEEFFFF';

    t.test('#getPiecesBetween', t => {
        const resource = makeDummyResource(/* totalSize */ 20, /* pieceSize */ 5);

        // Convert between ranges of bytes, and ranges of pieces. This conversion will depend on the
        // piece size. In this case, it is 5. The ranges here are open, meaning the ending byte (or piece)
        // is not included in the actual range.

        // So, a byte range of [0, 5[ will actually only include the bytes
        // 0, 1, 2, 3 and 4. That is why the corresponding piece range is [0, 1[,
        // in which case, the pieces included in that range are only 0. Indeed, since the piece size if
        // 5, it checks out that all bytes between 0 and 4, included, are indeed all inside piece 0.

        // Only if we needed to include another byte, for example for byte range [0, 6[, we would have the
        // corresponding piece range [0, 2[. In this scenario, the pieces 0 and 1 are included in the range.
        // From piece index number 2 we only need one byte, number 5, but we need the whole piece none the less.

        t.deepEquals(resource.getPiecesBetween(0, 4), { start: 0, end: 1 });
        t.deepEquals(resource.getPiecesBetween(0, 5), { start: 0, end: 1 });
        t.deepEquals(resource.getPiecesBetween(0, 9), { start: 0, end: 2 });
        t.deepEquals(resource.getPiecesBetween(0, 10), { start: 0, end: 2 });
        t.deepEquals(resource.getPiecesBetween(0, 11), { start: 0, end: 3 });
        t.end();
    });

    t.test('#createReadableStream', t => {
        t.test('single output stream for complete content', async t => {
            const ras = new MemoryRAS();

            // Prepare the readable stream
            const streamFactoryFirst = (range?: IRange) => {
                const { readable, push, end } = makeMockStream(source);

                // Mock the stream stub method
                const readStub = sinon.stub(readable, '_read');
                readStub.onCall(0).callsFake(_ => push(5));
                readStub.onCall(1).callsFake(_ => push(5));
                readStub.onCall(2).callsFake(_ => push(10));
                readStub.onCall(3).callsFake(_ => push(3));
                readStub.onCall(4).callsFake(_ => push(6));
                readStub.onCall(5).callsFake(_ => end());

                return readable;
            };

            const streamFactory = sinon.stub<[IRange], Readable>();
            streamFactory.onCall(0).callsFake(streamFactoryFirst);

            const resource = new ReplicatedResource(streamFactory, () => ras, 29, 5);
            // resource.logger = logger;

            const stream = resource.createReadableStream();

            const streamResult = await drainStream(stream, 'utf8');
            t.equals(streamResult, source, "Stream result should be equal to source string");

            const cacheResult = ras.buffer.toString('utf8');
            t.equals(cacheResult, source, "Cache result should be equal to source string");
        });

        t.test('single output stream for partial content (first two pieces)', async t => {
            const ras = new MemoryRAS();

            // Prepare the readable stream
            const streamFactoryFirst = (range?: IRange) => {
                const { readable, push } = makeMockStream(source);

                // Mock the stream stub method
                const readStub = sinon.stub(readable, '_read');
                readStub.onCall(0).callsFake(_ => push(5));
                readStub.onCall(1).callsFake(_ => push(5));
                readStub.onCall(2).callsFake(_ => { });

                return readable;
            };

            const streamFactory = sinon.stub<[IRange], Readable>();
            streamFactory.onCall(0).callsFake(streamFactoryFirst);

            const resource = new ReplicatedResource(streamFactory, () => ras, 20, 5);
            // resource.logger = logger;

            const stream = resource.createReadableStream({ start: 0, end: 10 });

            // Get the first 10 bytes from the string
            const expected = Buffer.from(source).subarray(0, 10).toString('utf8');

            const streamResult = await drainStream(stream, 'utf8');
            t.equals(streamResult, expected, "Stream result should be equal to source string");

            const cacheResult = ras.buffer.toString('utf8');
            t.equals(cacheResult, expected, "Cache result should be equal to source string");

            t.equals(streamFactory.callCount, 1, "source stream factory should have been called once.");
            t.equals(streamFactory.getCall(0).returnValue.destroyed, true, "readable should have been destroyed");
        });

        t.test('single output stream for partial content (part of the first two pieces)', async t => {
            const ras = new MemoryRAS();

            // Prepare the readable stream
            const streamFactoryFirst = (range?: IRange) => {
                const { readable, push } = makeMockStream(source);

                // Mock the stream read method
                const readStub = sinon.stub(readable, '_read');
                readStub.onCall(0).callsFake(_ => push(3));
                readStub.onCall(1).callsFake(_ => push(3));
                readStub.onCall(2).callsFake(_ => push(4));
                // Stall the mock source stream
                readStub.onCall(3).callsFake(_ => { });

                return readable;
            };

            const streamFactory = sinon.stub<[IRange], Readable>();
            streamFactory.onCall(0).callsFake(streamFactoryFirst);

            const resource = new ReplicatedResource(streamFactory, () => ras, 20, 5);
            // resource.logger = logger;

            const stream = resource.createReadableStream({ start: 3, end: 9 });

            // Get the first 10 bytes from the string
            const expectedStream = Buffer.from(source).subarray(3, 9).toString('utf8');
            const streamResult = await drainStream(stream, 'utf8');
            t.equals(streamResult, expectedStream, "Stream result should be equal to source string");

            const expectedCache = Buffer.from(source).subarray(0, 10).toString('utf8');
            const cacheResult = ras.buffer.toString('utf8');
            t.equals(cacheResult, expectedCache, "Cache result should be equal to source string");

            t.equals(streamFactory.getCall(0).returnValue.destroyed, true, "readable should have been destroyed");
        });

        t.test('single output stream with unaligned last chunks from source', async t => {
            // This test simulates the case where the last piece is smaller than all other pieces,
            // and the source stream serves that last piece in multiple chunks instead of just one.
            // For example, for piece size of 5, and a file with 14 bytes, we have three pieces, each with length:
            // [5, 5, 4]
            // If the source stream provides the chunks with the following lengths in this order:
            // [5, 6, 3]
            // Then we can cache the first piece right away, cache the second piece as well, but we are left
            // with one byte of the next piece, and we cannot write it right away because the piece is not complete.
            // As such we save it in a buffer (with max size 5 bytes, the piece size) and wait for that buffer
            // to be filled. But it would never be filled, because the last piece is smaller than all other pieces, and
            // as such we would never "flush" that temporary buffer and write the last piece to the cache.
            // This bug was fixed, so that instead of looking at the buffer size and waiting for it to be filled,
            // we look at the piece index that the buffer is currently holding, and use that to determine the
            // expected piece size (is always the same except for the last piece, where it may be lower).
            // And then we compare the bytes we received with that piece size value, instead of with the buffer
            // size, and with that solution, we can correctly flush the buffer.

            const ras = new MemoryRAS();

            // Prepare the readable stream
            const streamFactoryFirst = (range?: IRange) => {
                const { readable, push, end } = makeMockStream(source);

                // Mock the stream read method
                const readStub = sinon.stub(readable, '_read');
                readStub.onCall(0).callsFake(_ => push(5));
                readStub.onCall(1).callsFake(_ => push(6));
                readStub.onCall(2).callsFake(_ => push(3));
                readStub.onCall(3).callsFake(_ => end());

                return readable;
            };

            const streamFactory = sinon.stub<[IRange], Readable>();
            streamFactory.onCall(0).callsFake(streamFactoryFirst);

            const resource = new ReplicatedResource(streamFactory, () => ras, 14, 5);
            // resource.logger = logger;

            const stream = resource.createReadableStream();

            // Get the first 14 bytes from the string
            const expected = Buffer.from(source).subarray(0, 14).toString('utf8');

            const streamResult = await drainStream(stream, 'utf8');
            t.equals(streamResult, expected, "Stream result should be equal to source string");

            const cacheResult = ras.buffer.toString('utf8');
            t.equals(cacheResult, expected, "Cache result should be equal to source string");

            t.equals(streamFactory.getCall(0).returnValue.destroyed, true, "readable should have been destroyed");
        });

        t.test('two output transmissions sharing the same input transmission', async t => {
            // This test simulates two output transmissions that overlap at the start,
            // and as such share the same input transmission for a while. After the input
            // transmission ends, a new one is created just for the second output

            const ras = new MemoryRAS();

            // Prepare the readable stream
            const streamFactoryFirst = (range?: IRange) => {
                const { readable, push, end } = makeMockStream(source);

                // Mock the stream read method
                const readStub = sinon.stub(readable, '_read');
                readStub.onCall(0).callsFake(_ => push(10));
                readStub.onCall(1).callsFake(_ => end());

                return readable;
            };
            const streamFactorySecond = (range?: IRange) => {
                const { readable, skip, push, end } = makeMockStream(source);

                skip(10);

                // Mock the stream read method
                const readStub = sinon.stub(readable, '_read');
                readStub.onCall(0).callsFake(_ => push(5));
                readStub.onCall(1).callsFake(_ => end());

                return readable;
            };

            const streamFactory = sinon.stub<[IRange], Readable>();
            streamFactory.onCall(0).callsFake(streamFactoryFirst);
            streamFactory.onCall(1).callsFake(streamFactorySecond);

            const resource = new ReplicatedResource(streamFactory, () => ras, 15, 5);
            // resource.logger = logger;

            const stream1 = resource.createReadableStream({ start: 0, end: 9 });
            const stream2 = resource.createReadableStream({ start: 0, end: 15 });

            const outputTransmissions = Array.from(resource.activeTransmissions)
                .filter(tr => tr.kind === TransmissionKind.Output) as OutputTransmission[];

            const inputTransmissions = Array.from(resource.activeTransmissions)
                .filter(tr => tr.kind === TransmissionKind.Input) as InputTransmission[];

            t.equals(outputTransmissions.length, 2, "should have two output transmissions");
            t.equals(inputTransmissions.length, 1, "should have one input transmission");
            t.notLooseEqual(outputTransmissions[0].dependency, null, "both outputs should depend on the same input");
            t.notLooseEqual(outputTransmissions[1].dependency, null, "both outputs should depend on the same input");
            t.strictEquals(outputTransmissions[0].dependency, outputTransmissions[1].dependency, "both outputs should depend on the same input");

            const stream1Result = await drainStream(stream1, 'utf8');
            const stream2Result = await drainStream(stream2, 'utf8');

            const expected1 = Buffer.from(source).subarray(0, 9).toString('utf8');
            t.equals(stream1Result, expected1, "Stream 1 result should be equal to source string");

            const expected2 = Buffer.from(source).subarray(0, 15).toString('utf8');
            t.equals(stream2Result, expected2, "Stream 2 result should be equal to source string");

            t.equals(streamFactory.callCount, 2, "should have callend source stream factory twice");
            t.deepLooseEqual(streamFactory.getCall(0).args[0], { start: 0, end: 10 }, "first stream should have byte range 0-10");
            t.deepLooseEqual(streamFactory.getCall(1).args[0], { start: 10, end: 15 }, "second stream should have byte range 10-15");
        });
    });
});

function makeMockStream (source: string) {
    const readable = new Readable();

    let pushedCount: number = 0;

    const sourceBuffer = Buffer.from(source, 'utf8');

    const skip = (length: number) => {
        pushedCount += length;
    };
    const pushRange = (start: number, length: number) => {
        readable.push(Buffer.from(source.substring(start, start + length), 'utf8'));
    };
    const push = (length: number) => {
        readable.push(sourceBuffer.subarray(pushedCount, pushedCount + length));
        pushedCount += length;
    };
    const end = () => readable.push(null);

    return { readable, skip, pushRange, push, end };
}

function makeDummyResource (totalSize: number, pieceSize: number) {
    const resource = new ReplicatedResource(null as any, null as any, totalSize, pieceSize);

    return resource;
}

function drainStream (stream: Readable): Promise<Buffer>;
function drainStream (stream: Readable, encoding: BufferEncoding): Promise<string>;
function drainStream (stream: Readable, encoding: BufferEncoding | null = null): Promise<Buffer | string> {
    return new Promise<Buffer | string>((resolve, reject) => {
        const chunks: Buffer[] = [];

        stream.on('data', chunk => chunks.push(chunk));
        stream.on('error', err => reject(err));
        if (encoding == null) {
            stream.on('end', () => resolve(Buffer.concat(chunks)));
        } else {
            stream.on('end', () => resolve(Buffer.concat(chunks).toString(encoding)));
        }
    });
}
