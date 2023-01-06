import { Readable, ReadableOptions, Transform, TransformCallback, TransformOptions } from 'stream';
import { PiecesSet, IRange } from 'data-pieces';
import { RandomAccessStorage } from './RandomAccessStorage';
import { Semaphore } from 'data-semaphore';
import { LoggerInterface } from 'clui-logger';

/**
 * Class that connects three components: a Readable stream, a RandomAccessStorage cache, and one or more output streams.
 * Say you intend to stream a resource over HTTP, but you wish to cache it locally (in memory, on disk or through some other means).
 * With this class, the original stream is divided into pieces, which are stored on the cache RAS. When a new output stream is requested,
 * the class automatically checks which pieces are in cache already, and which ones are missing, and fetches them on-demand seamlessly.
 *
 * You can implement this abstract class with two methods, one which given a byte range, returns a Readable stream of the data from
 * the original source, and another one which provided access to an object that implements read and write operations into a backing cache storage.
 *
 * To see an example of a concrete implementation of this, one can look at the ReplicatedHTTPResource class.
 *
 * @TODO Eager input transmissions (try to cache missing pieces even when no output transmissions are active)
 * @TODO Support pausing/resuming transmissions (control input and output seperately, maybe even individually as well)
 * @TODO Support live source streams (no total size known ahead of time)
 */
export abstract class AbstractReplicatedResource {
    public readonly totalSize: number;

    public readonly pieceSize: number;

    public readonly lastPieceSize: number;

    public readonly piecesCount: number;

    public readonly pieces: PiecesSet;

    public defaultMinimumLag: number = 10;

    public activeTransmissions: Set<Transmission>;

    public cacheStream: RandomAccessStorage | null = null;

    protected get exposed (): ExposedReplicatedResource {
        return {
            totalSize: this.totalSize,
            pieces: this.pieces,
            logger: this.logger,
            writeToCache: this.writeToCache.bind(this),
            readFromCache: this.readFromCache.bind(this),
            closeActiveTransmission: this.closeActiveTransmission.bind(this),
        };
    }

    public logger: LoggerInterface | null = null;

    public constructor (totalSize: number, pieceSize: number) {
        this.totalSize = totalSize;
        this.pieceSize = pieceSize;
        this.lastPieceSize = this.totalSize % this.pieceSize == 0
            ? this.pieceSize
            : this.totalSize % this.pieceSize;
        this.piecesCount = Math.ceil(this.totalSize / this.pieceSize);
        this.pieces = new PiecesSet(this.piecesCount);
        this.activeTransmissions = new Set();
    }

    //#region Handle State

    protected async writeInputPieceToCache (transmission: InputTransmission, pieceBuffer: Buffer): Promise<void> {
        const piece = transmission.piecesCursor;
        if (piece < transmission.range.end) {
            await this.writeToCache(piece, pieceBuffer);

            // Register the piece
            this.pieces.add(piece);

            transmission.piecesCursor += 1;

            this.logger?.debug(`Written piece ${piece} from transmission ${transmission.id} to cache.`);

            // Check for waiting (blocked) dependant transmissions that are waiting for this piece specifically
            for (const outputTransmission of transmission.dependants) {
                if (outputTransmission.waiting && outputTransmission.piecesCursor == piece) {
                    const clonedBuffer = this.allocatePieceBuffer();
                    pieceBuffer.copy(clonedBuffer);

                    outputTransmission.stream.push(clonedBuffer);

                    outputTransmission.waiting = false;
                    outputTransmission.piecesCursor += 1;
                }
            }
        }
    }

    protected async onReceiveInputBuffer (transmission: InputTransmission, buffer: Buffer): Promise<void> {
        const release = await transmission.lock.acquire();

        if (transmission.closed) {
            return;
        }

        try {
            let offset = 0;

            // If we have received a partial buffer before that does not match our piece size
            // We try to fill it fully before writing to disk
            if (transmission.pieceBufferUsed > 0) {
                // Get the current piece size. Is guaranteed to be smaller than the buffer.
                // Is usually always the same, except for the very last piece, which can be smaller
                const pieceSize = this.getPieceSize(transmission.piecesCursor);

                // How many bytes the buffer has left
                const pieceBufferLeft = pieceSize - transmission.pieceBufferUsed;

                // How many bytes of the buffer we received here was used to fill
                // the partial piece buffer we had before
                const bufferUsed = Math.min(buffer.byteLength, pieceBufferLeft);

                buffer.copy(
                    /* target */ transmission.pieceBuffer,
                    /* targetStart */ transmission.pieceBufferUsed,
                    /* sourceStart */ offset,
                    /* sourceEnd */ offset + bufferUsed
                );

                transmission.pieceBufferUsed += bufferUsed;
                offset += bufferUsed;

                // If we filled the buffer, we can write it and clear it
                if (pieceSize === transmission.pieceBufferUsed) {
                    await this.writeInputPieceToCache(transmission, transmission.pieceBuffer.subarray(0, pieceSize));
                    transmission.pieceBufferUsed = 0;
                }
            }

            while (offset < buffer.byteLength) {
                const bufferLeft = buffer.byteLength - offset;

                const pieceSize = this.getPieceSize(transmission.piecesCursor);

                // Special case when the buffer is the exact size of a piece
                // and we have not consumed any of it yet (offset == 0)
                if (bufferLeft == pieceSize && offset == 0) {
                    await this.writeInputPieceToCache(transmission, buffer);
                    offset = buffer.byteLength;
                } else if (bufferLeft >= pieceSize) {
                    // Create a slice of the buffer with the size of one piece
                    // This buffer is backed by the same region of memory as it's parent buffer,
                    // only the start and end positions are changed
                    const sliceBuffer = buffer.subarray(offset, offset + pieceSize);

                    await this.writeInputPieceToCache(transmission, sliceBuffer);

                    offset += pieceSize;
                } else {
                    // If we do not have enough bytes left in this buffer to complete a whole piece,
                    // then save what we have in the piece buffer and wait for more messages
                    buffer.copy(
                        /* target */ transmission.pieceBuffer,
                        /* targetStart */ transmission.pieceBufferUsed,
                        /* sourceStart */ offset,
                        /* sourceEnd */ offset + bufferLeft
                    );

                    transmission.pieceBufferUsed += bufferLeft;
                    offset += bufferLeft;
                }
            }
        } finally {
            release();
        }

        // If we have received all the pieces required for this input transmission
        // we should close it, and then handle it's dependencies
        // (creating new input transmissions if any of them require them)
        if (transmission.piecesCursor >= transmission.range.end) {
            await this.closeActiveTransmission(transmission);
        }
    }

    protected createInputTransmission (piecesRange: IRange): InputTransmission {
        const bytesRange = this.getPiecesPositionsForRange(piecesRange);

        const transmission: InputTransmission = {
            id: randomString(10),
            kind: TransmissionKind.Input,
            bytesRange: bytesRange,
            range: piecesRange,
            piecesCursor: piecesRange.start,
            stream: null as any,
            pieceBuffer: this.allocatePieceBuffer(),
            pieceBufferUsed: 0,
            dependants: new Set(),
            lock: new Semaphore(1),
            error: null,
            closed: false,
        };

        this.logger?.debug(`Creating input transmission ${transmission.id} (range = [${piecesRange.start}, ${piecesRange.end}]).`);

        const stream = this.createSourceStream(bytesRange);

        if (stream == null) {
            throw new Error(`Create source stream function return null.`);
        }

        stream.on('data', buffer => this.onReceiveInputBuffer(transmission, buffer));
        stream.on('error', err => this.closeActiveTransmission(transmission, err));
        stream.on('close', () => this.closeActiveTransmission(transmission));

        transmission.stream = stream;

        this.activeTransmissions.add(transmission);

        return transmission;
    }

    protected createOutputTransmission (bytesRange: IRange): OutputTransmission {
        const piecesRange = this.getPiecesBetweenRange(bytesRange);

        const transmission: OutputTransmission = {
            id: randomString(10),
            kind: TransmissionKind.Output,
            bytesRange: bytesRange,
            range: piecesRange,
            piecesCursor: piecesRange.start,
            // We are tricking the typescript compiler here, because we know we will assign
            // this property right after this
            stream: null as any,
            trimmedStream: null as any,
            waiting: false,
            dependency: null,
            error: null,
            closed: false,
        };

        this.logger?.debug(`Creating output transmission ${transmission.id} (range = [${piecesRange.start}, ${piecesRange.end}]).`);

        transmission.stream = new OutputTransmissionStream(this.exposed, transmission);

        // Calculate the offsets required to trim from the first and last pieces
        // to exactly match the requested byte range
        const [skip, take] = this.getPieceTrimOffsets(bytesRange);

        if (skip != null || take != null) {
            this.logger?.debug(`Creating trimmed stream with skip ${skip}, take ${take} for transmission ${transmission.id}.`);

            transmission.trimmedStream = transmission.stream
                .pipe(new SkipTakeTransformStream(skip, take));
        } else {
            transmission.trimmedStream = transmission.stream;
        }

        // Save the newly created transmission
        this.activeTransmissions.add(transmission);

        // Determine if we need to create an input transmission as well,
        // in case there is any piece required for this output transmission that
        // is not cached yet
        const nextEmpty = this.pieces.empty(rangeToClosed(piecesRange)).next();

        if (!nextEmpty.done) {
            let inputTransmission = this.getActiveInputTransmissionFor(piecesRange.start);

            if (inputTransmission == null) {
                inputTransmission = this.createInputTransmission(rangeToOpened(nextEmpty.value));
            }

            this.logger?.debug(`Adding dependency between output ${transmission.id} and input ${inputTransmission.id}.`);

            // Create the relation between the dependencies
            transmission.dependency = inputTransmission;
            inputTransmission.dependants.add(transmission);
        }

        return transmission;
    }

    protected getActiveInputTransmissionFor (piece: number, pieceCursor: number | null = null): InputTransmission | null {
        // Sometimes our piece cursor can be different from the missing piece
        pieceCursor ??= piece;

        let closestInputTransmission: InputTransmission | null = null;

        for (const transmission of this.activeTransmissions) {
            if (transmission.kind === TransmissionKind.Input) {
                // Check if this transmission contains the expected piece, and
                // if it if not too far back from the one we require
                const isWithinRange = transmission.range.start <= piece
                    && transmission.piecesCursor >= pieceCursor - this.defaultMinimumLag;

                // Also check if it is closer than the transmission we already have
                const isCloser = closestInputTransmission == null
                    || closestInputTransmission.bytesRange.start;

                if (isWithinRange && isCloser) {
                    closestInputTransmission = transmission;
                }
            }
        }

        return closestInputTransmission;
    }

    protected async closeActiveTransmission (transmission: Transmission, error: Error | null = null): Promise<void> {
        if (transmission.closed) {
            return;
        }

        if (transmission.kind === TransmissionKind.Input) {
            // Wait for the lock to be acquired and release it right away
            // We do this just to make sure all the writes to the cache that are
            // in queue have been executed already
            await transmission.lock.use(() => { });
        }

        if (error != null) {
            this.logger?.debug(`Closing transmission ${transmission.id} with error ${error.message}.`);
        } else {
            this.logger?.debug(`Closing transmission ${transmission.id} with no error.`);
        }

        // Mark the transmission as closed to avoid running this code twice for the same one
        transmission.closed = true;

        // Remove the transmission object from the active transmissions' set
        this.activeTransmissions.delete(transmission);

        if (transmission.kind === TransmissionKind.Input) {
            // If there are any active output transmissions that depend on this input transmission
            // we need to handle what happens to them
            if (transmission.dependants.size > 0) {
                // There are two options:
                //   - if the input transmission is complete, we have to check if they have any
                //     other range of missing pieces and if so create a new input transmission
                //     for that range, as well as updating the dependency properties
                //   - if the input transmission was closed abruptly, then we have to also
                //     close the dependant output transmissions, since they will not have the data they require any more

                // TODO Implement a third option, where we employ some sort of transparent retry mechanism when the
                //      input is terminated abruptly

                const isInputComplete = transmission.piecesCursor >= transmission.range.end;

                if (isInputComplete) {
                    // Assign or create new transmissions if necessary for the dependants of the current input
                    // TODO Pass in a range for this empty call
                    const nextEmpty = this.pieces.empty().next();

                    if (!nextEmpty.done) {
                        const start = nextEmpty.value.start;

                        // The minimum piece cursor is the minimum value of all the
                        // remaining dependant transmissions pieceCursor
                        let minPieceCursor: number = start;

                        // Create a new set for the current dependant output transmissions
                        // that will remain dependant on the new transmission we will create
                        const remainingDependants = new Set<OutputTransmission>();

                        for (const depTransmission of transmission.dependants) {
                            // If this dependant still requires the next empty segment
                            // as well, we add it to the list of remaining
                            if (depTransmission.range.end > start) {
                                remainingDependants.add(depTransmission);

                                if (depTransmission.piecesCursor < minPieceCursor) {
                                    minPieceCursor = depTransmission.piecesCursor;
                                }
                            } else {
                                this.logger?.debug(`Removing dependency of output ${depTransmission.id}.`);
                                depTransmission.dependency = null;
                            }
                        }

                        if (remainingDependants.size > 0) {
                            // Before we create a new input transmission, we check if there is already one for this
                            let newTransmission = this.getActiveInputTransmissionFor(start, minPieceCursor);

                            // If there is none we have to create one
                            if (newTransmission == null) {
                                newTransmission = this.createInputTransmission(rangeToOpened(nextEmpty.value));
                            }

                            // Assign this new input transmission to the dependants and vice-versa
                            for (const depTransmission of remainingDependants) {
                                this.logger?.debug(`Switching dependency of output ${depTransmission.id} to input ${transmission.id}.`);
                                newTransmission.dependants.add(depTransmission);
                                depTransmission.dependency = newTransmission;
                            }
                        }
                    }
                } else {
                    // Close the dependant transmissions
                    const dependants = transmission.dependants;
                    transmission.dependants = new Set();

                    for (const depTransmission of dependants) {
                        // TODO Instead of closing right away, maybe we can remove the dependency, save the error,
                        //      and only when the output tries to block (waiting = true) we check if it has an error,
                        //      amd of so, close it only then
                        depTransmission.dependency = null;
                        await this.closeActiveTransmission(depTransmission, error);
                    }
                }
            }
        } else if (transmission.kind === TransmissionKind.Output) {
            // Close any dependencies we may have
            if (transmission.dependency != null) {
                const dependency = transmission.dependency;

                dependency.dependants.delete(transmission);

                // If there are no more output transmissions that depend on this
                // input transmission
                if (dependency.dependants.size === 0) {
                    await this.closeActiveTransmission(transmission, error);
                }
            }
        }

        if (error != null) {
            transmission.error = error;
        }

        if (!transmission.stream.destroyed) {
            transmission.stream.destroy();
        }

        // TODO If it's the last transmission, check if we can eagerly fill the cache. Make this behavior configurable

        if (this.activeTransmissions.size === 0 && this.cacheStream != null) {
            this.cacheStream?.close();
        }
    }

    protected allocatePieceBuffer (piecesNumber: number = 1): Buffer {
        return Buffer.alloc(piecesNumber * this.pieceSize);
    }

    protected async writeToCache (piece: number, data: Buffer) {
        if (this.cacheStream == null) {
            this.cacheStream = this.createCachedStream();
        }

        // Prepare the arguments to call the read method
        const writer = this.cacheStream;
        const position = this.getPiecePosition(piece);

        // Read to the pre-allocated buffer
        const bytesWritten = await writer?.write(position, data);

        this.logger?.debug(`Writing piece ${piece} to cache in position ${position}, length ${bytesWritten} (out of ${data.byteLength})`);

        return bytesWritten;
    }

    protected async readFromCache (piece: number): Promise<Buffer> {
        if (this.cacheStream == null) {
            this.cacheStream = this.createCachedStream();
        }

        // Prepare the arguments to call the read method
        const reader = this.cacheStream;
        const position = this.getPiecePosition(piece);
        const buffer = this.allocatePieceBuffer();

        // Read to the pre-allocated buffer
        const bytesRead = await reader?.read(position, buffer);

        this.logger?.debug(`Reading piece ${piece} from cache in position ${position}, length ${bytesRead}`);

        // Return the buffer. If we did not read enough to fill the buffer,
        // return just the slice that was read from it
        if (bytesRead < buffer.byteLength) {
            return buffer.subarray(0, bytesRead);
        } else {
            return buffer;
        }
    }

    //#endregion

    public getPieceIndex (positionByte: number): number {
        return Math.floor(positionByte / this.pieceSize);
    }

    public getPieceSize (pieceIndex: number): number {
        // If this is the last piece, we should return the last piece size (can
        // be smaller than the regular piece size)
        if (pieceIndex == this.piecesCount - 1) {
            return this.lastPieceSize;
        } else {
            return this.pieceSize;
        }
    }

    public getPiecePosition (pieceIndex: number): number {
        if (pieceIndex == this.piecesCount) {
            return this.totalSize;
        } else {
            return pieceIndex * this.pieceSize;
        }
    }

    public getPiecesPositionsFor (startPiece: number, endPiece: number): IRange {
        const startIndex = this.getPiecePosition(startPiece);
        const endIndex = this.getPiecePosition(endPiece);

        return { start: startIndex, end: endIndex };
    }

    public getPiecesPositionsForRange (piecesRange: IRange): IRange {
        return this.getPiecesPositionsFor(piecesRange.start, piecesRange.end);
    }

    public getPiecesBetween (startByte: number, endByte: number): IRange {
        const startIndex = this.getPieceIndex(startByte);
        const endIndex = this.getPieceIndex(endByte - 1) + 1;

        return { start: startIndex, end: endIndex };
    }

    public getPiecesBetweenRange (bytesRange: IRange): IRange {
        return this.getPiecesBetween(bytesRange.start, bytesRange.end);
    }

    public getBoundedBytesRange (range?: Partial<IRange>): IRange {
        // Start with a full range of the stream as the basis for the bounded range
        const boundedRange = { start: 0, end: this.totalSize };

        // If the range argument has a start, then shrink our bounded range to it
        if (range && typeof range.start === 'number' && range.start > 0) {
            boundedRange.start = Math.min(range.start, this.totalSize);
        }

        // If the range argument has na end, then shrink our bounded range to it
        if (range && typeof range.end === 'number' && range.end > 0) {
            boundedRange.end = Math.min(range.end, this.totalSize);
        }

        if (boundedRange.end < boundedRange.start) {
            const { start, end } = boundedRange;

            // Swap the values
            boundedRange.end = start;
            boundedRange.start = end;
        }

        return boundedRange;
    }

    public getPieceTrimOffsets (bytesRange: IRange, piecesRange?: IRange): [skip: number | null, take: number | null] {
        // The values to return. If they align with the piece's boundaries, and nothing
        // needs to be trimmed, they can be null
        let skip: number | null = null;
        let take: number | null = null;

        // If no pieces range is provided, we calculate the smallest range that
        // includes the required bytes
        piecesRange ??= this.getPiecesBetweenRange(bytesRange);

        // Calculate the exact byte positions for the pieces, so we can compare
        // those positions with the requested `bytesRange` and then return the offsets
        const piecesBytesRange = this.getPiecesPositionsForRange(piecesRange);

        // If the start positions do not align, calculate the difference between them
        // so we know how many bytes from the piece we have to trim at the start
        if (piecesBytesRange.start != bytesRange.start) {
            skip = bytesRange.start - piecesBytesRange.start;
        }

        // If the end positions do not aligh, calculate the length of the requested
        // `bytesRange`, since we know those are the number of bytes we want to take
        if (piecesBytesRange.end != bytesRange.end) {
            take = bytesRange.end - bytesRange.start;
        }

        return [skip, take];
    }

    abstract createSourceStream (range: IRange): Readable;

    abstract createCachedStream (): RandomAccessStorage;

    createReadableStream (bytesRange?: Partial<IRange>): Readable {
        const boundedBytesRange = this.getBoundedBytesRange(bytesRange);

        const transmission = this.createOutputTransmission(boundedBytesRange);

        return transmission.trimmedStream;
    }

    async close (): Promise<void> {
        let activeTransismissions = Array.from(this.activeTransmissions);

        // Close the transmissions in two steps:
        //  - first, close the output transmissiosn (since doing so is guaranteed
        //    to now create new transmissions)
        //  - second, close any input transmissions leftover (since doing so could only
        //    spawn new transmissions if they had any output dependants, and all outputs
        //    are closed now, so that never happens)
        for (const transmission of activeTransismissions) {
            if (transmission.kind == TransmissionKind.Output) {
                await this.closeActiveTransmission(transmission);
            }
        }

        for (const transmission of activeTransismissions) {
            if (transmission.kind == TransmissionKind.Input) {
                await this.closeActiveTransmission(transmission);
            }
        }

        if (this.cacheStream != null) {
            await this.cacheStream.close();
        }

        this.activeTransmissions = new Set();
    }
}

export class ReplicatedResource extends AbstractReplicatedResource {
    public readonly createSourceStreamCallback: CreateSourceStreamCallback;
    public readonly createCachedStreamCallback: CreateCachedStreamCallback;

    public constructor (
        createSourceStreamCallback: CreateSourceStreamCallback,
        createCachedStreamCallback: CreateCachedStreamCallback,
        totalSize: number,
        pieceSize: number
    ) {
        super(totalSize, pieceSize);

        this.createSourceStreamCallback = createSourceStreamCallback;
        this.createCachedStreamCallback = createCachedStreamCallback;
    }

    public createSourceStream (range: IRange): Readable {
        return this.createSourceStreamCallback(range);
    }

    public createCachedStream (): RandomAccessStorage {
        return this.createCachedStreamCallback();
    }
}

export type CreateSourceStreamCallback = (range: IRange) => Readable;
export type CreateCachedStreamCallback = () => RandomAccessStorage;

export enum TransmissionKind {
    // Input transmissions are reading from the source and writing it directly to the cache
    Input = 'input',
    // Output transmissions are reading from the cache and and serving to the client
    Output = 'output',
}

export interface BaseTransmission {
    id: string;
    kind: TransmissionKind;
    range: IRange;
    piecesCursor: number;
    bytesRange: IRange;
    closed: boolean;
    error: Error | null;
}

export interface InputTransmission extends BaseTransmission {
    kind: TransmissionKind.Input;
    stream: Readable;
    pieceBuffer: Buffer;
    pieceBufferUsed: number;
    lock: Semaphore;
    dependants: Set<OutputTransmission>;
}

export interface OutputTransmission extends BaseTransmission {
    kind: TransmissionKind.Output;
    // The stream into which the raw piece chunks are pushed into
    stream: Readable;
    // The stream from which we can read the results of this transmission
    // To support reading from any byte range, including byte ranges that
    // do not align with the pieces completly, this stream is a transform stream
    // that automatically trims the first and last chunks to the correct length.
    // If we do align with the pieces, then this is just the same reference to the
    // original stream.
    trimmedStream: Readable;
    // Indicates if the readable stream is currently waiting for data
    waiting: boolean;
    // Usually each output transmission has an input dependency
    // Unless the full range of pieces possibly required by this output transmission
    // are already all cached, in which case, no dependency is required (we can just
    // read from the cache whenever and whatever we need)
    dependency: InputTransmission | null;
}

export type Transmission = InputTransmission | OutputTransmission;

interface ExposedReplicatedResource {
    readonly totalSize: number;

    readonly pieces: PiecesSet;

    readonly logger: LoggerInterface | null;

    closeActiveTransmission (transmission: Transmission, error?: Error | null): Promise<void>;

    writeToCache (piece: number, data: Buffer): Promise<void>;

    readFromCache (piece: number): Promise<Buffer>;
}

class OutputTransmissionStream extends Readable {
    protected resource: ExposedReplicatedResource;

    protected transmission: OutputTransmission;

    constructor (cachedStream: ExposedReplicatedResource, transmission: OutputTransmission, options?: ReadableOptions) {
        super(options);

        this.resource = cachedStream;
        this.transmission = transmission;
    }

    _read (size: number): void {
        // Ignore the suggested size argument for now

        // TODO Respect bytes range when serving the first and last pieces

        if (this.transmission.error != null && !this.destroyed) {
            this.resource.logger?.debug(`Destroy output transmission ${this.transmission.id} stream`);
            this.destroy(this.transmission.error);
            return;
        }

        const piece = this.transmission.piecesCursor;

        // If this transmission has already sent all the data it was requested
        if (piece >= this.transmission.range.end) {
            this.resource.logger?.debug(`Ending output transmission ${this.transmission.id} stream`);
            this.push(null);
            this.resource.closeActiveTransmission(this.transmission);
            return;
        }

        // if the piece we currently need is already cached, just retrieve it and push it to the stream
        if (this.resource.pieces?.has(piece)) {
            this.resource.logger?.debug(`Reading piece ${piece} for output transmission ${this.transmission.id} stream`);
            this.resource.readFromCache(piece)
                .then(buffer => {
                    this.resource.logger?.debug(`Piece length: ${buffer.byteLength}`);
                    this.transmission.piecesCursor += 1;
                    this.push(buffer);
                })
                .catch(err => this.destroy(err));
        } else {
            this.resource.logger?.debug(`Waiting piece ${piece} for output transmission ${this.transmission.id} stream`);
            // Otherwise mark this stream as waiting for data to arrive from it's dependency input
            this.transmission.waiting = true;
        }
    }

    _destroy (error: Error | null, callback: (error?: Error | null) => void): void {
        this.resource.closeActiveTransmission(this.transmission, error)
            .then(() => callback(error))
            .catch(closeErr => callback(closeErr || error));
    }
}

class SkipTakeTransformStream extends Transform {
    public skip: number | null;

    public take: number | null;

    protected _skipped: number = 0;

    protected _taken: number = 0;

    public constructor (skip: number | null = null, take: number | null = null, options?: TransformOptions) {
        super(options);

        this.skip = skip;
        this.take = take;
    }

    protected transformBuffer (chunk: Buffer, encoding: BufferEncoding): Buffer {
        if (this.skip != null) {
            const bufferBytesToSkip = Math.min(chunk.byteLength, this.skip - this._skipped);

            this._skipped += bufferBytesToSkip;

            chunk = chunk.subarray(bufferBytesToSkip);
        }

        if (this.take != null) {
            const bufferBytesToTake = Math.min(chunk.byteLength, this.take - this._taken);

            this._taken += bufferBytesToTake;

            chunk = chunk.subarray(0, bufferBytesToTake);
        }

        return chunk;
    }

    protected transformString (chunk: string, encoding: BufferEncoding): string {
        if (this.skip != null) {
            const stringCharsToSkip = Math.min(chunk.length, this.skip - this._skipped);

            this._skipped += stringCharsToSkip;

            chunk = chunk.substring(stringCharsToSkip);
        }

        if (this.take != null) {
            const stringCharsToTake = Math.min(chunk.length, this.take - this._taken);

            this._taken += stringCharsToTake;

            chunk = chunk.substring(0, stringCharsToTake);
        }

        return chunk;
    }

    public _transform (chunk: any, encoding: BufferEncoding, callback: TransformCallback): void {
        try {
            if (chunk == null) {
                callback(null, null);
            } else if (Buffer.isBuffer(chunk)) {
                callback(null, this.transformBuffer(chunk, encoding));
            } else if (typeof chunk === 'string') {
                callback(null, this.transformString(chunk, encoding));
            } else {
                throw new Error(`Invalid type`);
            }
        } catch (err) {
            callback(err as any);
        }
    }
}

function randomString (len: number, charSet?: string) {
    charSet ??= 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var randomString = '';
    for (var i = 0; i < len; i++) {
        var randomPoz = Math.floor(Math.random() * charSet.length);
        randomString += charSet.substring(randomPoz, randomPoz + 1);
    }
    return randomString;
}

function rangeToOpened (range: IRange): IRange {
    return {
        start: range.start,
        end: range.end + 1
    };
}

function rangeToClosed (range: IRange): IRange {
    return {
        start: range.start,
        end: range.end - 1
    };
}
