import fs, { FileHandle } from 'fs/promises';
import { Semaphore } from 'data-semaphore';

export interface RandomAccessStorage {
    readonly isReadable: boolean;

    readonly isWritable: boolean;

    write (position: number, data: Buffer): Promise<number>;

    writeSlice (position: number, data: Buffer, offset: number, length: number): Promise<number>;

    read (position: number, buffer: Buffer): Promise<number>;

    readSlice (position: number, buffer: Buffer, offset: number, length: number): Promise<number>;

    close (): Promise<void>;
}

export abstract class AbstractRandomAccessStorage implements RandomAccessStorage {
    abstract readonly isReadable: boolean;
    abstract readonly isWritable: boolean;

    write (position: number, data: Buffer): Promise<number> {
        return this.writeSlice(position, data, 0, data.byteLength);
    }

    read (position: number, buffer: Buffer): Promise<number> {
        return this.readSlice(position, buffer, 0, buffer.byteLength);
    }

    abstract writeSlice (position: number, data: Buffer, offset: number, length: number): Promise<number>;

    abstract readSlice (position: number, buffer: Buffer, offset: number, length: number): Promise<number>;

    abstract close (): Promise<void>;
}

export class FileSystemRAS extends AbstractRandomAccessStorage {
    public readonly isReadable: boolean = true;

    public readonly isWritable: boolean = true;

    public readonly path: string;

    protected writeLock: Semaphore;

    protected handleLock: Semaphore;

    protected handle: FileHandle | null = null;

    public constructor (path: string) {
        super();

        this.path = path;
        this.writeLock = new Semaphore(1);
        this.handleLock = new Semaphore(1);
    }

    protected async openHandle (): Promise<FileHandle> {
        const release = await this.handleLock.acquire();

        try {
            if (this.handle == null) {
                // TODO Open flags (readable, writable)
                // File modes:
                // r	To open file to read and throws exception if file doesn’t exists.
                // w+	Open file to read and write. File is created if it doesn’t exists.
                this.handle = await fs.open(this.path, 'w+');
            }

            return this.handle;
        } finally {
            release();
        }
    }

    async writeSlice (position: number, data: Buffer, offset: number, length: number): Promise<number> {
        const release = await this.writeLock.acquire();

        try {
            let handle = this.handle;

            if (handle == null) {
                handle = await this.openHandle();
            }

            const result = await handle.write(data, offset, length, position);

            return result.bytesWritten;
        } finally {
            release();
        }
    }

    async readSlice (position: number, buffer: Buffer, offset: number, length: number): Promise<number> {
        let handle = this.handle;

        if (handle == null) {
            handle = await this.openHandle();
        }

        const result = await handle.read(buffer, offset, length, position);

        return result.bytesRead;
    }

    async close (): Promise<void> {
        const releaseWrite = await this.writeLock.acquire();

        try {
            const releaseHandle = await this.handleLock.acquire();

            try {
                if (this.handle != null) {
                    await this.handle.sync();
                    await this.handle.close();
                    this.handle = null;
                }
            } finally {
                releaseHandle();
            }
        } finally {
            releaseWrite();
        }
    }
}

export class MemoryRAS extends AbstractRandomAccessStorage {
    public readonly isReadable: boolean = true;

    public readonly isWritable: boolean = true;

    public minimumBufferByteLength: number = 512;

    public length: number = 0;

    protected _rawBuffer: Buffer | null = null;

    public get buffer (): Buffer {
        if (this._rawBuffer == null) {
            return Buffer.alloc(0);
        }

        return this._rawBuffer.subarray(0, this.length);
    }

    public constructor () {
        super();
    }

    protected ensureBufferCapacity (buffer: Buffer | null, byteLength: number): Buffer {
        // Check if the buffer we already have has enough space for it
        if (buffer == null || buffer.byteLength < byteLength) {
            // Ensure a minimum buffer capacity when allocating a new buffer
            if (byteLength < this.minimumBufferByteLength) {
                byteLength = this.minimumBufferByteLength;
            }

            // Convert the byte length to the nearest upward power of 2 number
            // For example, convert 6 to 8, 24 to 32, 500 to 512, etc...
            const alignedByteLength = Math.pow(2, Math.ceil(Math.log2(byteLength)));

            const newBuffer = Buffer.alloc(alignedByteLength);

            // If we had a buffer before, we can copy it to the new one
            if (buffer != null) {
                buffer.copy(newBuffer);
            }

            return newBuffer;
        } else {
            return buffer;
        }
    }

    async writeSlice (position: number, data: Buffer, offset: number, length: number): Promise<number> {
        this._rawBuffer = this.ensureBufferCapacity(this._rawBuffer, position + length);

        // Copy from `buffer`, starting to read from `offset` until `offset + length`,
        // into `this.buffer`, starting to write at `position`
        const copied = data.copy(this._rawBuffer, position, offset, offset + length);

        // Update our internal length array
        if (this.length < position + copied) {
            this.length = position + copied;
        }

        return Promise.resolve(length);
    }

    async readSlice (position: number, buffer: Buffer, offset: number, length: number): Promise<number> {
        if (this._rawBuffer == null) {
            return Promise.resolve(0);
        }

        // If the buffer is smaller than what we want to read, we need to scale
        // down the `length` variable to read everything after position until the end of the buffer.
        if (position + length > this.length) {
            // Make sure length is never negative (for example, if position > buffer.byteLength)
            length = Math.max(0, this.length - position);
        }

        // Length of zero indicates end-of-file, and in such a case, we don't need to copy anything
        if (length > 0) {
            // Copy from `this.buffer`, starting to read from `position` until `position + length`,
            // into `buffer`, starting to write at `offset`
            return this._rawBuffer.copy(buffer, offset, position, position + length);
        } else {
            return 0;
        }
    }

    async close (): Promise<void> {
        // Close is a noop here
        return Promise.resolve();
    }
}
