import { IRange } from 'data-pieces';
import { RandomAccessStorage } from './RandomAccessStorage';
import { Readable, PassThrough } from 'stream';
import { AbstractReplicatedResource, CreateCachedStreamCallback } from './ReplicatedResource';
import fetch from 'node-fetch';

/**
 * Represents a replicated resource that is accessed through HTTP
 *
 * ```ts
 * const sourceAddress = "https://cdn.jsdelivr.net/npm/big-buck-bunny-1080p@0.0.6/video.mp4";
 * const cacheFile = "big_buck_bunny_1080p_h264.mp4";
 * const pieceSize = 1024 * 100; // 100KB pieces
 *
 * const resource = await ReplicatedHTTPResource.from(sourceAddress, pieceSize, new FileSystemRAS(cacheFile));
 *
 * // Create the readable stream to start the caching process
 * const readable = resource.createReadableStream();
 * // Just for demo purposes, we don't want to do nothing with this stream, just want to force it to consume it's pieces (so it becomes cached)
 * readable.on('data', () => { });
 * // When it ends, force the resource to close (and flush the pieces to disk before closing)
 * readable.on('end', async () => {
 *   await resource.close();
 * });
 * ```
 */
export class ReplicatedHTTPResource extends AbstractReplicatedResource {
    public static async getContentLength (address: string): Promise<number | null> {
        // We send the identity header to avoid receiving the content type of
        // the compressed response, since we need the raw length instead
        // This is a best effort approach, since nothing prevents HTTP servers
        // that do not follow the spec to ignore this header and always return
        // the compressed length instead, or no length at all, or not responding
        // to HEAD requests and only GET instead.
        const headers = { 'accept-encoding': 'identity' };

        const response = await fetch(address, { method: 'HEAD', headers: headers });

        const contentLength = parseInt(response.headers.get('content-length') ?? '', 10);

        if (isNaN(contentLength)) {
            return null;
        }

        return contentLength;
    }

    /**
     * Retrieves the content length of the HTTP resource by performing an HEAD
     * request to the server. Then returns an instance of the Replicated Resource
     * class with that length as total size and the rest of the passed parameters
     *
     * @param address
     * @param pieceSize
     * @param cache
     * @returns
     */
    public static async from (address: string, pieceSize: number, cache: RandomAccessStorage | CreateCachedStreamCallback): Promise<ReplicatedHTTPResource> {
        const contentLength = await this.getContentLength(address);

        if (contentLength == null) {
            throw new Error(`Invalid content length returned for resource. Could not calculate total size.`);
        }

        return new ReplicatedHTTPResource(address, contentLength, pieceSize, cache);
    }

    public readonly address: string;

    protected createCachedStreamCallback: CreateCachedStreamCallback;

    public constructor (address: string, totalSize: number, pieceSize: number, cache: RandomAccessStorage | CreateCachedStreamCallback) {
        super(totalSize, pieceSize);
        this.address = address;

        if (typeof cache === 'function') {
            this.createCachedStreamCallback = cache;
        } else {
            this.createCachedStreamCallback = () => cache;
        }
    }

    public createSourceStream (range: IRange): Readable {
        // Save the address in a local variable because we will not be able to access `this`
        // inside the read function, as it will be bound to the Readable stream itself
        const address = this.address;

        // Initialize a passthrough stream (a stream that just passes along the content
        // it receives, as it receives it). This is because this method must return a stream right away,
        // cannot be asynchronous, but performing an HTTP request is asynchronous. As such,
        // we create this PassThrough to be able to return it right away, and later,
        // when we have the response from the request, we pipe the contents into this
        let stream: PassThrough = new PassThrough();

        // Asynchronously perform the HTTP request. Once we get a response, we can pipe it
        // into the passthrough stream we just created
        fetch(address, {
            headers: {
                'range': `bytes=${range.start}-${range.end}`
            }
        }).then(response => {
            if (response.body) {
                // Pipe the body of the response into the stream we returned
                PassThrough.from(response.body).pipe(stream);
            } else {
                // If no body is returned, end the stream
                stream.push(null);
            }
        }).catch(err => stream.destroy(err));

        return stream;
    }

    createCachedStream (): RandomAccessStorage {
        return this.createCachedStreamCallback();
    }
}
