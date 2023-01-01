import test from 'tape';
import fs from 'fs/promises';
import path from 'path';
import { FileSystemRAS, MemoryRAS, RandomAccessStorage } from './RandomAccessStorage';

const firstBuffer = Buffer.from('Lorem ipsum dolor sit amet, consectetur adipiscing elit.', 'utf8');
const firstBufferOffset = 0;
const secondBuffer = Buffer.from('Praesent turpis velit, facilisis ut sapien ut, suscipit dignissim ipsum.', 'utf8');
const secondBufferOffset = firstBuffer.byteLength;
const thirdBuffer = Buffer.from('Vestibulum vel sapien condimentum, commodo ligula vel, pellentesque magna.', 'utf8');
const thirdBufferOffset = secondBufferOffset + secondBuffer.byteLength;

test('FileSystemRAS auto growth writes', async t => {
    const folder = await fs.mkdtemp('unicast-cache-test-filesystemras');

    const filepath = path.join(folder, "file.txt");

    const ras: RandomAccessStorage = new FileSystemRAS(filepath);

    t.true(ras.isReadable, "Property isReadable should be true.");
    t.true(ras.isWritable, "Property isWritable should be true.");

    try {
        await ras.write(firstBufferOffset, firstBuffer);
        await ras.write(secondBufferOffset, secondBuffer);
        await ras.write(thirdBufferOffset, thirdBuffer);
        await ras.close();

        const actualContents = await fs.readFile(filepath, { encoding: 'utf8' });
        const expectedContents = firstBuffer.toString('utf8') + secondBuffer.toString('utf8') + thirdBuffer.toString('utf8');

        t.equals(actualContents, expectedContents);
    } finally {
        await fs.rm(folder, { recursive: true, force: true });
    }
});

test('FileSystemRAS out of order writes', async t => {
    const folder = await fs.mkdtemp('unicast-cache-test-filesystemras');

    const filepath = path.join(folder, "file.txt");

    const ras: RandomAccessStorage = new FileSystemRAS(filepath);

    t.true(ras.isReadable, "Property isReadable should be true.");
    t.true(ras.isWritable, "Property isWritable should be true.");

    try {
        await ras.write(firstBufferOffset, firstBuffer);
        await ras.write(thirdBufferOffset, thirdBuffer);
        await ras.write(secondBufferOffset, secondBuffer);
        await ras.close();

        const actualContents = await fs.readFile(filepath, { encoding: 'utf8' });
        const expectedContents = firstBuffer.toString('utf8') + secondBuffer.toString('utf8') + thirdBuffer.toString('utf8');

        t.equals(actualContents, expectedContents);
    } finally {
        await fs.rm(folder, { recursive: true, force: true });
    }
});

test('FileSystemRAS over writes', async t => {
    const folder = await fs.mkdtemp('unicast-cache-test-filesystemras');

    const filepath = path.join(folder, "file.txt");

    const ras: RandomAccessStorage = new FileSystemRAS(filepath);

    t.true(ras.isReadable, "Property isReadable should be true.");
    t.true(ras.isWritable, "Property isWritable should be true.");

    try {
        await ras.write(firstBufferOffset, firstBuffer);
        await ras.write(secondBufferOffset, firstBuffer.subarray(0, secondBuffer.length));
        await ras.write(thirdBufferOffset, firstBuffer.subarray(0, thirdBuffer.length));

        await ras.write(thirdBufferOffset, thirdBuffer);
        await ras.write(secondBufferOffset, secondBuffer);
        await ras.close();

        const actualContents = await fs.readFile(filepath, { encoding: 'utf8' });
        const expectedContents = firstBuffer.toString('utf8') + secondBuffer.toString('utf8') + thirdBuffer.toString('utf8');

        t.equals(actualContents, expectedContents);
    } finally {
        await fs.rm(folder, { recursive: true, force: true });
    }
});


test('FileSystemRAS concurrent out of order writes', async t => {
    const folder = await fs.mkdtemp('unicast-cache-test-filesystemras');

    const filepath = path.join(folder, "file.txt");

    const ras: RandomAccessStorage = new FileSystemRAS(filepath);

    t.true(ras.isReadable, "Property isReadable should be true.");
    t.true(ras.isWritable, "Property isWritable should be true.");

    try {
        await Promise.all([
            ras.write(firstBufferOffset, firstBuffer),
            ras.write(thirdBufferOffset, thirdBuffer),
            ras.write(secondBufferOffset, secondBuffer),
        ]);
        await ras.close();

        const actualContents = await fs.readFile(filepath, { encoding: 'utf8' });
        const expectedContents = firstBuffer.toString('utf8') + secondBuffer.toString('utf8') + thirdBuffer.toString('utf8');

        t.equals(actualContents, expectedContents);
    } finally {
        await fs.rm(folder, { recursive: true, force: true });
    }
});


test('MemoryRAS auto growth writes', async t => {
    const ras: MemoryRAS = new MemoryRAS();

    t.true(ras.isReadable, "Property isReadable should be true.");
    t.true(ras.isWritable, "Property isWritable should be true.");

    await ras.write(firstBufferOffset, firstBuffer);
    await ras.write(secondBufferOffset, secondBuffer);
    await ras.write(thirdBufferOffset, thirdBuffer);
    await ras.close();

    const actualContents = ras.buffer.toString('utf8');
    const expectedContents = firstBuffer.toString('utf8') + secondBuffer.toString('utf8') + thirdBuffer.toString('utf8');

    t.equals(actualContents, expectedContents);
});

test('MemoryRAS out of order writes', async t => {
    const ras: MemoryRAS = new MemoryRAS();

    t.true(ras.isReadable, "Property isReadable should be true.");
    t.true(ras.isWritable, "Property isWritable should be true.");

    await ras.write(firstBufferOffset, firstBuffer);
    await ras.write(thirdBufferOffset, thirdBuffer);
    await ras.write(secondBufferOffset, secondBuffer);
    await ras.close();


    const actualContents = ras.buffer.toString('utf8');
    const expectedContents = firstBuffer.toString('utf8') + secondBuffer.toString('utf8') + thirdBuffer.toString('utf8');

    t.equals(actualContents, expectedContents);
});

test('MemoryRAS over writes', async t => {
    const ras: MemoryRAS = new MemoryRAS();

    t.true(ras.isReadable, "Property isReadable should be true.");
    t.true(ras.isWritable, "Property isWritable should be true.");

    await ras.write(firstBufferOffset, firstBuffer);
    await ras.write(secondBufferOffset, firstBuffer.subarray(0, secondBuffer.length));
    await ras.write(thirdBufferOffset, firstBuffer.subarray(0, thirdBuffer.length));

    await ras.write(thirdBufferOffset, thirdBuffer);
    await ras.write(secondBufferOffset, secondBuffer);
    await ras.close();

    const actualContents = ras.buffer.toString('utf8');
    const expectedContents = firstBuffer.toString('utf8') + secondBuffer.toString('utf8') + thirdBuffer.toString('utf8');

    t.equals(actualContents, expectedContents);
});


test('MemoryRAS concurrent out of order writes', async t => {
    const ras: MemoryRAS = new MemoryRAS();

    t.true(ras.isReadable, "Property isReadable should be true.");
    t.true(ras.isWritable, "Property isWritable should be true.");

    await Promise.all([
        ras.write(firstBufferOffset, firstBuffer),
        ras.write(thirdBufferOffset, thirdBuffer),
        ras.write(secondBufferOffset, secondBuffer),
    ]);
    await ras.close();

    const actualContents = ras.buffer.toString('utf8');
    const expectedContents = firstBuffer.toString('utf8') + secondBuffer.toString('utf8') + thirdBuffer.toString('utf8');

    t.equals(actualContents, expectedContents);
});
