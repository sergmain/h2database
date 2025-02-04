/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * The base class for file implementations.
 */
public abstract class FileBase extends FileChannel {


    private static final ReentrantReadWriteLock lock0 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock0 = lock0.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock0 = lock0.writeLock();

    @Override
    public int read(ByteBuffer dst, long position)
            throws IOException {
        writeLock0.lock();
        try {
        long oldPos = position();
        position(position);
        int len = read(dst);
        position(oldPos);
        return len;
        } finally {
            writeLock0.unlock();
        }
    }


    private static final ReentrantReadWriteLock lock1 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock1 = lock1.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock1 = lock1.writeLock();

    @Override
    public int write(ByteBuffer src, long position)
            throws IOException {
        writeLock1.lock();
        try {
        long oldPos = position();
        position(position);
        int len = write(src);
        position(oldPos);
        return len;
        } finally {
            writeLock1.unlock();
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        // ignore
    }

    @Override
    protected void implCloseChannel() throws IOException {
        // ignore
    }

    @Override
    public FileLock lock(long position, long size, boolean shared)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException {
        throw new UnsupportedOperationException();
    }

}
