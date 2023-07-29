/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Default implementation of the slow operations that need synchronization because they
 * involve the file position.
 */
public abstract class FileBaseDefault extends FileBase {

    private long position = 0;


    private static final ReentrantReadWriteLock lock0 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock0 = lock0.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock0 = lock0.writeLock();

    @Override
    public final long position() throws IOException {
        writeLock0.lock();
        try {
        return position;
        } finally {
            writeLock0.unlock();
        }
    }


    private static final ReentrantReadWriteLock lock1 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock1 = lock1.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock1 = lock1.writeLock();

    @Override
    public final FileChannel position(long newPosition) throws IOException {
        writeLock1.lock();
        try {
        if (newPosition < 0) {
            throw new IllegalArgumentException();
        }
        position = newPosition;
        return this;
        } finally {
            writeLock1.unlock();
        }
    }


    private static final ReentrantReadWriteLock lock2 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock2 = lock2.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock2 = lock2.writeLock();

    @Override
    public final int read(ByteBuffer dst) throws IOException {
        writeLock2.lock();
        try {
        int read = read(dst, position);
        if (read > 0) {
            position += read;
        }
        return read;
        } finally {
            writeLock2.unlock();
        }
    }


    private static final ReentrantReadWriteLock lock3 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock3 = lock3.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock3 = lock3.writeLock();

    @Override
    public final int write(ByteBuffer src) throws IOException {
        writeLock3.lock();
        try {
        int written = write(src, position);
        if (written > 0) {
            position += written;
        }
        return written;
        } finally {
            writeLock3.unlock();
        }
    }


    private static final ReentrantReadWriteLock lock4 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock4 = lock4.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock4 = lock4.writeLock();

    @Override
    public final FileChannel truncate(long newLength) throws IOException {
        writeLock4.lock();
        try {
        implTruncate(newLength);
        if (newLength < position) {
            position = newLength;
        }
        return this;
        } finally {
            writeLock4.unlock();
        }
    }

    /**
     * The truncate implementation.
     *
     * @param size the new size
     * @throws IOException on failure
     */
    protected abstract void implTruncate(long size) throws IOException;
}
