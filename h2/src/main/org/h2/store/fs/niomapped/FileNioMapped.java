/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.niomapped;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.EOFException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Paths;
import org.h2.engine.SysProperties;
import org.h2.store.fs.FileBaseDefault;
import org.h2.store.fs.FileUtils;
import org.h2.util.MemoryUnmapper;

/**
 * Uses memory mapped files.
 * The file size is limited to 2 GB.
 */
class FileNioMapped extends FileBaseDefault {

    private static final int GC_TIMEOUT_MS = 10_000;
    private final String name;
    private final MapMode mode;
    private FileChannel channel;
    private MappedByteBuffer mapped;
    private long fileLength;

    FileNioMapped(String fileName, String mode) throws IOException {
        if ("r".equals(mode)) {
            this.mode = MapMode.READ_ONLY;
        } else {
            this.mode = MapMode.READ_WRITE;
        }
        this.name = fileName;
        channel = FileChannel.open(Paths.get(fileName), FileUtils.modeToOptions(mode), FileUtils.NO_ATTRIBUTES);
        reMap();
    }

    private void unMap() throws IOException {
        if (mapped == null) {
            return;
        }
        // first write all data
        mapped.force();

        // need to dispose old direct buffer, see bug
        // https://bugs.openjdk.java.net/browse/JDK-4724038

        if (SysProperties.NIO_CLEANER_HACK) {
            if (MemoryUnmapper.unmap(mapped)) {
                mapped = null;
                return;
            }
        }
        WeakReference<MappedByteBuffer> bufferWeakRef = new WeakReference<>(mapped);
        mapped = null;
        long stopAt = System.nanoTime() + GC_TIMEOUT_MS * 1_000_000L;
        while (bufferWeakRef.get() != null) {
            if (System.nanoTime() - stopAt > 0L) {
                throw new IOException("Timeout (" + GC_TIMEOUT_MS + " ms) reached while trying to GC mapped buffer");
            }
            System.gc();
            Thread.yield();
        }
    }

    /**
     * Re-map byte buffer into memory, called when file size has changed or file
     * was created.
     */
    private void reMap() throws IOException {
        if (mapped != null) {
            unMap();
        }
        fileLength = channel.size();
        checkFileSizeLimit(fileLength);
        // maps new MappedByteBuffer; the old one is disposed during GC
        mapped = channel.map(mode, 0, fileLength);
        int limit = mapped.limit();
        int capacity = mapped.capacity();
        if (limit < fileLength || capacity < fileLength) {
            throw new IOException("Unable to map: length=" + limit +
                    " capacity=" + capacity + " length=" + fileLength);
        }
        if (SysProperties.NIO_LOAD_MAPPED) {
            mapped.load();
        }
    }

    private static void checkFileSizeLimit(long length) throws IOException {
        if (length > Integer.MAX_VALUE) {
            throw new IOException(
                    "File over 2GB is not supported yet when using this file system");
        }
    }

    @Override
    public void implCloseChannel() throws IOException {
        if (channel != null) {
            unMap();
            channel.close();
            channel = null;
        }
    }

    @Override
    public String toString() {
        return "nioMapped:" + name;
    }


    private static final ReentrantReadWriteLock lock0 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock0 = lock0.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock0 = lock0.writeLock();

    @Override
    public long size() throws IOException {
        writeLock0.lock();
        try {
        return fileLength;
        } finally {
            writeLock0.unlock();
        }
    }


    private static final ReentrantReadWriteLock lock1 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock1 = lock1.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock1 = lock1.writeLock();

    @Override
    public int read(ByteBuffer dst, long pos) throws IOException {
        writeLock1.lock();
        try {
        checkFileSizeLimit(pos);
        try {
            int len = dst.remaining();
            if (len == 0) {
                return 0;
            }
            len = (int) Math.min(len, fileLength - pos);
            if (len <= 0) {
                return -1;
            }
            mapped.position((int)pos);
            mapped.get(dst.array(), dst.arrayOffset() + dst.position(), len);
            dst.position(dst.position() + len);
            pos += len;
            return len;
        } catch (IllegalArgumentException | BufferUnderflowException e) {
            EOFException e2 = new EOFException("EOF");
            e2.initCause(e);
            throw e2;
        }
        } finally {
            writeLock1.unlock();
        }
    }

    @Override
    protected void implTruncate(long newLength) throws IOException {
        // compatibility with JDK FileChannel#truncate
        if (mode == MapMode.READ_ONLY) {
            throw new NonWritableChannelException();
        }
        if (newLength < size()) {
            setFileLength(newLength);
        }
    }

    private static final ReentrantReadWriteLock lock2 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock2 = lock2.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock2 = lock2.writeLock();


    public void setFileLength(long newLength) throws IOException {
        writeLock2.lock();
        try {
        if (mode == MapMode.READ_ONLY) {
            throw new NonWritableChannelException();
        }
        checkFileSizeLimit(newLength);
        unMap();
        for (int i = 0;; i++) {
            try {
                long length = channel.size();
                if (length >= newLength) {
                    channel.truncate(newLength);
                } else {
                    channel.write(ByteBuffer.wrap(new byte[1]), newLength - 1);
                }
                break;
            } catch (IOException e) {
                if (i > 16 || !e.toString().contains("user-mapped section open")) {
                    throw e;
                }
            }
            System.gc();
        }
        reMap();
        } finally {
            writeLock2.unlock();
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        mapped.force();
        channel.force(metaData);
    }


    private static final ReentrantReadWriteLock lock3 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock3 = lock3.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock3 = lock3.writeLock();

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        writeLock3.lock();
        try {
        checkFileSizeLimit(position);
        int len = src.remaining();
        // check if need to expand file
        if (mapped.capacity() < position + len) {
            setFileLength(position + len);
        }
        mapped.position((int)position);
        mapped.put(src);
        return len;
        } finally {
            writeLock3.unlock();
        }
    }


    private static final ReentrantReadWriteLock lock4 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock4 = lock4.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock4 = lock4.writeLock();

    @Override
    public FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        writeLock4.lock();
        try {
        return channel.tryLock(position, size, shared);
        } finally {
            writeLock4.unlock();
        }
    }

}