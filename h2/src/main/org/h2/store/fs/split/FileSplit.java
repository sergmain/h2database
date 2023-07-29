/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.split;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.store.fs.FileBaseDefault;
import org.h2.store.fs.FilePath;

/**
 * A file that may be split into multiple smaller files.
 */
class FileSplit extends FileBaseDefault {

    private final FilePathSplit filePath;
    private final String mode;
    private final long maxLength;
    private FileChannel[] list;
    private volatile long length;

    FileSplit(FilePathSplit file, String mode, FileChannel[] list, long length,
            long maxLength) {
        this.filePath = file;
        this.mode = mode;
        this.list = list;
        this.length = length;
        this.maxLength = maxLength;
    }


    private static final ReentrantReadWriteLock lock0 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock0 = lock0.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock0 = lock0.writeLock();

    @Override
    public void implCloseChannel() throws IOException {
        writeLock0.lock();
        try {
        for (FileChannel c : list) {
            c.close();
        }
        } finally {
            writeLock0.unlock();
        }
    }

    @Override
    public long size() {
        return length;
    }


    private static final ReentrantReadWriteLock lock1 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock1 = lock1.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock1 = lock1.writeLock();

    @Override
    public int read(ByteBuffer dst, long position)
            throws IOException {
        writeLock1.lock();
        try {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        len = (int) Math.min(len, length - position);
        if (len <= 0) {
            return -1;
        }
        long offset = position % maxLength;
        len = (int) Math.min(len, maxLength - offset);
        FileChannel channel = getFileChannel(position);
        return channel.read(dst, offset);
        } finally {
            writeLock1.unlock();
        }
    }

    private FileChannel getFileChannel(long position) throws IOException {
        int id = (int) (position / maxLength);
        while (id >= list.length) {
            int i = list.length;
            FileChannel[] newList = new FileChannel[i + 1];
            System.arraycopy(list, 0, newList, 0, i);
            FilePath f = filePath.getBase(i);
            newList[i] = f.open(mode);
            list = newList;
        }
        return list[id];
    }

    @Override
    protected void implTruncate(long newLength) throws IOException {
        if (newLength >= length) {
            return;
        }
        int newFileCount = 1 + (int) (newLength / maxLength);
        if (newFileCount < list.length) {
            // delete some of the files
            FileChannel[] newList = new FileChannel[newFileCount];
            // delete backwards, so that truncating is somewhat transactional
            for (int i = list.length - 1; i >= newFileCount; i--) {
                // verify the file is writable
                list[i].truncate(0);
                list[i].close();
                try {
                    filePath.getBase(i).delete();
                } catch (DbException e) {
                    throw DataUtils.convertToIOException(e);
                }
            }
            System.arraycopy(list, 0, newList, 0, newList.length);
            list = newList;
        }
        long size = newLength - maxLength * (newFileCount - 1);
        list[list.length - 1].truncate(size);
        this.length = newLength;
    }


    private static final ReentrantReadWriteLock lock2 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock2 = lock2.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock2 = lock2.writeLock();

    @Override
    public void force(boolean metaData) throws IOException {
        writeLock2.lock();
        try {
        for (FileChannel c : list) {
            c.force(metaData);
        }
        } finally {
            writeLock2.unlock();
        }
    }


    private static final ReentrantReadWriteLock lock3 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock3 = lock3.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock3 = lock3.writeLock();

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        writeLock3.lock();
        try {
        if (position >= length && position > maxLength) {
            // may need to extend and create files
            long oldFilePointer = position;
            long x = length - (length % maxLength) + maxLength;
            for (; x < position; x += maxLength) {
                if (x > length) {
                    // expand the file size
                    position(x - 1);
                    write(ByteBuffer.wrap(new byte[1]));
                }
                position = oldFilePointer;
            }
        }
        long offset = position % maxLength;
        int len = src.remaining();
        FileChannel channel = getFileChannel(position);
        int l = (int) Math.min(len, maxLength - offset);
        if (l == len) {
            l = channel.write(src, offset);
        } else {
            int oldLimit = src.limit();
            src.limit(src.position() + l);
            l = channel.write(src, offset);
            src.limit(oldLimit);
        }
        length = Math.max(length, position + l);
        return l;
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
        return list[0].tryLock(position, size, shared);
        } finally {
            writeLock4.unlock();
        }
    }

    @Override
    public String toString() {
        return filePath.toString();
    }

}