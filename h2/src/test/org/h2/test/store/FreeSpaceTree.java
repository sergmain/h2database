/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.TreeSet;

import org.h2.mvstore.DataUtils;
import org.h2.util.MathUtils;

/**
 * A list that maintains ranges of free space (in blocks) in a file.
 */
public class FreeSpaceTree {

    /**
     * The first usable block.
     */
    private final int firstFreeBlock;

    /**
     * The block size in bytes.
     */
    private final int blockSize;

    /**
     * The list of free space.
     */
    private TreeSet<BlockRange> freeSpace = new TreeSet<>();

    public FreeSpaceTree(int firstFreeBlock, int blockSize) {
        this.firstFreeBlock = firstFreeBlock;
        if (Integer.bitCount(blockSize) != 1) {
            throw DataUtils.newIllegalArgumentException("Block size is not a power of 2");
        }
        this.blockSize = blockSize;
        clear();
    }

    private static final ReentrantReadWriteLock lock0 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock0 = lock0.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock0 = lock0.writeLock();


    /**
     * Reset the list.
     */
    public void clear() {
        writeLock0.lock();
        try {
        freeSpace.clear();
        freeSpace.add(new BlockRange(firstFreeBlock,
                Integer.MAX_VALUE - firstFreeBlock));
        } finally {
            writeLock0.unlock();
        }
    }

    private static final ReentrantReadWriteLock lock1 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock1 = lock1.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock1 = lock1.writeLock();


    /**
     * Allocate a number of blocks and mark them as used.
     *
     * @param length the number of bytes to allocate
     * @return the start position in bytes
     */
    public long allocate(int length) {
        writeLock1.lock();
        try {
        int blocks = getBlockCount(length);
        BlockRange x = null;
        for (BlockRange b : freeSpace) {
            if (b.blocks >= blocks) {
                x = b;
                break;
            }
        }
        long pos = getPos(x.start);
        if (x.blocks == blocks) {
            freeSpace.remove(x);
        } else {
            x.start += blocks;
            x.blocks -= blocks;
        }
        return pos;
        } finally {
            writeLock1.unlock();
        }
    }

    private static final ReentrantReadWriteLock lock2 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock2 = lock2.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock2 = lock2.writeLock();


    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public void markUsed(long pos, int length) {
        writeLock2.lock();
        try {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        BlockRange x = new BlockRange(start, blocks);
        BlockRange prev = freeSpace.floor(x);
        if (prev == null) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL, "Free space already marked");
        }
        if (prev.start == start) {
            if (prev.blocks == blocks) {
                // match
                freeSpace.remove(prev);
            } else {
                // cut the front
                prev.start += blocks;
                prev.blocks -= blocks;
            }
        } else if (prev.start + prev.blocks == start + blocks) {
            // cut the end
            prev.blocks -= blocks;
        } else {
            // insert an entry
            x.start = start + blocks;
            x.blocks = prev.start + prev.blocks - x.start;
            freeSpace.add(x);
            prev.blocks = start - prev.start;
        }
        } finally {
            writeLock2.unlock();
        }
    }

    private static final ReentrantReadWriteLock lock3 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock3 = lock3.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock3 = lock3.writeLock();


    /**
     * Mark the space as free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public void free(long pos, int length) {
        writeLock3.lock();
        try {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        BlockRange x = new BlockRange(start, blocks);
        BlockRange next = freeSpace.ceiling(x);
        if (next == null) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL, "Free space sentinel is missing");
        }
        BlockRange prev = freeSpace.lower(x);
        if (prev != null) {
            if (prev.start + prev.blocks == start) {
                // extend the previous entry
                prev.blocks += blocks;
                if (prev.start + prev.blocks == next.start) {
                    // merge with the next entry
                    prev.blocks += next.blocks;
                    freeSpace.remove(next);
                }
                return;
            }
        }
        if (start + blocks == next.start) {
            // extend the next entry
            next.start -= blocks;
            next.blocks += blocks;
            return;
        }
        freeSpace.add(x);
        } finally {
            writeLock3.unlock();
        }
    }

    private long getPos(int block) {
        return (long) block * (long) blockSize;
    }

    private int getBlock(long pos) {
        return (int) (pos / blockSize);
    }

    private int getBlockCount(int length) {
        if (length <= 0) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL, "Free space invalid length");
        }
        return MathUtils.roundUpInt(length, blockSize) / blockSize;
    }

    @Override
    public String toString() {
        return freeSpace.toString();
    }

    /**
     * A range of free blocks.
     */
    private static final class BlockRange implements Comparable<BlockRange> {

        /**
         * The starting point (the block number).
         */
        public int start;

        /**
         * The length, in blocks.
         */
        public int blocks;

        public BlockRange(int start, int blocks) {
            this.start = start;
            this.blocks = blocks;
        }

        @Override
        public int compareTo(BlockRange o) {
            return Integer.compare(start, o.start);
        }

        @Override
        public String toString() {
            if (blocks + start == Integer.MAX_VALUE) {
                return Integer.toHexString(start) + "-";
            }
            return Integer.toHexString(start) + "-" +
                Integer.toHexString(start + blocks - 1);
        }

    }

}
