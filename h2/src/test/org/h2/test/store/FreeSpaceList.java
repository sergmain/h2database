/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.ArrayList;
import java.util.List;

import org.h2.mvstore.DataUtils;
import org.h2.util.MathUtils;

/**
 * A list that maintains ranges of free space (in blocks).
 */
public class FreeSpaceList {

    /**
     * The first usable block.
     */
    private final int firstFreeBlock;

    /**
     * The block size in bytes.
     */
    private final int blockSize;

    private List<BlockRange> freeSpaceList = new ArrayList<>();

    public FreeSpaceList(int firstFreeBlock, int blockSize) {
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
        freeSpaceList.clear();
        freeSpaceList.add(new BlockRange(firstFreeBlock,
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
        int required = getBlockCount(length);
        for (BlockRange pr : freeSpaceList) {
            if (pr.length >= required) {
                int result = pr.start;
                this.markUsed(pr.start * blockSize, length);
                return result * blockSize;
            }
        }
        throw DataUtils.newMVStoreException(
                DataUtils.ERROR_INTERNAL,
                "Could not find a free page to allocate");
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
        int start = (int) (pos / blockSize);
        int required = getBlockCount(length);
        BlockRange found = null;
        int i = 0;
        for (BlockRange pr : freeSpaceList) {
            if (start >= pr.start && start < (pr.start + pr.length)) {
                found = pr;
                break;
            }
            i++;
        }
        if (found == null) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL,
                    "Cannot find spot to mark as used in free list");
        }
        if (start + required > found.start + found.length) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL,
                    "Runs over edge of free space");
        }
        if (found.start == start) {
            // if the used space is at the beginning of a free-space-range
            found.start += required;
            found.length -= required;
            if (found.length == 0) {
                // if the free-space-range is now empty, remove it
                freeSpaceList.remove(i);
            }
        } else if (found.start + found.length == start + required) {
            // if the used space is at the end of a free-space-range
            found.length -= required;
        } else {
            // it's in the middle, so split the existing entry
            int length1 = start - found.start;
            int start2 = start + required;
            int length2 = found.start + found.length - start - required;

            found.length = length1;
            BlockRange newRange = new BlockRange(start2, length2);
            freeSpaceList.add(i + 1, newRange);
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
        int start = (int) (pos / blockSize);
        int required = getBlockCount(length);
        BlockRange found = null;
        int i = 0;
        for (BlockRange pr : freeSpaceList) {
            if (pr.start > start) {
                found = pr;
                break;
            }
            i++;
        }
        if (found == null) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL,
                    "Cannot find spot to mark as unused in free list");
        }
        if (start + required == found.start) {
            // if the used space is adjacent to the beginning of a
            // free-space-range
            found.start = start;
            found.length += required;
            // compact: merge the previous entry into this one if
            // they are now adjacent
            if (i > 0) {
                BlockRange previous = freeSpaceList.get(i - 1);
                if (previous.start + previous.length == found.start) {
                    previous.length += found.length;
                    freeSpaceList.remove(i);
                }
            }
            return;
        }
        if (i > 0) {
            // if the used space is adjacent to the end of a free-space-range
            BlockRange previous = freeSpaceList.get(i - 1);
            if (previous.start + previous.length == start) {
                previous.length += required;
                return;
            }
        }

        // it is between 2 entries, so add a new one
        BlockRange newRange = new BlockRange(start, required);
        freeSpaceList.add(i, newRange);
        } finally {
            writeLock3.unlock();
        }
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
        return freeSpaceList.toString();
    }

    /**
     * A range of free blocks.
     */
    private static final class BlockRange {

        /**
         * The starting point, in blocks.
         */
        int start;

        /**
         * The length, in blocks.
         */
        int length;

        public BlockRange(int start, int length) {
            this.start = start;
            this.length = length;
        }

        @Override
        public String toString() {
            if (start + length == Integer.MAX_VALUE) {
                return Integer.toHexString(start) + "-";
            }
            return Integer.toHexString(start) + "-" +
                Integer.toHexString(start + length - 1);
        }

    }

}
