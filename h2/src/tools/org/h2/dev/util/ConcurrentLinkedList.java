/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.Iterator;

/**
 * A very simple linked list that supports concurrent access.
 * Internally, it uses immutable objects.
 * It uses recursion and is not meant for long lists.
 *
 * @param <K> the key type
 */
public class ConcurrentLinkedList<K> {

    /**
     * The sentinel entry.
     */
    static final Entry<?> NULL = new Entry<>(null, null);

    /**
     * The head entry.
     */
    @SuppressWarnings("unchecked")
    volatile Entry<K> head = (Entry<K>) NULL;

    /**
     * Get the first element, or null if none.
     *
     * @return the first element
     */
    public K peekFirst() {
        Entry<K> x = head;
        return x.obj;
    }

    /**
     * Get the last element, or null if none.
     *
     * @return the last element
     */
    public K peekLast() {
        Entry<K> x = head;
        while (x != NULL && x.next != NULL) {
            x = x.next;
        }
        return x.obj;
    }

    private static final ReentrantReadWriteLock lock0 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock0 = lock0.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock0 = lock0.writeLock();


    /**
     * Add an element at the end.
     *
     * @param obj the element
     */
    public void add(K obj) {
        writeLock0.lock();
        try {
        head = Entry.append(head, obj);
        } finally {
            writeLock0.unlock();
        }
    }

    private static final ReentrantReadWriteLock lock1 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock1 = lock1.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock1 = lock1.writeLock();


    /**
     * Remove the first element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public boolean removeFirst(K obj) {
        writeLock1.lock();
        try {
        if (head.obj != obj) {
            return false;
        }
        head = head.next;
        return true;
        } finally {
            writeLock1.unlock();
        }
    }

    private static final ReentrantReadWriteLock lock2 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock2 = lock2.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock2 = lock2.writeLock();


    /**
     * Remove the last element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public boolean removeLast(K obj) {
        writeLock2.lock();
        try {
        if (peekLast() != obj) {
            return false;
        }
        head = Entry.removeLast(head);
        return true;
        } finally {
            writeLock2.unlock();
        }
    }

    /**
     * Get an iterator over all entries.
     *
     * @return the iterator
     */
    public Iterator<K> iterator() {
        return new Iterator<K>() {

            Entry<K> current = head;

            @Override
            public boolean hasNext() {
                return current != NULL;
            }

            @Override
            public K next() {
                K x = current.obj;
                current = current.next;
                return x;
            }

        };
    }

    /**
     * An entry in the linked list.
     */
    private static class Entry<K> {
        final K obj;
        Entry<K> next;

        Entry(K obj, Entry<K> next) {
            this.obj = obj;
            this.next = next;
        }

        @SuppressWarnings("unchecked")
        static <K> Entry<K> append(Entry<K> list, K obj) {
            if (list == NULL) {
                return new Entry<>(obj, (Entry<K>) NULL);
            }
            return new Entry<>(list.obj, append(list.next, obj));
        }

        @SuppressWarnings("unchecked")
        static <K> Entry<K> removeLast(Entry<K> list) {
            if (list == NULL || list.next == NULL) {
                return (Entry<K>) NULL;
            }
            return new Entry<>(list.obj, removeLast(list.next));
        }

    }

}
