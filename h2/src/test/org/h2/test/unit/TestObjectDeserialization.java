/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Noah Fontes <nfontes@invectorate.com>
 */
package org.h2.test.unit;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

import java.util.function.Consumer;

/**
 * Tests the ability to deserialize objects that are not part of the system
 * class-loading scope.
 */
public class TestObjectDeserialization extends TestBase {

    private static final String CLAZZ = "org.h2.test.unit.SampleObject";
    private static final String OBJECT =
        "aced00057372001d6f72672e68322e746573742e756" +
        "e69742e53616d706c654f626a65637400000000000000010200007870";

    /**
     * The thread context class loader was used.
     */
    protected boolean usesThreadContextClassLoader;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        System.setProperty("h2.useThreadContextClassLoader", "true");
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() {
        testThreadContextClassLoader();
    }

    private void testThreadContextClassLoader() {
        usesThreadContextClassLoader = false;
        Thread.currentThread().setContextClassLoader(new TestClassLoader(CLAZZ, (b)-> usesThreadContextClassLoader=b));
        assertThrows(ErrorCode.DESERIALIZATION_FAILED_1,
                () -> JdbcUtils.deserialize(StringUtils.convertHexToBytes(OBJECT), null));
        assertTrue(usesThreadContextClassLoader);
    }

    /**
     * A special class loader.
     */
    private static class TestClassLoader extends ClassLoader {

        private final Consumer<Boolean> classProcessing;
        private final String clazz;

        public TestClassLoader(String clazz, Consumer<Boolean> classProcessing) {
            super();
            this.clazz = clazz;
            this.classProcessing = classProcessing;
        }


    private static final ReentrantReadWriteLock lock0 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock0 = lock0.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock0 = lock0.writeLock();

        @Override
        protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
        writeLock0.lock();
        try {
            if (name.equals(clazz)) {
                classProcessing.accept(true);
            }
            return super.loadClass(name, resolve);
            } finally {
            writeLock0.unlock();
        }
    }

    }

}
