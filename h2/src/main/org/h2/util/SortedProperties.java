/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import org.h2.store.fs.FileUtils;

/**
 * Sorted properties file.
 * This implementation requires that store() internally calls keys().
 */
public class SortedProperties extends Properties {

    private static final long serialVersionUID = 1L;


    private static final ReentrantReadWriteLock lock0 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock0 = lock0.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock0 = lock0.writeLock();

    @Override
    public Enumeration<Object> keys() {
        writeLock0.lock();
        try {
        ArrayList<Object> v = new ArrayList<>();
        for (Object o : keySet()) {
            v.add(o.toString());
        }
        v.sort(null);
        return Collections.enumeration(v);
        } finally {
            writeLock0.unlock();
        }
    }

    /**
     * Get a boolean property value from a properties object.
     *
     * @param prop the properties object
     * @param key the key
     * @param def the default value
     * @return the value if set, or the default value if not
     */
    public static boolean getBooleanProperty(Properties prop, String key,
            boolean def) {
        try {
            return Utils.parseBoolean(prop.getProperty(key, null), def, true);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return def;
        }
    }

    /**
     * Get an int property value from a properties object.
     *
     * @param prop the properties object
     * @param key the key
     * @param def the default value
     * @return the value if set, or the default value if not
     */
    public static int getIntProperty(Properties prop, String key, int def) {
        String value = prop.getProperty(key, Integer.toString(def));
        try {
            return Integer.decode(value);
        } catch (Exception e) {
            e.printStackTrace();
            return def;
        }
    }

    /**
     * Get a string property value from a properties object.
     *
     * @param prop the properties object
     * @param key the key
     * @param def the default value
     * @return the value if set, or the default value if not
     */
    public static String getStringProperty(Properties prop, String key, String def) {
        return prop.getProperty(key, def);
    }

    private static final ReentrantReadWriteLock lock1 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock1 = lock1.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock1 = lock1.writeLock();


    /**
     * Load a properties object from a file.
     *
     * @param fileName the name of the properties file
     * @return the properties object
     * @throws IOException on failure
     */
    public static SortedProperties loadProperties(String fileName)
            throws IOException {
        writeLock1.lock();
        try {
        SortedProperties prop = new SortedProperties();
        if (FileUtils.exists(fileName)) {
            try (InputStream in = FileUtils.newInputStream(fileName)) {
                prop.load(new InputStreamReader(in, StandardCharsets.ISO_8859_1));
            }
        }
        return prop;
        } finally {
            writeLock1.unlock();
        }
    }

    private static final ReentrantReadWriteLock lock2 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock2 = lock2.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock2 = lock2.writeLock();


    /**
     * Store a properties file. The header and the date is not written.
     *
     * @param fileName the target file name
     * @throws IOException on failure
     */
    public void store(String fileName) throws IOException {
        writeLock2.lock();
        try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        store(out, null);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        InputStreamReader reader = new InputStreamReader(in, StandardCharsets.ISO_8859_1);
        LineNumberReader r = new LineNumberReader(reader);
        Writer w;
        try {
            w = new OutputStreamWriter(FileUtils.newOutputStream(fileName, false), StandardCharsets.ISO_8859_1);
        } catch (Exception e) {
            throw new IOException(e.toString(), e);
        }
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(w))) {
            while (true) {
                String line = r.readLine();
                if (line == null) {
                    break;
                }
                if (!line.startsWith("#")) {
                    writer.print(line + "\n");
                }
            }
        }
        } finally {
            writeLock2.unlock();
        }
    }

    private static final ReentrantReadWriteLock lock3 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock3 = lock3.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock3 = lock3.writeLock();


    /**
     * Convert the map to a list of line in the form key=value.
     *
     * @return the lines
     */
    public String toLines() {
        writeLock3.lock();
        try {
        StringBuilder buff = new StringBuilder();
        for (Entry<Object, Object> e : new TreeMap<>(this).entrySet()) {
            buff.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        return buff.toString();
        } finally {
            writeLock3.unlock();
        }
    }

    /**
     * Convert a String to a map.
     *
     * @param s the string
     * @return the map
     */
    public static SortedProperties fromLines(String s) {
        SortedProperties p = new SortedProperties();
        for (String line : StringUtils.arraySplit(s, '\n', true)) {
            int idx = line.indexOf('=');
            if (idx > 0) {
                p.put(line.substring(0, idx), line.substring(idx + 1));
            }
        }
        return p;
    }

}
