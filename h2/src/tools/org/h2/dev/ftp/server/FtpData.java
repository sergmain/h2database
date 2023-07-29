/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.ftp.server;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;

/**
 * The implementation of the data channel of the FTP server.
 */
public class FtpData extends Thread {

    private final FtpServer server;
    private final InetAddress address;
    private ServerSocket serverSocket;
    private volatile Socket socket;
    private final boolean active;
    private final int port;

    FtpData(FtpServer server, InetAddress address, ServerSocket serverSocket) {
        this.server = server;
        this.address = address;
        this.serverSocket = serverSocket;
        this.port = 0;
        this.active = false;
    }

    FtpData(FtpServer server, InetAddress address, int port) {
        this.server = server;
        this.address = address;
        this.port = port;
        this.active = true;
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                Socket s = serverSocket.accept();
                if (s.getInetAddress().equals(address)) {
                    server.trace("Data connected:" + s.getInetAddress() + " expected:" + address);
                    socket = s;
                    notifyAll();
                } else {
                    server.trace("Data REJECTED:" + s.getInetAddress() + " expected:" + address);
                    close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void connect() throws IOException {
        if (active) {
            socket = new Socket(address, port);
        } else {
            waitUntilConnected();
        }
    }

    private void waitUntilConnected() {
        while (serverSocket != null && socket == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        server.trace("connected");
    }

    /**
     * Close the socket.
     */
    void close() {
        serverSocket = null;
        socket = null;
    }

    private static final ReentrantReadWriteLock lock1 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock1 = lock1.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock1 = lock1.writeLock();


    /**
     * Read a file from a client.
     *
     * @param fileName the target file name
     */ void receive(String fileName) throws IOException {
        writeLock1.lock();
        try {
        connect();
        try {
            InputStream in = socket.getInputStream();
            OutputStream out = FileUtils.newOutputStream(fileName, false);
            IOUtils.copy(in, out);
            out.close();
        } finally {
            socket.close();
        }
        server.trace("closed");
        } finally {
            writeLock1.unlock();
        }
    }

    private static final ReentrantReadWriteLock lock2 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock2 = lock2.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock2 = lock2.writeLock();


    /**
     * Send a file to the client. This method waits until the client has
     * connected.
     *
     * @param fileName the source file name
     * @param skip the number of bytes to skip
     */ void send(String fileName, long skip) throws IOException {
        writeLock2.lock();
        try {
        connect();
        try {
            OutputStream out = socket.getOutputStream();
            InputStream in = FileUtils.newInputStream(fileName);
            IOUtils.skipFully(in, skip);
            IOUtils.copy(in, out);
            in.close();
        } finally {
            socket.close();
        }
        server.trace("closed");
        } finally {
            writeLock2.unlock();
        }
    }

    private static final ReentrantReadWriteLock lock3 = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock3 = lock3.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock3 = lock3.writeLock();


    /**
     * Wait until the client has connected, and then send the data to him.
     *
     * @param data the data to send
     */ void send(byte[] data) throws IOException {
        writeLock3.lock();
        try {
        connect();
        try {
            OutputStream out = socket.getOutputStream();
            out.write(data);
        } finally {
            socket.close();
        }
        server.trace("closed");
        } finally {
            writeLock3.unlock();
        }
    }

}
