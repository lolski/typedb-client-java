/*
 * Copyright (C) 2021 Vaticle
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.vaticle.typedb.client.connection;

import com.google.protobuf.ByteString;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBOptions;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typedb.client.common.rpc.TypeDBStub;
import com.vaticle.typedb.client.stream.RequestTransmitter;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import io.grpc.ManagedChannel;

import javax.annotation.Nullable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.client.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.client.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static com.vaticle.typedb.client.common.rpc.RequestBuilder.Client.closeReq;
import static com.vaticle.typedb.client.common.rpc.RequestBuilder.Client.pulseReq;
import static com.vaticle.typedb.common.util.Objects.className;

public abstract class TypeDBClientImpl implements TypeDBClient {

    private static final int PULSE_INTERVAL_MILLIS = 5_000;
    private static final String TYPEDB_CLIENT_RPC_THREAD_NAME = "typedb-client-rpc";

    @Nullable
    protected final Integer idleTimeoutMillis;
    private final RequestTransmitter transmitter;
    private final TypeDBDatabaseManagerImpl databaseMgr;
    private final ConcurrentMap<ByteString, TypeDBSessionImpl> sessions;
    private final Timer pulse;
    private final AtomicBoolean isOpen;

    protected TypeDBClientImpl(@Nullable Integer idleTimeoutMillis, int parallelisation) {
        this.idleTimeoutMillis = idleTimeoutMillis;
        NamedThreadFactory threadFactory = NamedThreadFactory.create(TYPEDB_CLIENT_RPC_THREAD_NAME);
        transmitter = new RequestTransmitter(parallelisation, threadFactory);
        databaseMgr = new TypeDBDatabaseManagerImpl(this);
        sessions = new ConcurrentHashMap<>();
        pulse = new Timer();
        isOpen = new AtomicBoolean(true);
    }

    public static int calculateParallelisation() {
        int cores = Runtime.getRuntime().availableProcessors();
        if (cores <= 4) return 2;
        else if (cores <= 9) return 3;
        else if (cores <= 16) return 4;
        else return (int) Math.ceil(cores / 4.0);
    }

    @Override
    public TypeDBDatabaseManagerImpl databases() {
        return databaseMgr;
    }

    @Override
    public TypeDBSessionImpl session(String database, TypeDBSession.Type type) {
        return session(database, type, TypeDBOptions.core());
    }

    @Override
    public TypeDBSessionImpl session(String database, TypeDBSession.Type type, TypeDBOptions options) {
        TypeDBSessionImpl session = new TypeDBSessionImpl(this, database, type, options);
        assert !sessions.containsKey(session.ID());
        sessions.put(session.ID(), session);
        return session;
    }

    void removeSession(TypeDBSessionImpl session) {
        sessions.remove(session.ID());
    }

    public abstract ByteString ID();

    @Override
    public boolean isOpen() {
        return !channel().isShutdown();
    }

    public abstract ManagedChannel channel();

    public abstract TypeDBStub stub();

    RequestTransmitter transmitter() {
        return transmitter;
    }

    protected void pulseActivate() {
        pulse.scheduleAtFixedRate(this.new PulseTask(), 0, PULSE_INTERVAL_MILLIS);
    }

    protected void pulseDeactivate() {
        pulse.cancel();
    }

    @Override
    public boolean isCluster() {
        return false;
    }

    @Override
    public Cluster asCluster() {
        throw new TypeDBClientException(ILLEGAL_CAST, className(TypeDBClient.Cluster.class));
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            closeResources();
        }
    }

    protected void closeResources() {
        sessions.values().forEach(TypeDBSessionImpl::close);
        stub().clientClose(closeReq(ID()));
        try {
            channel().shutdown().awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new TypeDBClientException(UNEXPECTED_INTERRUPTION);
        }
        transmitter.close();
    }

    private class PulseTask extends TimerTask {

        @Override
        public void run() {
            if (!isOpen()) return;
            boolean alive;
            try {
                alive = stub().clientPulse(pulseReq(ID())).getAlive();
            } catch (TypeDBClientException exception) {
                alive = false;
            }
            if (!alive) {
                close();
            }
        }
    }
}
