/*
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.vertx.mods

import org.vertx.java.core.logging.Logger

import org.voltdb.VoltTable
import org.voltdb.ClientResponseImpl
import org.voltdb.exceptions.SerializableException

import org.voltdb.client.*

import java.util.concurrent.*
import java.util.concurrent.atomic.*

import groovy.transform.TypeChecked

@TypeChecked
class VoltClient {
    AtomicBoolean doShutdown = new AtomicBoolean(false)
    AtomicStampedReference<Client> clref = new AtomicStampedReference<Client>(null, 0)
    volatile int clientStamp = 0;
    List<String> hosts = new ArrayList<String>();

    Logger log

    static final ExecutorService es = Executors.newCachedThreadPool(new ThreadFactory() {
        public Thread newThread(Runnable arg0) {
            Thread thread = new Thread(arg0, "Volt Connection Retrier");
            thread.setDaemon(true);
            return thread;
        }
    })

    Closure connect = {  String hostname ->
        int[] stamp = new int[1]
        Client client = clref.get(stamp)
        int sleep = 1000
        while (!doShutdown.get() && client != null && clref.getStamp() == stamp[0]) {
            Exception connExc = null
            try {
                client.createConnection(hostname)
                log.info "Connected to ${hostname}"
                return
            } catch (java.net.ConnectException ex) {
                log.warn "failed connection to ${hostname}: [${ex.getClass().getName()}] ${ex.getMessage()}"
            } catch (java.io.IOException ex) {
                connExc = ex
                return
            } catch (Exception ex) {
                connExc = ex
            } finally {
                if (connExc) {
                    log.error "failed connection to ${hostname}: [${connExc.getClass().getName()}] ${connExc.getMessage()}"
                    try { client.close()} catch (Exception ignoreIt) {}
                    clref.compareAndSet(client,null,stamp[0],stamp[0]+1)
                }
            }
            try { Thread.sleep(sleep) } catch (InterruptedException ignoreIt) {}
            if (sleep < 8000) sleep += sleep;
        }
    }

    class StatusListener extends ClientStatusListenerExt {
        Closure connectionLost

        public void connectionLost(
                String hostname,
                int port,
                int connectionsLeft,
                ClientStatusListenerExt.DisconnectCause cause) {
            if (!doShutdown.get()) {
                log.warn "detected connection loss to ${hostname}:${port}"
                es.submit( connectionLost.curry("${hostname}:${port}") as Runnable)
            }
        }
    }

    ClientConfig clconf = new ClientConfig("","", new StatusListener(connectionLost: connect))

    boolean createOrReplaceClient(Client old, int oldStamp) {
        Client young = ClientFactory.createClient(clconf)
        Client closeThis = young
        if (clref.compareAndSet(old,young,oldStamp,oldStamp+1)) {
            closeThis = old
            clientStamp = oldStamp + 1
        }

        try { closeThis?.close() } catch (Exception ignoreIt) {}
        if (closeThis == old) connectToAtLeastOne(hosts)
        return closeThis == old
    }

    public VoltClient(List<String> hostAndPorts, Logger logger ) {
        log = logger
        hosts.addAll(hostAndPorts)
        createOrReplaceClient(null,0)
    }

    public void shutdown() {
        doShutdown.set(true)
        int[] stamp = new int[1]
        Client client = clref.get(stamp)
        if (clref.compareAndSet(client,null,stamp[0],stamp[0]+1)) {
            try { client?.close() } catch (Exception ignoreIt) {}
        }
    }

    public Client getClient() {
        int [] stamp = new int[1]
        Client client = clref.get(stamp)
        while (client == null) {
            createOrReplaceClient(client,stamp[0])
            client = clref.get(stamp)
        }
        return client
    }

    void connectToAtLeastOne( List<String> hostAndPorts) {
        if ( !hostAndPorts) return

        final CountDownLatch connections = new CountDownLatch(1)

        hostAndPorts.each {
            final Closure curriedConnect = connect.curry(it)
            Thread.startDaemon("Volt connection attempter to ${it}") {
                curriedConnect()
                connections.countDown()
            }
        }
        connections.await()
    }

    boolean callProcedure(Closure callback, String procName, Object ... params) {
        try {
            return getClient().callProcedure( callback as ProcedureCallback, procName, params)
        } catch (Exception ex) {
            callback(new ClientResponseImpl(
                    (byte)-7,
                    new VoltTable[0],
                    "immediate invocation exception",
                    new SerializableException(ex)
            ))
            return false
        }
    }

    public boolean callProcedure( VoltInvocation invocation, Closure callback ) {
        return callProcedure( callback, invocation.name, invocation.params)
    }
}
