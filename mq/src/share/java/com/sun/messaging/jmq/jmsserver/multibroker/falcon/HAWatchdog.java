/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2000-2010 Oracle and/or its affiliates. All rights reserved.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common Development
 * and Distribution License("CDDL") (collectively, the "License").  You
 * may not use this file except in compliance with the License.  You can
 * obtain a copy of the License at
 * https://glassfish.dev.java.net/public/CDDL+GPL_1_1.html
 * or packager/legal/LICENSE.txt.  See the License for the specific
 * language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice in each
 * file and include the License file at packager/legal/LICENSE.txt.
 *
 * GPL Classpath Exception:
 * Oracle designates this particular file as subject to the "Classpath"
 * exception as provided by Oracle in the GPL Version 2 section of the License
 * file that accompanied this code.
 *
 * Modifications:
 * If applicable, add the following below the License Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyright [year] [name of copyright owner]"
 *
 * Contributor(s):
 * If you wish your version of this file to be governed by only the CDDL or
 * only the GPL Version 2, indicate your decision by adding "[Contributor]
 * elects to include this software in this distribution under the [CDDL or GPL
 * Version 2] license."  If you don't indicate a single choice of license, a
 * recipient has the option to distribute your version of this file under
 * either the CDDL, the GPL Version 2 or to extend the choice of license to
 * its licensees as provided above.  However, if you add GPL Version 2 code
 * and therefore, elected the GPL Version 2 license, then the option applies
 * only if the new code is made subject to such option by the copyright
 * holder.
 */

/*
 * @(#)HAWatchdog.java	1.10 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.multibroker.falcon;

import java.io.*;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.multibroker.Cluster;
import com.sun.messaging.jmq.jmsserver.multibroker.ClusterGlobals;
import com.sun.messaging.jmq.jmsserver.multibroker.Protocol;
import com.sun.messaging.jmq.jmsserver.multibroker.MessageBusCallback;
import com.sun.messaging.jmq.jmsserver.multibroker.CallbackDispatcher;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;

/**
 * This class encapsulates the broker HA state management logic.
 * It uses a dedicated thread to monitor the HA cluster and trigger
 * broker HAState transitions.
 *
 * <pre>
 * HA Watchdog state engine:

 * ==================================================================
 * State            Event                   Action          New state
 * ==================================================================

 * Start        -->                     --> Initialize  --> HA_ACTIVATE

 * HA_WAITING   --> Active broker down  -->             --> HA_ACTIVATE
 * HA_WAITING   --> Lease timeout       -->             --> HA_ACTIVATE
 * HA_WAITING   --> ActiveBrokerUpdate  --> Reset Timer --> HA_UPDATE

 * HA_ACTIVATE  --> Election success    --> Go active!  --> HA_ADVERTIZE
 * HA_ACTIVATE  --> Election failure    -->             --> HA_WAITING

 * HA_UPDATE    -->                     --> Reset timer --> HA_WAITING

 * HA_ACTIVE    --> Time to send update -->             --> HA_ADVERTIZE
 * HA_ADVERTIZE -->                     --> Reset timer --> HA_ACTIVE

 * ANY          --> Shutdown Command    -->             --> HA_SHUTDOWN

 * HA_SHUTDOWN  -->                     --> Shutdown watchdog.
 * </pre>
 */
public class HAWatchdog extends Thread {
    private static boolean DEBUG = false;
    private Logger logger = Globals.getLogger();
    private static final BrokerResources br = Globals.getBrokerResources();

    private MessageBusCallback cb = null;
    private CallbackDispatcher cbDispatcher = null;
    private Cluster c = null;
    private Protocol mbus = null;

    private BrokerAddress currentActiveBroker = null;
    private Object eventObject = null;
    private int state;

    private static final int HA_WAITING = 1;
    private static final int HA_UPDATE = 2;
    private static final int HA_ACTIVATE = 3;
    private static final int HA_ACTIVE = 4;
    private static final int HA_ADVERTIZE = 5;
    private static final int HA_SHUTDOWN = 6;

    private static final long HA_LEASE_TIMEOUT = 90000;
    private static final long HA_ACTIVE_UPDATE_TIMEOUT = 30000;

    public HAWatchdog(MessageBusCallback cb,
        CallbackDispatcher cbDispatcher,
        Cluster c, Protocol mbus) {
        if (DEBUG) {
            logger.log(Logger.DEBUG, "HAWatchdog started");
        }

        this.cb = cb;
        this.cbDispatcher = cbDispatcher;
        this.c = c;
        this.mbus = mbus;

        eventObject = new Object();
        updateActiveBroker(null);
        switchState(HA_ACTIVATE, false);
        setName("HAWatchdog");
        start();
    }

    public void shutdown() {
        switchState(HA_SHUTDOWN, true);
    }

    public void run() {
        boolean shutdown = false;

        while (! shutdown) {
            switch (state) {
            case HA_WAITING:
                waitTimerEvent(HA_LEASE_TIMEOUT, HA_ACTIVATE);
                break;

            case HA_UPDATE:
                switchState(HA_WAITING, false);
                break;

            case HA_ACTIVATE:
                electActiveBroker();
                break;

            case HA_ACTIVE:
                waitTimerEvent(HA_ACTIVE_UPDATE_TIMEOUT, HA_ADVERTIZE);
                break;

            case HA_ADVERTIZE:
                sendActiveBrokerUpdate();
                switchState(HA_ACTIVE, false);
                break;

            case HA_SHUTDOWN:
                shutdown = true;
                break;
            }
        }
    }

    /**
     * Monitor events and state changes.
     */
    private void waitTimerEvent(long timeout, int newState) {
        synchronized (eventObject) {
            if (DEBUG) {
                logger.log(Logger.DEBUG,
    "HAWatchdog sleeping. Current state = {0}, active = {1}",
                    Integer.toString(state),
                    currentActiveBroker);
            }

            int initialState = state;

            while (state == initialState && timeout > 0) {
                long start = System.currentTimeMillis();
                try {
                    eventObject.wait(timeout);
                }
                catch (InterruptedException e) {
                    // received interrupted exception
                }
                timeout -= (System.currentTimeMillis() - start);
            }
            if (timeout <= 0) {
                switchState(newState, false);
            }
        }
    }

    /**
     * Try to become the active broker.
     */
    private void electActiveBroker() {
        if (DEBUG) {
            logger.log(Logger.DEBUG, "HAWatchdog: electActiveBroker.");
        }

        int lockStatus = mbus.lockResource("HA:ActiveBroker",0, null);
        if (lockStatus == ClusterGlobals.MB_LOCK_SUCCESS) {
            updateActiveBroker(Globals.getMyAddress());
            switchState(HA_ADVERTIZE, false);
            cbDispatcher.goHAActive();
        }
        else {
            switchState(HA_WAITING, false);
        }
    }

    /**
     * Broadcast active broker update to all the standby brokers.
     */
    private void sendActiveBrokerUpdate() {
        if (DEBUG) {
            logger.log(Logger.DEBUG, "HAWatchdog: sendActiveBrokerUpdate.");
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        try {
            dos.writeInt(ClusterGlobals.VERSION_210);

            dos.flush();
            bos.flush();
        }
        catch (Exception e) {}

        byte[] buf = bos.toByteArray();
        try {
            c.broadcast(ClusterGlobals.MB_HA_ACTIVE_UPDATE, buf);
        }
        catch (IOException e) {}
    }

    /**
     * This method is called by ClusterGlobals whenever a broker goes
     * down.
     */
    public void handleBrokerDown(BrokerAddress b) {
        if (DEBUG) {
            logger.log(Logger.DEBUG,
    "HAWatchdog: broker down = {0}. Active Broker = {1}",
                b, currentActiveBroker);
        }

        synchronized (eventObject) {
            if (currentActiveBroker == null ||
                currentActiveBroker.equals(b)) {
                updateActiveBroker(null);
                switchState(HA_ACTIVATE, true);
            }
        }
    }

    /**
     * This method is called by ClusterGlobals whenever a broker joins the
     * cluster.
     */
    public void handleBrokerUp(BrokerAddress b) {
        if (DEBUG) {
            logger.log(Logger.DEBUG,
    "HAWatchdog: broker started = {0}. Active Broker = {1}",
                b, currentActiveBroker);
        }
    }

    /**
     * This method is called by ClusterGlobals whenever a active broker update
     * packet is received.
     */
    public void handleActiveBrokerUpdate(BrokerAddress b, byte[] pkt)
    {
        if (DEBUG) {
            logger.log(Logger.DEBUG,
                "HAWatchdog: Active broker update from : {0}", b);
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(pkt);
        DataInputStream dis = new DataInputStream(bis);

        try {
            int pktVersion = dis.readInt();
            if (pktVersion > ClusterGlobals.VERSION_210) {
                logger.log(Logger.WARNING, br.W_MBUS_BAD_VERSION,
                    Integer.toString(pktVersion),
                    Integer.toString(ClusterGlobals.MB_HA_ACTIVE_UPDATE));
                return;
            }
        }
        catch (Exception e) {
            return;
        }

        synchronized (eventObject) {
            updateActiveBroker(b);
            switchState(HA_UPDATE, true);
        }
    }

    private void switchState(int newstate, boolean notify) {
        if (DEBUG) {
            logger.log(Logger.DEBUGMED,
    "HAWatchdog: State transition. Old = {0}, New = {1}.",
                Integer.toString(state),
                Integer.toString(newstate));
        }

        synchronized (eventObject) {
            state = newstate;
            if (notify)
                eventObject.notify();
        }
    }

    private void updateActiveBroker(BrokerAddress b) {
        currentActiveBroker = b;
    }
}

/*
 * EOF
 */
