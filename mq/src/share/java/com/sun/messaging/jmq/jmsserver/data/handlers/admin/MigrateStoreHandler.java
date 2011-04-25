/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2000-2011 Oracle and/or its affiliates. All rights reserved.
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
 */ 

package com.sun.messaging.jmq.jmsserver.data.handlers.admin;

import java.util.Hashtable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Vector;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.Executors;

import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.util.ServiceType;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.BrokerStateHandler;
import com.sun.messaging.jmq.jmsserver.cluster.*;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;
import com.sun.messaging.jmq.jmsserver.core.BrokerMQAddress;
import com.sun.messaging.jmq.jmsserver.core.ClusterBroadcast;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;
import com.sun.messaging.jmq.jmsserver.cluster.ha.*;
import com.sun.messaging.jmq.jmsserver.service.HAMonitorService;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQBasicConnection;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.util.OperationNotAllowedException;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.PacketType;
import com.sun.messaging.jmq.io.Status;
import com.sun.messaging.jmq.util.admin.MessageType;
import com.sun.messaging.jmq.util.log.Logger;

public class MigrateStoreHandler extends AdminCmdHandler
{
    private static boolean DEBUG = getDEBUG();
    public static final String MAX_WAIT_ADMIN_CLIENT_PROP =
        Globals.IMQ+".cluster.migratestore.shutdown.maxWaitAdminClient";
    private static final int DEFAULT_MAX_WAIT_ADMIN_CLIENT = 60; //sec;

    public MigrateStoreHandler(AdminDataHandler parent) {
        super(parent);
    }

    /**
     * Handle the incomming administration message.
     *
     * @param con	The Connection the message came in on.
     * @param cmd_msg	The administration message
     * @param cmd_props The properties from the administration message
     */
    public boolean handle(IMQConnection con, Packet cmd_msg,
				       Hashtable cmd_props) {
        boolean noop = true;
        int status = Status.OK;
        String errMsg = "";
        if (DEBUG) {
            logger.log(Logger.INFO, this.getClass().getName() + ": " +
                       "Request migrate this broker''s store: " + cmd_props);
        }
        String brokerID = (String)cmd_props.get(MessageType.JMQ_BROKER_ID);
        if (brokerID != null) {
            logger.log(Logger.INFO, BrokerResources.I_ADMIN_MIGRATESTORE_TO, brokerID);
        } else {
            logger.log(Logger.INFO, BrokerResources.I_ADMIN_MIGRATESTORE);
        }

        HAMonitorService hamonitor = Globals.getHAMonitorService(); 

        if (Globals.getHAEnabled()) {
            status = Status.NOT_MODIFIED;
            errMsg =  rb.getKString(rb.E_OPERATION_NOT_SUPPORTED_IN_HA,
                          MessageType.getString(MessageType.MIGRATESTORE_BROKER));
            logger.log(Logger.ERROR, errMsg);
        } else if (hamonitor != null && hamonitor.inTakeover()) {
            status = Status.NOT_MODIFIED;
            errMsg =  rb.getString(rb.E_CANNOT_PROCEED_TAKEOVER_IN_PROCESS);
            logger.log(Logger.ERROR, this.getClass().getName() + ": " + errMsg);
        } else if (Globals.isJMSRAManagedBroker()) {
            status = Status.NOT_MODIFIED;
            errMsg =  "Can not process migration store request because this broker's life cycle is JMSRA managed";
            logger.log(Logger.ERROR, this.getClass().getName() + ": " + errMsg);
        } else  if (brokerID == null) {
            try {
                brokerID = getBrokerID();
            } catch (Throwable t) {
                status = Status.NOT_MODIFIED;
                errMsg = "Unable to get a connected broker to takeover this broker's store: "+t.getMessage(); 
                if ((t instanceof OperationNotAllowedException) &&
                     ((OperationNotAllowedException)t).getOperation().equals(
                      MessageType.getString(MessageType.MIGRATESTORE_BROKER))) {
                    logger.log(logger.ERROR, errMsg);
                } else {
                    logger.logStack(logger.ERROR, errMsg, t);
                }
            }
        }

        try {
            BrokerStateHandler.setExclusiveRequestLock(
                            ExclusiveRequest.MIGRATE_STORE);
        } catch (Throwable t) {
            status = Status.PRECONDITION_FAILED;
            if (t instanceof BrokerException) {
                status = ((BrokerException)t).getStatusCode();
            }
            errMsg = MessageType.getString(MessageType.MIGRATESTORE_BROKER)+": "+
                   Status.getString(status)+" - "+t.getMessage();
            logger.log(Logger.ERROR, errMsg);
            status = Status.NOT_MODIFIED;
        }
        try { //unset lock

        Long syncTimeout = null;
        final BrokerStateHandler bsh = Globals.getBrokerStateHandler();
        if (status == Status.OK) {

            try {
                syncTimeout = (Long)cmd_props.get(MessageType.JMQ_MIGRATESTORE_SYNC_TIMEOUT);

                ClusterManager cm =  Globals.getClusterManager();
                BrokerMQAddress self = (BrokerMQAddress)cm.getMQAddress(); 
                BrokerMQAddress master = (cm.getMasterBroker() == null ?
                    null:(BrokerMQAddress)cm.getMasterBroker().getBrokerURL());
                if (self.equals(master)) {
                    throw new BrokerException(
                        rb.getKString(rb.E_CHANGE_MASTER_BROKER_FIRST,
                        MessageType.getString(MessageType.MIGRATESTORE_BROKER)),
                        Status.NOT_ALLOWED);
                }
            } catch (Throwable t) { 
                status = Status.PRECONDITION_FAILED;
                if (t instanceof BrokerException) {
                    status = ((BrokerException)t).getStatusCode();
                }
                errMsg = MessageType.getString(MessageType.MIGRATESTORE_BROKER)+": "+
                       Status.getString(status)+" - "+t.getMessage();
                logger.log(Logger.ERROR, errMsg);
                status = Status.NOT_MODIFIED;
            }
        }

        SysMessageID replyMessageID = null;
        String replyStatusStr = null;

        try { //shutdown if !noop

        String hostport = null;
        if (status == Status.OK) {
            try {
                noop = false;
                hostport = bsh.takeoverME(brokerID, syncTimeout, con);
            } catch (BrokerException ex) {
                status = ex.getStatusCode();
                if (status == Status.BAD_REQUEST ||
                    status == Status.NOT_ALLOWED ||
                    status == Status.NOT_MODIFIED ||
                    status == Status.UNAVAILABLE ||
                    status == Status.PRECONDITION_FAILED) {

                    status = Status.PRECONDITION_FAILED;

                    if (ex instanceof OperationNotAllowedException) {
                        if (((OperationNotAllowedException)ex).getOperation().equals(
                            MessageType.getString(MessageType.MIGRATESTORE_BROKER))) {
                            status = Status.NOT_MODIFIED;
                            noop = true;
                        }
                    }
                    errMsg = Globals.getBrokerResources().getKString(
                             BrokerResources.E_FAIL_MIGRATESTORE_NOT_MIGRATED,
                             ex.getMessage());
                    if (noop) {
                        logger.log(Logger.ERROR, errMsg);
                    } else {
                        logger.logStack(Logger.ERROR, errMsg, ex);
                    }
                } else {
                    status = Status.EXPECTATION_FAILED;
                    errMsg = Globals.getBrokerResources().getKString(
                             BrokerResources.E_FAIL_TAKEOVERME,
                             brokerID, ex.getMessage());
                   logger.logStack(Logger.ERROR, errMsg, ex);
               }
            } 
        }

        if (status == Status.OK) {
            try {
                Globals.getClusterBroadcast().stopClusterIO(false, true, null);
            } catch (Throwable t) {
               logger.logStack(Logger.WARNING, "Failed to stop cluster IO", t);
            }
        }

        if (errMsg != null) {
            errMsg = errMsg+"["+Status.getString(status)+"]";
        }
        Hashtable p = new Hashtable();
        if (brokerID != null) {
            p.put(MessageType.JMQ_BROKER_ID, brokerID);
        }
        if (hostport != null) {
            p.put(MessageType.JMQ_MQ_ADDRESS, hostport);
        }
        Packet reply = new Packet(con.useDirectBuffers());
        reply.setPacketType(PacketType.OBJECT_MESSAGE);
        setProperties(reply, MessageType.MIGRATESTORE_BROKER_REPLY, status, errMsg, p);
        logger.log(logger.INFO, rb.getKString(rb.I_SEND_TO_ADMIN_CLIENT,
                   MessageType.getString(MessageType.MIGRATESTORE_BROKER_REPLY)+
                   "["+Status.getString(status)+"]"));
        replyMessageID = parent.sendReply(con, cmd_msg, reply);
        replyStatusStr = Status.getString(status);

        } finally {
        final SysMessageID mid = replyMessageID;
        final String statusStr = replyStatusStr;

        if (!noop) {
            try {
            if (con instanceof IMQBasicConnection)  {
                IMQBasicConnection ipCon = (IMQBasicConnection)con;
                ipCon.flushControl(1000);
            }
            try {
                Globals.getServiceManager().stopNewConnections(ServiceType.NORMAL);
            } catch (Exception e) {
                logger.logStack(logger.WARNING, rb.getKString(rb.W_STOP_SERVICE_FAIL,
                    ServiceType.getServiceTypeString(ServiceType.NORMAL), e.getMessage()), e);
            }
            try {
                Globals.getServiceManager().stopNewConnections(ServiceType.ADMIN);
            } catch (Exception e) {
                logger.logStack(logger.WARNING, rb.getKString(rb.W_STOP_SERVICE_FAIL,
                    ServiceType.getServiceTypeString(ServiceType.ADMIN), e.getMessage()), e);
            }
            BrokerStateHandler.shuttingDown = true;
            bsh.prepareShutdown(false, true);
            waitForHandlersToComplete(20);
            if (mid == null) {
                logger.log(Logger.INFO, BrokerResources.I_ADMIN_SHUTDOWN_REQUEST);
                bsh.initiateShutdown("admin", 0, false, 0, true);
                return true;
            } 

            final String waitstr = rb.getKString(rb.I_WAIT_ADMIN_RECEIVE_REPLY,
                                   MessageType.getString(MessageType.MIGRATESTORE_BROKER_REPLY)+
                                   "["+statusStr+"]");
            final long totalwait = Globals.getConfig().getIntProperty(MAX_WAIT_ADMIN_CLIENT_PROP,
                                                       DEFAULT_MAX_WAIT_ADMIN_CLIENT)*1000L;
            Executors.newSingleThreadExecutor().execute(new Runnable() {
                public void run() {
                    try {

                    PacketReference ref = Destination.get(mid);
                    long waited = 0L;
                    while (ref != null && 
                           !ref.isInvalid() && !ref.isDestroyed() &&
                           (ref.getDeliverCnt() > ref.getCompleteCnt()) &&
                           waited < totalwait) {
                        logger.log(logger.INFO, waitstr);
                        try {
                            Thread.sleep(500);
                            waited += 500L;
                        } catch (Exception e) {}
                        ref = Destination.get(mid);
                    }
                    logger.log(Logger.INFO, BrokerResources.I_ADMIN_SHUTDOWN_REQUEST);
                    bsh.initiateShutdown("admin-migratestore-shutdown", 0, false, 0, true);

                    } catch (Throwable t) {
                    bsh.initiateShutdown("admin-migratestore-shutdown::["+t.toString()+"]", 0, false, 0, true);
                    }
                }
            });

            } catch (Throwable t) {
            bsh.initiateShutdown("admin-migratestore-shutdown:["+t.toString()+"]", 0, false, 0, true);
            }
        }
        }

        } finally {
            BrokerStateHandler.unsetExclusiveRequestLock(ExclusiveRequest.MIGRATE_STORE);
        }
        return true;
    }

    /**
     */
    private String getBrokerID() throws Exception {
        String brokerID = null;
        ClusterManager cm = Globals.getClusterManager();
        ClusterBroadcast cbc = Globals.getClusterBroadcast();
        String replica = null;
        List<String> replicas = null;
        replicas = Globals.getStore().getMyReplicas();
        Iterator<String> itr = replicas.iterator();
        while (itr.hasNext()) {
            replica = itr.next();
            if (cbc.lookupBrokerAddress(replica) != null) {
                brokerID = replica;
            }
        }
        if (brokerID == null) {
            BrokerMQAddress mqaddr = (BrokerMQAddress)cm.getBrokerNextToMe();
            BrokerAddress addr = cbc.lookupBrokerAddress(mqaddr);
            if (addr != null && addr.getBrokerID() != null) {
                brokerID = addr.getBrokerID();
            }
        }
        if (brokerID == null) {
            BrokerMQAddress mqaddr = null;
            BrokerAddress addr = null;
            Iterator itr1 = cm.getActiveBrokers();
            while (itr1.hasNext()) {
                mqaddr = (BrokerMQAddress)((ClusteredBroker)itr1.next()).getBrokerURL();
                addr = cbc.lookupBrokerAddress(mqaddr);
                if (addr != null && addr.getBrokerID() != null) {
                    brokerID = addr.getBrokerID();
                }
            }
        }
        if (brokerID == null) {
            throw new OperationNotAllowedException(
                "No connected broker found in cluster",
                MessageType.getString(MessageType.MIGRATESTORE_BROKER));
        }
        return brokerID;
    }
}
