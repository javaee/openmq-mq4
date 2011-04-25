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
 * @(#)ClusterBroadcaster.java	1.13 07/23/07
 */ 

package com.sun.messaging.jmq.jmsserver.core.cluster;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.Map;
import java.util.Set;
import java.util.Hashtable;
import com.sun.messaging.jmq.util.UID;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;
import com.sun.messaging.jmq.jmsserver.core.*;
import com.sun.messaging.jmq.jmsserver.service.ConnectionUID;
import com.sun.messaging.jmq.jmsserver.multibroker.Protocol;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.io.SysMessageID;
import java.lang.reflect.*;


/**
 * Hides access to the clustering interface.
 */
public class ClusterBroadcaster implements ClusterBroadcast {

    ClusterBroadcast child = null;

    public ClusterBroadcaster(Integer maxBrokers, Integer clusterversion) 
        throws ClassNotFoundException, NoSuchMethodException, 
               InstantiationException, IllegalAccessException,
               InvocationTargetException, BrokerException
    {
        this(maxBrokers.intValue(), clusterversion.intValue());
    }

    public ClusterBroadcaster(int maxBrokers, int clusterversion) 
        throws ClassNotFoundException, NoSuchMethodException, 
               InstantiationException, IllegalAccessException,
               InvocationTargetException, BrokerException
    {
        try {

        Class c = Class.forName("com.sun.messaging.jmq.jmsserver"
                      + ".multibroker.ClusterBroadcaster");
        Class[] paramTypes = { Integer.class, Integer.class };
        Constructor cons = c.getConstructor(paramTypes);
        Object[] paramArgs = { new Integer(maxBrokers), 
                        new Integer(clusterversion) };
        child = (ClusterBroadcast)cons.newInstance(paramArgs);

        } catch (InvocationTargetException e) {
        Globals.getLogger().logStack(Logger.DEBUG, e.getMessage(), e); 
        Globals.getLogger().logStack(Logger.DEBUG, 
          e.getTargetException().getMessage(), e.getTargetException()); 
        throw e;
        }
    
    }

    public com.sun.messaging.jmq.jmsserver.multibroker.ClusterBroadcaster
        getRealClusterBroadcaster() {
        return (com.sun.messaging.jmq.jmsserver.multibroker.ClusterBroadcaster)child;
    }

    public Protocol getProtocol()
    {
        return child.getProtocol();
    }

    public boolean waitForConfigSync() {
        return child.waitForConfigSync();
    }

    public void setMatchProps(Properties match) {
        child.setMatchProps(match);
    }

    public int getClusterVersion() throws BrokerException {
        return child.getClusterVersion();
    }

    public void startClusterIO() {
        child.startClusterIO();
    }

    public void stopClusterIO(boolean requestTakeover, boolean force,
                              BrokerAddress excludedBroker) {
        child.stopClusterIO(requestTakeover, force, excludedBroker);
    }

    public void pauseMessageFlow() throws IOException
    {
        child.pauseMessageFlow();
    }

    public void resumeMessageFlow() throws IOException
    {
        child.resumeMessageFlow();
    }

    public void messageDelivered(SysMessageID id, ConsumerUID uid,
                BrokerAddress ba)
    {
        child.messageDelivered(id, uid, ba);
    }

    public void forwardMessage(PacketReference ref, Collection consumers)
    {
        child.forwardMessage(ref, consumers); // XXX - use broker consumers
    }

    /**
     * Returns the address of this broker.
     * @return <code> BrokerAddress </code> object representing this
     * broker.
     */
    public BrokerAddress getMyAddress()
    {
        return child.getMyAddress();
    }

    public boolean lockSharedResource(String resource, Object owner)
    {
         return child.lockSharedResource(resource, owner);
    }

    public boolean lockDestination(DestinationUID uid, Object owner)
    {
        return child.lockDestination(uid, owner);

    }

    public void unlockDestination(DestinationUID uid, Object owner)
    {
        child.unlockDestination(uid, owner);
    }

    public boolean lockClientID(String clientid, Object owner, boolean shared)
    {
        return child.lockClientID(clientid, owner, shared);
    }

    public void unlockClientID(String clientid, Object owner)
    {
        child.unlockClientID(clientid, owner);
    }

    public boolean getConsumerLock(ConsumerUID uid,
                    DestinationUID duid, int position,
                    int maxActive, Object owner)
            throws BrokerException
    {
        return child.getConsumerLock(uid, duid, position,
                   maxActive, owner);
    }

    public void unlockConsumer(ConsumerUID uid, DestinationUID duid, int position)
    {
        child.unlockConsumer(uid, duid, position);
    }
    
    public void acknowledgeMessage(BrokerAddress address,
                SysMessageID sysid, ConsumerUID cuid, int ackType, 
                Map optionalProps, boolean ackack) throws BrokerException {
        child.acknowledgeMessage(address, sysid, cuid, ackType, optionalProps, ackack);
    }

    public void acknowledgeMessage2P(BrokerAddress address,
                SysMessageID[] sysids, ConsumerUID[] cuids, int ackType,
                Map optionalProps, Long txnID, boolean ackack, boolean async) 
                throws BrokerException {

        child.acknowledgeMessage2P(address, sysids, cuids, ackType, 
                                   optionalProps, txnID, ackack, async);
    }

    public void recordUpdateDestination(Destination d)
        throws BrokerException
    {
        child.recordUpdateDestination(d);
    }

    public void recordRemoveDestination(Destination d)
        throws BrokerException
    {
         child.recordRemoveDestination(d);
    }

    public void createDestination(Destination dest)
            throws BrokerException
    {
         child.createDestination(dest);
    }

    public void recordCreateSubscription(Subscription sub)
        throws BrokerException
    {
        child.recordCreateSubscription(sub);
    }

    public void recordUnsubscribe(Subscription sub)
        throws BrokerException
    {
         child.recordUnsubscribe(sub);
    }

    public void createSubscription(Subscription sub, Consumer cons)
            throws BrokerException
    {
        child.createSubscription(sub, cons);
    }

    public void createConsumer(Consumer con)
            throws BrokerException
    {
        child.createConsumer(con);
    }

    public void updateDestination(Destination dest)
            throws BrokerException
    {
        child.updateDestination(dest);
    }

    public void updateSubscription(Subscription sub)
            throws BrokerException
    {
        child.updateSubscription(sub);
    }

    public void updateConsumer(Consumer con)
            throws BrokerException
    {
        child.updateConsumer(con);
    }


    public void destroyDestination(Destination dest)
            throws BrokerException
    {
        child.destroyDestination(dest);
    }

    public void destroyConsumer(Consumer con, Map pendingMsgs, boolean cleanup)
            throws BrokerException
    {
        child.destroyConsumer(con, pendingMsgs, cleanup);
    }

    public void connectionClosed(ConnectionUID uid, boolean admin)
    {
        child.connectionClosed(uid, admin);
    }

    public void reloadCluster()
    {
        child.reloadCluster();
    }

    public Hashtable getAllDebugState()
    {
        return child.getAllDebugState();
    }

    /**
     * Ensures that the given "prefix" number is unique in the
     * cluster. This method is used to ensure the uniqueness of the
     * UIDs generated by a broker.
     *
     * @return true if the number is unique. false if some other
     * broker is using this number as a UID prefix.
     */
    public boolean lockUIDPrefix(short p)
    {
        return child.lockUIDPrefix(p);
    }

    public void preTakeover(String brokerID, UID storeSession, 
                String brokerHost, UID brokerSession) throws BrokerException {
        child.preTakeover(brokerID, storeSession, brokerHost, brokerSession);
    }
    public void postTakeover(String brokerID, UID storeSession, boolean aborted) {
        child.postTakeover(brokerID, storeSession, aborted);
    }

    /**
     * Send information for a cluster transaction
     */
    public void sendClusterTransactionInfo(long tid, BrokerAddress to) {
        child.sendClusterTransactionInfo(tid, to);
    }

    /**
     * Lookup broker address for a brokerID - for HA mode and BDBREP mode only
     */
    public BrokerAddress lookupBrokerAddress(String brokerid) {
        return child.lookupBrokerAddress(brokerid);
    }

    /**
     * Lookup broker address
     */
    public BrokerAddress lookupBrokerAddress(BrokerMQAddress mqaddr) {
        return child.lookupBrokerAddress(mqaddr);
    }

    /**
     * Change master broker
     */
    public void changeMasterBroker(BrokerMQAddress newmaster, BrokerMQAddress oldmaster)
    throws BrokerException {
        child.changeMasterBroker(newmaster, oldmaster);
    }

    /**
     */
    public String sendTakeoverMEPrepare(String brokerID, byte[] token,
                                        Long syncTimeout, String uuid)
                                        throws BrokerException {
        return child.sendTakeoverMEPrepare(brokerID, token, syncTimeout, uuid);
    }

    /**
     */
    public String sendTakeoverME(String brokerID, String uuid)
    throws BrokerException {
        return child.sendTakeoverME(brokerID, uuid);
    }
}

