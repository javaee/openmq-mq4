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
 * %W% %G%
 */ 

package com.sun.messaging.jmq.jmsserver.core;

import com.sun.messaging.jmq.util.lists.*;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Hashtable;
import java.util.Vector;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.lang.ref.*;
import java.io.*;
import com.sun.messaging.jmq.io.*;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.util.lists.*;
import com.sun.messaging.jmq.jmsserver.service.ConnectionUID;
import com.sun.messaging.jmq.jmsserver.service.Connection;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.license.LicenseBase;
import com.sun.messaging.jmq.util.log.*;
import com.sun.messaging.jmq.util.JMQXid;



public class Session implements EventBroadcaster, EventListener
{
    private static boolean DEBUG = false;
    private static boolean DEBUG_CLUSTER_MSG =
                   Globals.getConfig().getBooleanProperty(
                        Globals.IMQ + ".cluster.debug.msg") || DEBUG;

    /**
     * types of consumers
     */
    public static final int AUTO_ACKNOWLEDGE = 1;
    public static final int CLIENT_ACKNOWLEDGE = 2;
    public static final int DUPS_OK_ACKNOWLEDGE = 3;
    public static final int NO_ACK_ACKNOWLEDGE= 32768;
    // NONE may be transacted or an error
    public static final int NONE = 0;

    protected Logger logger = Globals.getLogger();

    private int ackType = 0; // XXX -should really use this not consumer

    private boolean isTransacted = false;
    private boolean isXATransacted = false;
    private TransactionUID currentTransactionID = null;

    SessionUID uid;
    Map deliveredMessages;

    Map cidToStoredCid = null;

    // single session lock
    Object sessionLock = new Object();

    EventBroadcastHelper evb = new EventBroadcastHelper();

    private Map cleanupList = new HashMap();
    private Map storeMap = new HashMap();

    Map consumers = null;
    Map listeners = null;
    Set busyConsumers = null;

    boolean paused = false;
    int pausecnt = 0;
    boolean valid = false;
    private boolean busy = false;

    ConnectionUID parentCuid = null;


    transient String creator = null;


    private static boolean NOACK_ENABLED = false;
    static {
        try {
            LicenseBase license = Globals.getCurrentLicense(null);
            NOACK_ENABLED = license.getBooleanProperty(
                                license.PROP_ENABLE_NO_ACK, false);
        } catch (BrokerException ex) {
            NOACK_ENABLED = false;
        }

    }

    public static boolean isValidAckType(int type) {
        switch (type) {
            case Session.NONE: // transacted
            case Session.AUTO_ACKNOWLEDGE:
            case Session.CLIENT_ACKNOWLEDGE:
            case Session.DUPS_OK_ACKNOWLEDGE:
            case Session.NO_ACK_ACKNOWLEDGE:
                return true;
            default:
                return false;
        }
    }


    public ConnectionUID getConnectionUID() {
        return parentCuid;
    }


    public static Hashtable getAllDebugState() {
        Hashtable ht = new Hashtable();
        ht.put("TABLE", "All Sessions");
        Hashtable all = new Hashtable();
        synchronized(allSessions) {
            ht.put("allSessionCnt", String.valueOf(allSessions.size()));
            Iterator itr = allSessions.values().iterator();
            while (itr.hasNext()) {
                Session s = (Session)itr.next();
                all.put(String.valueOf(s.getSessionUID().longValue()),
                       s.getDebugState());
            }
        }
        ht.put("allSessions", all);
        all = new Hashtable();
        synchronized(ConsumerToSession) {
            ht.put("ConsumerToSession", String.valueOf(ConsumerToSession.size()));
            Iterator itr = ConsumerToSession.keySet().iterator();
            while (itr.hasNext()) {
                Object o = itr.next();
                all.put(o.toString(),
                       ConsumerToSession.get(o).toString());
            }
        }
        ht.put("ConsumerToSession", all);
        return ht;
    }

    public Hashtable getDebugState() {
        Hashtable ht = new Hashtable();
        ht.put("TABLE", "Session[" + uid.longValue() + "]");
        ht.put("uid", String.valueOf(uid.longValue()));
        ht.put("connection", String.valueOf(parentCuid.longValue()));
        ht.put("paused", String.valueOf(paused));
        ht.put("pausecnt", String.valueOf(pausecnt));
        ht.put("valid", String.valueOf(valid));
        ht.put("busy", String.valueOf(busy));
        ht.put("PendingAcks(deliveredMessages)", String.valueOf(deliveredMessages.size()));

        // ok deal w/ unacked per consumer - its easier at this level

        if (deliveredMessages.size() > 0) {
            HashMap copyDelivered = null;
            ArrayList copyCuids = null;
            int cuidCnt[] = null;
            synchronized (deliveredMessages) {
                copyDelivered = new HashMap(deliveredMessages);
            }
            synchronized (consumers) {
                copyCuids = new ArrayList(consumers.keySet());
            }
            cuidCnt = new int[copyCuids.size()];
    
            Iterator itr = copyDelivered.values().iterator();
            while (itr.hasNext()) {
                ackEntry e = (ackEntry)itr.next();
                int indx = copyCuids.indexOf(e.getConsumerUID());
                if (indx == -1)
                    continue;
                 else
                     cuidCnt[indx] ++;
            }
            Hashtable m = new Hashtable();
            for (int i=0; i < copyCuids.size(); i ++ ) {
                if (cuidCnt[i] == 0) continue;
                ConsumerUID cuid = (ConsumerUID) copyCuids.get(i);
                m.put(String.valueOf(cuid.longValue()), String.valueOf(cuidCnt[i]));
            }
            if (!m.isEmpty())
                ht.put("PendingAcksByConsumer", m);
        }


        ht.put("consumerCnt", String.valueOf(consumers.size()));
        Vector v = new Vector();
        synchronized (consumers) {
            Iterator itr = consumers.keySet().iterator();
            while (itr.hasNext()) {
                ConsumerUID cuid = (ConsumerUID)itr.next();
                v.add(String.valueOf(cuid.longValue()));
            }
        }
        ht.put("consumers", v);
        ht.put("busyConsumerCnt", String.valueOf(busyConsumers.size()));
        v = new Vector();
        synchronized (busyConsumers) {
            Iterator itr = busyConsumers.iterator();
            while (itr.hasNext()) {
                ConsumerUID cuid = (ConsumerUID)itr.next();
                v.add(String.valueOf(cuid.longValue()));
            }
        }
        ht.put("busyConsumers", v);
        return ht;
    }




    public Vector getDebugMessages(boolean full) {
        Vector v = new Vector();
        synchronized (deliveredMessages) {
            Iterator itr = deliveredMessages.values().iterator();
            while (itr.hasNext()) {
                ackEntry e = (ackEntry)itr.next();
                v.add(e.getDebugMessage(full));
            }
        }
        return v;
    }


    // used for JMX
    public int getNumPendingAcks(ConsumerUID uid)
    {
         return getPendingAcks(uid).size();
    }
    

    public List getPendingAcks(ConsumerUID uid)
    {
        List acks = new ArrayList();
        Map copyDelivered = new HashMap();
        synchronized (deliveredMessages) {
            if (deliveredMessages.size() == 0)
                   return acks;
            copyDelivered.putAll(deliveredMessages);
        }

        Iterator itr = copyDelivered.values().iterator();
        while (itr.hasNext()) {
            ackEntry e = (ackEntry)itr.next();
            if (e.getConsumerUID().equals(uid)) {
                acks.add(e.getSysMessageID());
            }
        }
        return acks;
    }



    public void setAckType(int type) 
        throws BrokerException
    {
        if (!Session.isValidAckType(type))

            throw new BrokerException(
                        "Internal Error: Invalid Ack Type :" + type,
                        Status.BAD_REQUEST);

        if (type == Session.NO_ACK_ACKNOWLEDGE && !NOACK_ENABLED) {
            throw new BrokerException(
                        Globals.getBrokerResources().getKString(
                        BrokerResources.E_FEATURE_UNAVAILABLE,
                            Globals.getBrokerResources().getKString(
                                BrokerResources.M_NO_ACK_FEATURE)),
                        BrokerResources.E_FEATURE_UNAVAILABLE,
                        (Throwable) null,
                        Status.NOT_ALLOWED);
        }
        ackType = type;
    }

    public int getConsumerCnt() {
        if (consumers == null) return 0;
        return consumers.size();
    }

    public Iterator getConsumers() {
        if (consumers == null) 
            return (new ArrayList()).iterator();
        return (new ArrayList(consumers.values())).iterator();
    }

    public boolean isAutoAck(ConsumerUID uid) {
        if (isUnknown()) {
            return uid.isAutoAck();
        }
        return ackType == Session.AUTO_ACKNOWLEDGE;
    }

    public boolean isUnknown() {
        return ackType == Session.NONE;
    }

    public boolean isClientAck(ConsumerUID uid) {
        if (isUnknown()) {
            return !uid.isAutoAck() && !uid.isDupsOK();
        }
        return ackType == Session.CLIENT_ACKNOWLEDGE;
    }

    public boolean isDupsOK(ConsumerUID uid) {
        if (isUnknown()) {
            return uid.isDupsOK();
        }
        return ackType == Session.DUPS_OK_ACKNOWLEDGE;
    }

    public boolean isUnsafeAck(ConsumerUID uid) {
        return isDupsOK(uid) || isNoAck(uid);
    }

    public boolean isNoAck(ConsumerUID uid) {
        if (isUnknown()) {
            return uid.isNoAck();
        }
        return ackType == Session.NO_ACK_ACKNOWLEDGE;
    }

    public boolean isTransacted() {
        return isTransacted;
    }

    public TransactionUID getCurrentTransactionID() {
        return currentTransactionID;
    }

    public ConsumerUID getStoredIDForDetatchedConsumer(ConsumerUID cuid) {
        return (ConsumerUID)storeMap.get(cuid);
    }

    public void debug(String prefix) {
        if (prefix == null)
            prefix = "";
        logger.log(Logger.INFO,prefix + "Session " + uid);
        logger.log(Logger.INFO,"Paused " + paused);
        logger.log(Logger.INFO,"pausecnt " + pausecnt);
        logger.log(Logger.INFO,"busy " + busy);
        logger.log(Logger.INFO,"ConsumerCnt " + consumers.size());
        logger.log(Logger.INFO,"BusyConsumerCnt " + consumers.size());
        Iterator itr = consumers.values().iterator();
        while (itr.hasNext()) {
            Consumer c = (Consumer)itr.next();
            c.debug("\t");
        }

    }

    private Session(ConnectionUID uid, String sysid) {
        this(new SessionUID(), uid, sysid);
    }

    private Session(SessionUID uid, ConnectionUID cuid, String sysid ) {
        this.uid = uid;
        parentCuid = cuid;
        deliveredMessages = Collections.synchronizedMap(new LinkedHashMap());
        cidToStoredCid = Collections.synchronizedMap(new HashMap());
        consumers = Collections.synchronizedMap(new HashMap());
        listeners = Collections.synchronizedMap(new HashMap());
        busyConsumers = Collections.synchronizedSet(new LinkedHashSet());
        valid = true;
        creator = sysid;
        DEBUG = (DEBUG || logger.getLevel() <= Logger.DEBUG || DEBUG_CLUSTER_MSG);
        logger.log(Logger.DEBUG,"Created new session " + uid
              + " on connection " + cuid);
    }

    public void dump(String prefix) {
        if (prefix == null)
            prefix = "";

        logger.log(Logger.INFO,prefix + " Session " + uid);
        logger.log(Logger.INFO, prefix + "---------------------------");
        logger.log(Logger.INFO, prefix + "busyConsumers (size) " + busyConsumers.size());
        logger.log(Logger.INFO, prefix + "busyConsumers (list) " + busyConsumers);
        logger.log(Logger.INFO, prefix + "consumers (size) " + consumers.size());
        logger.log(Logger.INFO, prefix + "consumers (list) " + consumers);
        logger.log(Logger.INFO, prefix + "---------------------------");
        Iterator itr = consumers.values().iterator();
        while (itr.hasNext()) {
            ((Consumer)itr.next()).dump(prefix + "\t");
        }
    }

    class ackEntry
    {
        ConsumerUID uid = null;
        ConsumerUID storedcid = null;

        Object pref = null;
        SysMessageID id = null;
        TransactionUID tuid = null;
        int hc = 0;

        public ackEntry(SysMessageID id,
              ConsumerUID uid) 
        { 
             assert id != null;
             assert uid != null;
             this.id = id;
             this.uid = uid;
             pref = null;
        }

        public String toString() {
            return id+"["+uid+","+storedcid+"]"+ (tuid == null? "":"TUID="+tuid);
        }

        public String getDebugMessage(boolean full)
        {
            PacketReference ref = getReference();
            Packet p = (ref == null ? null : ref.getPacket());

            String str = "[" + uid + "," + storedcid + "," +
                    (p == null ? "null" : p.toString()) + "]";
            if (full && p != null) {
                 str += "\n" + p.dumpPacketString(">>");
            }
            return str;
        }

        public void setTUID(TransactionUID uid) {
            this.tuid = uid;
        }
        public TransactionUID getTUID() {
            return tuid;
        }

        public ConsumerUID getConsumerUID() {
            return uid;
        }
        public ConsumerUID getStoredUID() {
            return storedcid;
        }
        public SysMessageID getSysMessageID() {
            return id;
        }
        public PacketReference getReference() {
            if (pref instanceof WeakReference) {
                return (PacketReference)((WeakReference)pref).get();
            } else {
                return (PacketReference)pref;
            }
        }


        public ackEntry(PacketReference ref, 
               ConsumerUID uid, ConsumerUID storedUID) 
        {
            if (ref.isLocal()) {
                pref = new WeakReference(ref);
            } else {
                pref = ref;
            }
            id = ref.getSysMessageID();
            storedcid = storedUID;
            this.uid = uid;
        }

        public PacketReference acknowledged(boolean notify) throws BrokerException {
            return acknowledged(notify, null, null, true);
        }

        public PacketReference acknowledged(boolean notify, boolean ackack) throws BrokerException {
            return acknowledged(notify, null, null, ackack);
        }

        public PacketReference acknowledged(boolean notify, TransactionUID tid,
                                            HashMap remoteNotified, boolean ackack) 
            throws BrokerException
        {
            assert pref != null;
            boolean rm = false;

            PacketReference ref = getReference();

            try {
                if (ref != null && ref.isOverrided()) {
                    BrokerException bex = new BrokerException(
                               "Message requeued:"+ref, Status.GONE);
                    bex.setRemoteConsumerUIDs(String.valueOf(getConsumerUID().longValue()));
                    bex.setRemote(true); 
                    throw bex;
                }
                if (ref == null) {
                    // XXX weird
                    ref = Destination.get(id);
                }
                if (ref == null) {
                    String emsg = null;
                    if (tid == null) {
                        emsg = Globals.getBrokerResources().getKString(
                        BrokerResources.W_ACK_MESSAGE_GONE, this.toString());
                    } else {
                        emsg = Globals.getBrokerResources().getKString(
                        BrokerResources.W_ACK_MESSAGE_GONE_IN_TXN, tid.toString(),
                                             this.toString());
                    }
                    logger.log(Logger.WARNING, emsg);
                    throw new BrokerException(emsg, Status.CONFLICT);
                }
                rm = ref.acknowledged(uid, storedcid, !isUnsafeAck(uid), 
                                      notify, tid, remoteNotified, ackack);
                Consumer c = (Consumer)consumers.get(uid);
                if (c != null) {
                    c.setLastAckTime(System.currentTimeMillis());
                }
            } catch (Exception ex) {
                assert false : ref;
                String emsg = Globals.getBrokerResources().getKString(
                    BrokerResources.X_UNABLE_PROCESS_MESSAGE_ACK, 
                    this.toString()+"["+ref.getDestinationUID()+"]", ex.getMessage());
                if (logger.getLevel() <= Logger.DEBUG) {
                    logger.logStack(Logger.DEBUG, emsg, ex);
                } else {
                    logger.log(Logger.WARNING, emsg);
                }
                if (ex instanceof BrokerException) throw (BrokerException)ex;
                throw new BrokerException(emsg, ex);
            }
            return (rm ? ref : null);
        }

        public boolean equals(Object o) {
            if (! (o instanceof ackEntry)) {
                return false;
            }
            ackEntry ak = (ackEntry)o;
            return uid.equals(ak.uid) &&
                   id.equals(ak.id);
        }
        public int hashCode() {
            // uid is 4 bytes
            if (hc == 0) {
                hc = id.hashCode()*15 + uid.hashCode();
            }
            return hc;
        }
    }

    public SessionUID getSessionUID() {
        return uid;
    }


    public void pause(String reason) {
        synchronized(sessionLock) {
            paused = true;
            pausecnt ++;
            if (DEBUG)
                logger.log(Logger.INFO,"Session: Pausing " + this 
                    + "[" + pausecnt + "]" + reason);
        }
        checkState(null);
    }

    public void resume(String reason) {
        synchronized(sessionLock) {
            pausecnt --;
            if (pausecnt <= 0)
                paused = false;
            assert pausecnt >= 0: "Bad pause " + this;
            if (DEBUG)
                logger.log(Logger.INFO,"Session: Resuming " + this 
                     + "[" + pausecnt + "]" + reason);
        }
        checkState(null);
    }

    public boolean isPaused() {
        return paused;
    }

    public boolean hasWork() {
        return (busyConsumers.size() > 0);
    }


    public boolean fillNextPacket (Packet p, ConsumerUID cid)
    {
        if (paused) {
            return false;
        }
        Consumer consumer = (Consumer)consumers.get(cid);
        PacketReference ref = null;
        synchronized (sessionLock) {
            ref = consumer.getAndFillNextPacket(p);
            if (ref == null) return false;

            ConsumerUID sid = consumer.getStoredConsumerUID();

  
            ackEntry entry = null; 
            if (!consumer.getConsumerUID().isNoAck()) {
                entry = new ackEntry(ref, cid, sid);
                synchronized(deliveredMessages) {
                    deliveredMessages.put(entry, entry);
                }
            }

            try  {
                ConsumerUID c = consumer.getConsumerUID();
                boolean store = !isAutoAck(c) || 
                       deliveredMessages.size() == 1;
            
                if (ref.delivered(c, sid, !isUnsafeAck(c), store)) {
                    // either hit limit or need to ack message
                    Destination d = ref.getDestination();
                    //XXX - use destination limit
                    if (ref.isDead()) { // undeliverable
                        Packet pk = ref.getPacket();
                       if (pk != null && !pk.getConsumerFlow()) {
                           ref.removeInDelivery(sid);
                           d.removeDeadMessage(ref);
                           synchronized(deliveredMessages) {
                               deliveredMessages.remove(entry);
                           }
                           ref = null;
                       }
                    } else { // no ack remove
                       ref.removeInDelivery(sid);
                       d.removeMessage(ref.getSysMessageID(),
                           RemoveReason.ACKNOWLEDGED, !ref.isExpired());
                       synchronized(deliveredMessages) {
                           deliveredMessages.remove(entry);
                       }
                    }
                }

            } catch (Exception ex) {
           logger.logStack(Logger.WARNING, ex.getMessage(), ex);
                synchronized(deliveredMessages) {
                    if (entry != null)
                        deliveredMessages.get(entry);
                }
            }
        }
        return ref != null;
    }

    public ConsumerUID fillNextPacket(Packet p) 
       //throws BrokerException, IOException
    {
        if (paused) {
            return null;
        }
        
        ConsumerUID cid = null;
        Consumer consumer = null;
        while (!paused) {          
            // get a consumer
            synchronized (busyConsumers) {
               if (busyConsumers.isEmpty()) {
                   break;
               }
               Iterator itr = busyConsumers.iterator();
               cid = (ConsumerUID)itr.next();
               consumer = (Consumer)consumers.get(cid);
               itr.remove();
            }

            assert p != null;

            if (consumer == null) return null;

            PacketReference ref = null;
            synchronized (sessionLock) {
                if (paused)  {
                    synchronized (busyConsumers) {
                        if (consumer.isBusy())
                            busyConsumers.add(cid);
                    }
                    return null;
                }

                ref = consumer.getAndFillNextPacket(p);
                synchronized (busyConsumers) {
                    if (consumer.isBusy())
                        busyConsumers.add(cid);
                }

                if (ref == null) {
                    continue;
                }

                // ok the consumer wrote what it could
                // now store the session info

                ConsumerUID sid = consumer.getStoredConsumerUID();

  
                ackEntry entry = null; 
                if (!consumer.getConsumerUID().isNoAck()) {
                    entry = new ackEntry(ref, cid, 
                       sid);
                    synchronized(deliveredMessages) {
                        deliveredMessages.put(entry, entry);
                    }
                }

                try  {
                    ConsumerUID c = consumer.getConsumerUID();
                    boolean store = !isAutoAck(c) || 
                           deliveredMessages.size() == 1;
              
                    if (ref.delivered(c, sid, !isUnsafeAck(c), store)) {
                        // either hit limit or need to ack message
                        Destination d = ref.getDestination();
                        //XXX - use destination limit
                        if (ref.isDead()) { // undeliverable
                            Packet pk = ref.getPacket();
                            if (pk != null && !pk.getConsumerFlow()) {
                                ref.removeInDelivery(sid);
                                d.removeDeadMessage(ref);
                                ref = null;
                                continue;
                            }
                        } else { // no ack remove
                           ref.removeInDelivery(sid);
                           d.removeMessage(ref.getSysMessageID(),
                               RemoveReason.ACKNOWLEDGED, !ref.isExpired());
                        }
                    }

                } catch (Exception ex) {
                    logger.logStack(Logger.WARNING, ex.getMessage(), ex);
                    synchronized(deliveredMessages) {
                        if (entry != null)
                            deliveredMessages.get(entry);
                    }
                    continue;
                }
            }

            checkState(null);
            return (ref != null && cid != null ? cid : null);

        }
        checkState(null);
        return null;
             
    }

    public Object getBusyLock() {
        return busyConsumers;
    }

    public boolean isBusy() {
        synchronized (busyConsumers) {
            return busy;
        }
    }

    public String toString() {
        return "Session [" + uid + "]";

    }

    private Set detachedRConsumerUIDs = Collections.synchronizedSet(new LinkedHashSet());

    public synchronized void attachConsumer(Consumer c) throws BrokerException {
        logger.log(Logger.DEBUG,"Attaching Consumer " + c.getConsumerUID()
           + " to Session " + uid);

        if (!valid) {
            throw new BrokerException(Globals.getBrokerResources().
                getKString(BrokerResources.X_SESSION_CLOSED, this.toString()));
        }
        c.attachToSession(getSessionUID());
        ConsumerUID cuid = c.getConsumerUID();
        cuid.setAckType(ackType);
        c.getStoredConsumerUID().setAckType(ackType);
        consumers.put(cuid, c);

        Destination d = c.getFirstDestination();
        listeners.put(cuid, c.addEventListener(this, 
             EventType.BUSY_STATE_CHANGED, null));
        if (c.isBusy()) {
            busyConsumers.add(cuid);
        }
        synchronized(ConsumerToSession) {
            ConsumerToSession.put(c.getConsumerUID(),
                  getSessionUID());
        }

        checkState(null);
    }

    /**
     * clean indicated that it was made by a 3.5 consumer calling
     * close
     * @param id last SysMessageID seen (null indicates all have been seen)
     * @param redeliverAll  ignore id and redeliver all
     * @param redeliverPendingConsume - redeliver pending messages
     */
    public Consumer detatchConsumer(ConsumerUID c, SysMessageID id, 
               boolean redeliverPendingConsume, boolean redeliverAll)
        throws BrokerException
    {
        pause("Consumer.java: detatch consumer " + c);
        Consumer con = (Consumer)consumers.remove(c);
        if (con == null) {
            assert con != null;
            resume("Consumer.java: bad removal " + c);
            throw new BrokerException("Detatching consumer " + c 
                 + " not currently attached "
                  +  "to " + this );
        }
        con.pause("Consumer.java: detatch consumer " + c
             + " DEAD"); // we dont want to ever remove messages
        detatchConsumer(con, id, redeliverPendingConsume, redeliverAll);
        resume("Consumer.java: detatch consumer " + c);
        return con;
    }

    /**
     * @param id last SysMessageID seen (null indicates all have been seen)
     * @param redeliverAll  ignore id and redeliver all
     * @param redeliverPendingConsume - redeliver pending messages
     */
    private void detatchConsumer(Consumer con, SysMessageID id,
           boolean redeliverPendingConsume, boolean redeliverAll)
    {
        if (DEBUG) {
        logger.log(Logger.INFO,"Detaching Consumer "+con.getConsumerUID()+
           " on connection "+ con.getConnectionUID()+ 
           " from Session " + uid + " last id was " + id);
        }
        con.pause("Consumer.java: Detatch consumer 1 " + con   );
        pause("Consumer.java: Detatch consumer A " + con);
        ConsumerUID c = con.getConsumerUID();
        ConsumerUID sid = con.getStoredConsumerUID();
        Object listener= listeners.remove(c);
        assert listener != null;
        con.removeEventListener(listener);
        con.attachToSession(null);
        busyConsumers.remove(c);
        consumers.remove(c);
        checkState(null);

        // OK, we have 2 sets of messages:
        //    messages which were seen (and need the state
        //         set to consumed)
        //    messages which were NOT seen (and need the
        //         state reset)
        // get delivered messages
        Set s = new LinkedHashSet();
        HashMap remotePendings = new HashMap();

        boolean holdmsgs = false;

        // get all the messages for the consumer
        synchronized (deliveredMessages) {
            ackEntry startEntry = null;

            // workaround for client sending ack if acknowledged
            if (id != null) {
                ackEntry entry = new ackEntry(id, c);
                startEntry = (ackEntry)deliveredMessages.get(entry);
            }
          
            // make a copy of all of the data
            cleanupList.put(c, con.getParentList());
            storeMap.put(c, con.getStoredConsumerUID());

            // OK first loop through all of the consumed
            // messages and mark them consumed
            Iterator itr = deliveredMessages.values().iterator();
            boolean found = (startEntry == null && id != null);
            while (!redeliverAll && !found && itr.hasNext()) {
                ackEntry val = (ackEntry)itr.next();
                if (val == startEntry) { 
                     // we are done with consumed messages
                     found = true;
                }
                // see if we are for a different consumer
                //forward port 6829773
                if (!val.storedcid.equals(sid) || !val.uid.equals(c))
                    continue;
                PacketReference pr = val.getReference();
                // we know the consumer saw it .. mark it consumed
                if (pr != null) {
                    try {
                        pr.consumed(sid, !isUnsafeAck(c), isAutoAck(c));
                    } catch (Exception ex) {
                        Object[] args = { "["+pr+","+sid+"]", c, ex.getMessage() }; 
                        logger.log(Logger.WARNING, Globals.getBrokerResources().getKString(
                        BrokerResources.W_UNABLE_UPDATE_REF_STATE_ON_CLOSE_CONSUMER, args), ex);
                    } 
                }
                if (redeliverPendingConsume) {
                    if (pr != null) {
                        pr.removeInDelivery(sid);
                        s.add(pr);
                    }
                    itr.remove();
                    continue;
                } 
                if (this.isTransacted) { 
                    if (!Globals.getTransactionList().isConsumedInTransaction(
                                              val.getSysMessageID(), val.uid)) {
                        if (pr != null) {
                            pr.removeInDelivery(sid);
                            s.add(pr);
                        }
                        itr.remove();
                        continue;
                    }
                }
                if (pr != null && !pr.isLocal() && this.valid) {
                    BrokerAddress ba = pr.getAddress();   
                    if (ba != null) {
                        List l = (List)remotePendings.get(ba);
                        if (l == null) {
                            l = new ArrayList();
                            remotePendings.put(ba, l);
                        }
                        l.add(pr.getSysMessageID());
                        detachedRConsumerUIDs.add(val.uid);	
                    }
                }
                holdmsgs = true;
            }
            // now deal with re-queueing messages
            while (itr.hasNext()) {
                ackEntry val = (ackEntry)itr.next();
                // see if we are for a different consumer
                if (!val.storedcid.equals(sid)  || !val.uid.equals(c))
                    continue;
                PacketReference pr = val.getReference();
                if (this.isTransacted) { 
                    if (Globals.getTransactionList().isConsumedInTransaction(
                                              val.getSysMessageID(), val.uid)) {
				        if (pr != null && !pr.isLocal() && this.valid) {
                            BrokerAddress ba = pr.getAddress();   
                            if (ba != null) {
                                List l = (List)remotePendings.get(ba);
                                if (l == null) {
                                    l = new ArrayList();
                                    remotePendings.put(ba, l);
                                }
                                l.add(pr.getSysMessageID());
				                detachedRConsumerUIDs.add(val.uid);	
                            }
                        }
                        holdmsgs = true;
                        continue;
                    }
                }
                if ( pr != null) {
                    pr.removeInDelivery(sid);
                    s.add(pr);
                }
                itr.remove();
                try {
                    if (pr != null) {
                        pr.removeDelivered(sid, true);
                     }
                } catch (Exception ex) {
                    logger.log(Logger.INFO,"Internal Error " +
                        "Unable to consume " + sid + ":" + pr, ex);
                } 
            }
        }
        con.destroyConsumer(s, remotePendings,
                            (con.tobeRecreated() || (!valid && !isXATransacted)),
                            false, true);

        if (!holdmsgs && this.valid) {
            synchronized (deliveredMessages) {
                cleanupList.remove(c);
                storeMap.remove(c);
            }
            synchronized(ConsumerToSession) {
                ConsumerToSession.remove(c);
            }
        }

        resume("Consumer.java: resuming after detatch " + con);

    }

    public BrokerAddress acknowledgeInTransaction(ConsumerUID cuid, SysMessageID id, 
                                                  TransactionUID tuid, boolean isXA)
        throws BrokerException
    {
        //workaround client REDELIVER protocol for XA transaction
        if (!isTransacted) isTransacted = true;
        if (isXA && !isXATransacted) isXATransacted = true;
        currentTransactionID = tuid;

        // remove from the session pending list
        ackEntry entry = new ackEntry(id, cuid);
        synchronized(deliveredMessages) {
            entry = (ackEntry)deliveredMessages.get(entry);
        }
        if (entry == null) {
                String info = "Received unknown message for transaction " + tuid + " on session " + uid + " ack info is " + cuid + "," + id;
                // for debugging see if message exists
                PacketReference m = Destination.get(id);
                if (m == null) {
                    info +=": Broker does not know about the message";
                } else {
                    info += ":Broker knows about the message, not associated with the session";
                }
                // send acknowledge
                logger.log(Logger.WARNING, info);
                BrokerException bex = new BrokerException(info, Status.GONE);
                bex.setRemoteConsumerUIDs(String.valueOf(cuid.longValue()));
                bex.setRemote(true);
                throw bex;
        }
        if (entry.getTUID() != null && !entry.getTUID().equals(tuid)) {
                BrokerException bex = new BrokerException(
                "Message requeued:"+entry.getReference(), Status.GONE);
                bex.setRemoteConsumerUIDs(String.valueOf(entry.getConsumerUID().longValue()));
                bex.setRemote(true);
                throw bex;
        }
        PacketReference ref = entry.getReference();
        if (ref == null) {
            throw new BrokerException(Globals.getBrokerResources().getKString(
            BrokerResources.I_ACK_FAILED_MESSAGE_REF_CLEARED,
            ""+id+"["+cuid+":"+entry.getStoredUID()+"]TUID="+tuid), Status.CONFLICT);
        }
        if (ref.isOverrided()) {
                BrokerException bex = new BrokerException(
                "Message requeued:"+entry.getReference(), Status.GONE);
                bex.setRemoteConsumerUIDs(String.valueOf(entry.getConsumerUID().longValue()));
                bex.setRemote(true);
                throw bex;
        }
        entry.setTUID(tuid);
        return ref.getAddress();
        // return;
    }


    private void close() {
        synchronized(this) {
            if (!valid) return;
            valid = false;
        }
        logger.log(Logger.DEBUG,"Close Session " + uid);
        
        Connection conn = Globals.getConnectionManager().getConnection(getConnectionUID());
        boolean old = false;
        if (conn != null && conn.getClientProtocolVersion() < Connection.RAPTOR_PROTOCOL) {
            old =true;
        }

        Iterator itr = null;
        synchronized (this) {
            itr = new HashSet(consumers.values()).iterator();
        }
        while (itr.hasNext()) {
            Consumer c =(Consumer)itr.next();
            itr.remove();
            detatchConsumer(c, null, old, false);
        }

        // deal w/ old messages
        synchronized(deliveredMessages) {
            if (!deliveredMessages.isEmpty()) {
                // get the list by IDs
                HashMap openMsgs = new HashMap();
                itr = deliveredMessages.entrySet().iterator();
                while (itr.hasNext()) {
                    Map.Entry entry = (Map.Entry)itr.next();
                    ackEntry e = (ackEntry)entry.getValue();

                    ConsumerUID cuid = e.getConsumerUID();
                    ConsumerUID storeduid = (e.getStoredUID() == null ? cuid:e.getStoredUID()); 

                    // deal w/ orphan messages
                    TransactionUID tid = e.getTUID();
                    if (tid != null) {
                        TransactionState ts = Globals.getTransactionList().retrieveState(tid, true);
                        JMQXid jmqxid = Globals.getTransactionList().UIDToXid(tid);
                        if (jmqxid != null) {
                            Globals.getTransactionList().addOrphanAck(
                                tid, e.getSysMessageID(), storeduid, cuid);
                            itr.remove();
                            continue;
                        }
                        if (ts != null && ts.getState() == TransactionState.PREPARED) {
                            Globals.getTransactionList().addOrphanAck(
                                    tid, e.getSysMessageID(), storeduid, cuid);
                            itr.remove();
                            continue;
                        }
                        if (ts != null && ts.getState() == TransactionState.COMMITTED) {
                            itr.remove();
                            continue;
                        }
                        if (ts != null && conn != null &&
                            ts.getState() == TransactionState.COMPLETE &&
                            conn.getConnectionState() >= Connection.STATE_CLOSED) {
                            String[] args = { ""+tid,
                                              TransactionState.toString(ts.getState()),
                                              getConnectionUID().toString() };
                            logger.log(Logger.INFO, Globals.getBrokerResources().getKString(
                                       BrokerResources.I_CONN_CLEANUP_KEEP_TXN, args));
                            Globals.getTransactionList().addOrphanAck(
                                    tid, e.getSysMessageID(), storeduid, cuid);
                            itr.remove();
                            continue;
                        }
                    }
                    PacketReference ref = e.getReference();
                    if (ref == null) ref = Destination.get(e.getSysMessageID());
                    if (ref != null && !ref.isLocal()) {
                        itr.remove();
                        try {
                            if ((ref = e.acknowledged(false)) != null) {
                                Destination d = ref.getDestination();
                                d.removeRemoteMessage(ref.getSysMessageID(),
                                             RemoveReason.ACKNOWLEDGED, ref);
                            }
                        } catch(Exception ex) {
                            logger.logStack(DEBUG_CLUSTER_MSG ? 
          Logger.WARNING:Logger.DEBUG, "Unable to clean up remote message "
                            + e.getDebugMessage(false), ex);
                        }
                        continue;
                    }

                    // we arent in a transaction ID .. cool 
                    // add to redeliver list
                    Set s = (Set)openMsgs.get(cuid);
                    if (s == null) {
                        s = new LinkedHashSet();
                        openMsgs.put(cuid, s);
                    }
                    if (ref != null) {
                        ref.removeInDelivery(storeduid);
                    }
                    s.add(e);
                }

    
                // OK .. see if we ack or cleanup
                itr = openMsgs.keySet().iterator();
                while (itr.hasNext()) {
                    ConsumerUID cuid = (ConsumerUID)itr.next();
                    Prioritized parent = (Prioritized)cleanupList.get(cuid);
                    ConsumerUID sid = (ConsumerUID)storeMap.get(cuid);
                    if (parent == null) {
                        Set s = (Set)openMsgs.get(cuid);
                        Iterator sitr = s.iterator();
                        while (sitr.hasNext()) {
                            ackEntry e = (ackEntry)sitr.next();
                            try {
                                PacketReference ref = e.acknowledged(false);
                                if (ref != null ) {
                                      Destination d= ref.getDestination();
                                      try {
                                          d.removeMessage(ref.getSysMessageID(),
                                              RemoveReason.ACKNOWLEDGED);
                                      } catch (Exception ex) {
                                          logger.logStack(Logger.INFO,"Internal Error",
                                              ex);
                                      }
                                }
                            } catch (Exception ex) {
                                // ignore
                            }
                        }
                     } else {
                        LinkedHashSet msgs = new LinkedHashSet();
                        Set s = (Set)openMsgs.get(cuid);
                        Iterator sitr = s.iterator();
                        while (sitr.hasNext()) {
                            ackEntry e = (ackEntry)sitr.next();
                            PacketReference ref = e.getReference();
                            if (ref != null) {
                                try {
                                    ref.consumed(sid, !isUnsafeAck(cuid), isAutoAck(cuid));
                                } catch (Exception ex) {
                                    logger.log(Logger.INFO,"Internal Error " +
                                       "Unable to consume " + sid + ":" + ref, ex);
                                }
                                msgs.add(ref);
                            } else {
                                sitr.remove();
                            }
                        }
                        parent.addAllOrdered(msgs);
                     }
                 }
                 deliveredMessages.clear();
                 cleanupList.clear();
                 storeMap.clear();
              }
          }

        synchronized(detachedRConsumerUIDs) {
           itr = (new LinkedHashSet(detachedRConsumerUIDs)).iterator();
        }

        if (!isXATransacted) {
            while (itr.hasNext()) {
                Consumer c =Consumer.newInstance((ConsumerUID)itr.next()); 
                try {
                Globals.getClusterBroadcast().destroyConsumer(c, null, true); 
                } catch (Exception e) {
                logger.log(Logger.WARNING, 
                       "Unable to send consumer ["+c+ 
                       "] cleanup notification for closing of session ["+ this + "].");
                }
            }
        }

          // Clear up old session to consumer match
          synchronized(ConsumerToSession) {
              Iterator citr = ConsumerToSession.values().iterator();
              while (citr.hasNext()) {
                  SessionUID suid = (SessionUID)citr.next();
                  if (suid.equals(uid)) {
                      citr.remove();
                  }

              }
          }
          allSessions.remove(uid);

    }

    /**
     * handles an undeliverable message. This means:
     * <UL>
     *   <LI>removing it from the pending ack list</LI>
     *   <LI>Sending it back to the destination to route </LI>
     * </UL>
     * If the message can not be routed, returns the packet reference
     * (to clean up)
     */
    public PacketReference handleUndeliverable(ConsumerUID cuid,
           SysMessageID id)
        throws BrokerException
    {

        Consumer c = Consumer.getConsumer(cuid);
        // get our stored UID

        ackEntry entry = new ackEntry(id, cuid);
        PacketReference ref = null;
        synchronized(deliveredMessages) {
            entry = (ackEntry)deliveredMessages.remove(entry);
        }
        if (entry == null) {
                return null;
        }
        ref = entry.getReference();
        if (ref == null) {
           // already gone
           return null;
        }
        ConsumerUID storedid = c.getStoredConsumerUID();
        if (storedid.equals(cuid)) {
            //not a durable or receiver, nothing to do
            try {
                if (ref.acknowledged(cuid,
                        storedid, false, false)) {
                    return ref;
                }
            } catch (Exception ex) {
                logger.logStack(Logger.DEBUG,"Error handling undeliverable", ex);
            }
            return null;
        }
        // handle it like an orphan message
        // this re-queues it on the durable or queue
        Destination d = ref.getDestination();
        d.forwardOrphanMessage(ref, storedid);
        return null;
    }

    /**
     * handles an undeliverable message. This means:
     * <UL>
     *   <LI>removing it from the pending ack list</LI>
     *   <LI>Sending it back to the destination to route </LI>
     * </UL>
     * If the message can not be routed, returns the packet reference
     * (to clean up)
     */
    public PacketReference handleDead(ConsumerUID cuid,
           SysMessageID id, RemoveReason deadReason, Throwable thr, 
           String comment, int deliverCnt)
           throws BrokerException {

        if (DEBUG) {
        logger.log(logger.INFO, "handleDead["+id+", "+cuid+"]"+deadReason);
        }
        Consumer c = Consumer.getConsumer(cuid);
        // get our stored UID

        ackEntry entry = new ackEntry(id, cuid);
        PacketReference ref = null;
        synchronized(deliveredMessages) {
            entry = (ackEntry)deliveredMessages.remove(entry);
        }
        if (entry == null) {
                return null;
        }
        ref = entry.getReference();
        if (ref == null) {
           // already gone
           return null;
        }
        ConsumerUID storedid = c.getStoredConsumerUID();
        Destination d = ref.getDestination();
        if (ref.markDead(cuid, storedid, comment, thr, 
                         deadReason, deliverCnt, null)) {
            return ref;
        }
        return null;
    }


    /**
     * @param ackack whether client requested ackack
     */

    public PacketReference ackMessage(ConsumerUID cuid, SysMessageID id, boolean ackack)
        throws BrokerException
    {
        return ackMessage(cuid, id, null, null, ackack);
    }

    public PacketReference ackMessage(ConsumerUID cuid, SysMessageID id,
            TransactionUID tuid, HashMap remoteNotified, boolean ackack) 
        throws BrokerException
    {
        ackEntry entry = new ackEntry(id, cuid);
        PacketReference ref = null;
        synchronized(deliveredMessages) {
            entry = (ackEntry)deliveredMessages.remove(entry);
        }
        if (entry == null) {
            String emsg = null;
            if (tuid == null) {
                emsg = Globals.getBrokerResources().getKString(
                       BrokerResources.W_ACK_MESSAGE_GONE, id+"["+cuid+"]");
            } else {
                emsg = Globals.getBrokerResources().getKString(
                       BrokerResources.W_ACK_MESSAGE_GONE_IN_TXN,
                           tuid.toString(), id+"["+cuid+"]");
            }
            logger.log(Logger.WARNING, emsg);
            throw new BrokerException(emsg, Status.CONFLICT);
        }
        ref = entry.acknowledged(true, tuid, remoteNotified, ackack);
        if (isAutoAck(entry.getConsumerUID())) {
            synchronized(deliveredMessages) {
                Iterator itr = deliveredMessages.values().iterator();
                while (itr.hasNext()) {
                    ackEntry e= (ackEntry)itr.next();
                    PacketReference newref = e.getReference();
                    if (newref == null) {
                        // see if we can get it by ID
                        newref = Destination.get(id);
                        if (newref == null) {
                            logger.log(Logger.DEBUGMED,"Removing purged reference "
                                + e);
                        } else {
                            logger.log(Logger.INFO,"Weird reference behavior" +
                               newref);
                            // acknowledge it
                            try {
                                e.acknowledged(true, ackack);
                            } catch (Exception ex) {
                            }
                        }
                        itr.remove();
                    } else {
                     
                        try {
                            newref.delivered(e.getConsumerUID(),
                              e.getStoredUID(), 
                              true, newref.isStored());
                            break;
                        } catch (Exception ex) {
                           logger.logStack(Logger.WARNING, 
                               Globals.getBrokerResources().getKString(
                                  BrokerResources.W_UNABLE_UPDATE_MSG_DELIVERED_STATE,
                                  ref+"["+cuid+"]", ex.getMessage()), ex);
                        }
                    }
                }
            }
        }
        return ref;
    }

    /**
     * @param ackack whether client requested ackack
     */
    public boolean acknowledgeToMessage(ConsumerUID cuid, SysMessageID id, boolean ackack) 
        throws BrokerException
    {
        boolean removed = false;
        ackEntry entry = new ackEntry(id, cuid);
        synchronized(deliveredMessages) {
            ackEntry value = (ackEntry)deliveredMessages.get(entry);

            if (value == null)  {
                assert false : entry;
                return false;
            }

            Iterator itr = deliveredMessages.values().iterator();
            while (itr.hasNext()) {
                ackEntry val = (ackEntry)itr.next();
                PacketReference ref = val.acknowledged(true, ackack);
                if (ref != null) {
                    Destination d= ref.getDestination();
                    try {
                        d.removeMessage(ref.getSysMessageID(),
                                          RemoveReason.ACKNOWLEDGED);
                    } catch (Exception ex) {
                        logger.logStack(Logger.INFO, "Internal Error", ex);
                    }

                }
                itr.remove();
                removed = true;
                if (val.equals(value)) {
                    break;
                }
            }
        }
        return removed;
    }



    public void eventOccured(EventType type,  Reason r,
            Object target, Object oldval, Object newval, 
            Object userdata) {

        ConsumerUID cuid = ((Consumer)target).getConsumerUID();
        if (type == EventType.BUSY_STATE_CHANGED) {

            synchronized(busyConsumers) {
                Consumer c = (Consumer)consumers.get(cuid);
                if ( c != null && c.isBusy()) {
                    // busy
                    busyConsumers.add(cuid);
                }
            }
            checkState(null); // cant hold a lock, we need to prevent a
                              // deadlock

        } else  {
            assert false : " event is not valid ";
        }
            
    }


     /**
     * Request notification when the specific event occurs.
     * @param listener object to notify when the event occurs
     * @param type event which must occur for notification
     * @param userData optional data queued with the notification
     * @return an id associated with this notification
     * @throws UnsupportedOperationException if the broadcaster does not
     *          publish the event type passed in
     */
    public Object addEventListener(EventListener listener, 
                        EventType type, Object userData)
        throws UnsupportedOperationException {

        if (type != EventType.BUSY_STATE_CHANGED ) {
            throw new UnsupportedOperationException("Only " +
                "Busy and Not Busy types supported on this class");
        }
        return evb.addEventListener(listener,type, userData);
    }

    /**
     * Request notification when the specific event occurs AND
     * the reason matched the passed in reason.
     * @param listener object to notify when the event occurs
     * @param type event which must occur for notification
     * @param userData optional data queued with the notification
     * @param reason reason which must be associated with the
     *               event (or null for all events)
     * @return an id associated with this notification
     * @throws UnsupportedOperationException if the broadcaster does not
     *         support the event type or reason passed in
     */
    public Object addEventListener(EventListener listener, 
                        EventType type, Reason reason,
                        Object userData)
        throws UnsupportedOperationException
    {
        if (type != EventType.BUSY_STATE_CHANGED ) {
            throw new UnsupportedOperationException("Only " +
                "Busy and Not Busy types supported on this class");
        }
        return evb.addEventListener(listener,type, reason, userData);
    }

    /**
     * remove the listener registered with the passed in
     * id.
     * @return the listener callback which was removed
     */
    public Object removeEventListener(Object id) {
        return evb.removeEventListener(id);
    }
  
    private void checkState(Reason r) {

        boolean notify = false;
        boolean isBusy = false;
        synchronized(busyConsumers) {
            isBusy = !paused && (busyConsumers.size() > 0);
            if (isBusy != busy) {
                busy = isBusy;
                notify = true;
            }
        }
        if (notify) {
            notifyChange(EventType.BUSY_STATE_CHANGED,
                r, this, Boolean.valueOf(!isBusy), Boolean.valueOf(isBusy));
        }
       
    }

    private void notifyChange(EventType type,  Reason r, 
               Object target,
               Object oldval, Object newval) 
    {
        evb.notifyChange(type,r, target, oldval, newval);
    }

    public synchronized Consumer getConsumerOnSession(ConsumerUID uid)
    {
        return (Consumer)consumers.get(uid);
    }


// ----------------------------------------------------------------------------
// Static Methods
// ----------------------------------------------------------------------------

    static Map ConsumerToSession = new HashMap();
    static Map allSessions = new HashMap();


    public static void clearSessions()
    {
        ConsumerToSession.clear();
        allSessions.clear();
    }

    public static Session getSession(ConsumerUID uid)
    {
        SessionUID suid = null;
        synchronized(ConsumerToSession) {
            suid = (SessionUID)ConsumerToSession.get(uid);
        }
        if (suid == null) return null;
        return getSession(suid);
    }


    // used for internal errors only
    public static void dumpAll() {
        synchronized(allSessions) {
            Globals.getLogger().log(Logger.INFO,"Dumping active sessions");
            Iterator itr = allSessions.keySet().iterator();
            while (itr.hasNext()) {
                Object k = itr.next();
                Object v = allSessions.get(k);
                Globals.getLogger().log(Logger.INFO,"\t"+k+ " : " + v);
            }
        }
    }



    public static Session createSession(ConnectionUID uid, String id )
    {
        Session s = new Session(uid, id);
        synchronized(allSessions) {
            allSessions.put(s.getSessionUID(), s);
        }
        return s;
    }


    // for default session only
    public static Session createSession(SessionUID uid, ConnectionUID cuid,
                 String id)
    {
        Session s = new Session(uid, cuid, id);
        synchronized(allSessions) {
            allSessions.put(s.getSessionUID(), s);
        }
        return s;
    }


    /**
     * @param clean closeSession was called
     */
    public static void closeSession(SessionUID uid) {
        Session s = null;
        synchronized(allSessions) {
            s = (Session)allSessions.remove(uid);
        }
        if (s == null) {
            // already was closed
            return;
        }
        assert s != null;
        s.close();
    }

    public static Session getSession(SessionUID uid)
    {
        synchronized(allSessions) {
            return (Session)allSessions.get(uid);
        }
    }

    public static Session getSession(String creator)
    {
        if (creator == null) return null;

        synchronized(allSessions) {
            Iterator itr = allSessions.values().iterator();
            while (itr.hasNext()) {
                Session c = (Session)itr.next();
                if (creator.equals(c.creator))
                    return c;
            }
        }
        return null;
    }

}


