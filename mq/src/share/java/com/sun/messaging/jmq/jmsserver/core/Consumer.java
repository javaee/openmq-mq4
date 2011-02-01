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

import java.io.*;
import java.util.Map;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Set;
import java.util.Iterator;

import com.sun.messaging.jmq.util.lists.*;
import com.sun.messaging.jmq.jmsserver.util.lists.*;
import com.sun.messaging.jmq.io.*;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.util.selector.Selector;
import com.sun.messaging.jmq.util.selector.SelectorFormatException;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;
import com.sun.messaging.jmq.jmsserver.service.ConnectionUID;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.service.Connection;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.DMQ;
import com.sun.messaging.jmq.jmsserver.management.agent.Agent;

public class Consumer implements EventBroadcaster, 
     Serializable
{
    transient Logger logger = Globals.getLogger();
    static final long serialVersionUID = 3353669107150988952L;

    public static final String PREFETCH = "prefetch";

    private static boolean DEBUG = false;

    protected static final  boolean DEBUG_CLUSTER_TXN =
        Globals.getConfig().getBooleanProperty(
                            Globals.IMQ + ".cluster.debug.txn") || DEBUG;

    protected static final boolean DEBUG_CLUSTER_MSG =
        Globals.getConfig().getBooleanProperty(
        Globals.IMQ + ".cluster.debug.msg") || DEBUG_CLUSTER_TXN || DEBUG;

    transient private boolean useConsumerFlowControl = false;
    transient private int msgsToConsumer = 0;

    transient Set destinations = null;

    protected transient BrokerResources br = Globals.getBrokerResources();

    private static boolean C_FLOW_CONTROL_ALLOWED = 
             Globals.getConfig().getBooleanProperty(
             Globals.IMQ + ".destination.flowControlAllowed", true);

    long lastAckTime = 0;

    SessionUID sessionuid = null; 
    DestinationUID dest;
    ConsumerUID uid;
    transient ConsumerUID stored_uid;
    ConnectionUID conuid = null;
    transient boolean valid = true;
    transient boolean active = true;
    transient boolean paused = false;
    transient int pauseCnt = 0;
    transient int pauseFlowCnt =0;
    transient int resumeFlowCnt =0;
    boolean noLocal = false;
    transient boolean busy = false;
    transient Subscription parent = null;
    transient boolean isSpecialRemote = false;

    transient boolean isFailover = false;
    transient int position = 0;
    transient int lockPosition = -1;

    transient EventBroadcastHelper evb = null;

    boolean ackMsgsOnDestroy = true;

    transient int flowCount = 0;
    transient boolean flowPaused = false;

    transient int msgsOut = 0;

    transient int prefetch = -1; //unlimited
    transient int remotePrefetch = -1; 

    transient String creator = null;

    transient boolean requestedRecreation = false;

    /**
     * Optional selector string specified by the client application.
     */
    protected String selstr = null;
    protected transient Selector selector = null;

    transient NFLPriorityFifoSet msgs;
    protected transient SubSet parentList = null;


    private transient Object plistener = null;
    private transient Object mlistener = null;

    private transient boolean localConsumerCreationReady = false;

    transient EventListener busylistener = null;

     class BusyListener implements EventListener
        {
            public void eventOccured(EventType type,  Reason r,
                    Object target, Object oldval, Object newval, 
                    Object userdata) {

                assert type == EventType.EMPTY;
                assert newval instanceof Boolean;
                checkState(null);
            }
        }

    transient EventListener removeListener = null;


    class RemoveListener implements EventListener
        {
            public void eventOccured(EventType type,  Reason r,
                    Object target, Object oldval, Object newval, 
                    Object userdata) 
            {
                assert type == EventType.SET_CHANGED_REQUEST;
                if (! (r instanceof RemoveReason)) return;
                // OK .. we are only registered to get the
                // following events:
                //   RemoveReason.EXPIRED
                //   RemoveReason.PURGED
                //   RemoveReason.REMOVED_LOW_PRIORITY
                //   RemoveReason.REMOVED_OLDEST
                //   RemoveReason.REMOVED_REJECTED
                //   RemoveReason.REMOVED_OTHER
                assert r != RemoveReason.UNLOADED;
                assert r != RemoveReason.ROLLBACK;
                assert r != RemoveReason.DELIVERED;
                assert r != RemoveReason.ACKNOWLEDGED;
                assert r != RemoveReason.ROLLBACK;
                assert r != RemoveReason.OVERFLOW;
                assert r != RemoveReason.ERROR;
                PacketReference ref = (PacketReference)oldval;
                msgs.remove(ref);
            }
        }

    private transient Object expiredID = null;
    private transient Object purgedID = null;
    private transient Object removedID1 = null;
    private transient Object removedID2 = null;
    private transient Object removedID3 = null;
    private transient Object removedID4 = null;

    public void addRemoveListener(EventBroadcaster l) {
        // add consumer interest in REMOVE of messages
        expiredID = l.addEventListener(removeListener, EventType.SET_CHANGED_REQUEST,
             RemoveReason.EXPIRED, null);
        purgedID = l.addEventListener(removeListener, EventType.SET_CHANGED_REQUEST,
             RemoveReason.PURGED, null);
        removedID1 = l.addEventListener(removeListener, EventType.SET_CHANGED_REQUEST,
             RemoveReason.REMOVED_OLDEST, null);
        removedID2 = l.addEventListener(removeListener, EventType.SET_CHANGED_REQUEST,
             RemoveReason.REMOVED_LOW_PRIORITY, null);
        removedID3 = l.addEventListener(removeListener, EventType.SET_CHANGED_REQUEST,
             RemoveReason.REMOVED_REJECTED, null);
        removedID4 = l.addEventListener(removeListener, EventType.SET_CHANGED_REQUEST,
             RemoveReason.REMOVED_OTHER, null);
    }

    public void removeRemoveListener(EventBroadcaster l) {
        l.removeEventListener(expiredID);
        l.removeEventListener(purgedID);
        l.removeEventListener(removedID1);
        l.removeEventListener(removedID2);
        l.removeEventListener(removedID3);
        l.removeEventListener(removedID4);
    }

    private boolean getParentBusy() {
    	
        // Fix for CR 6875642 New direct mode hangs when creating a durable subscriber
//    	if (parent !=null){
//    		if (parent.isPaused()){
//    			return false;
//    		}
//    	}
    	
        return (parentList !=null && !parentList.isEmpty())
              || (parent != null && parent.isBusy());
    }

    public void setPrefetch(int count) {
        prefetch = count;
        useConsumerFlowControl = C_FLOW_CONTROL_ALLOWED;
    }
    
    public void setPrefetch(int count, boolean useConsumerFlowControl) {
        prefetch = count;
        this.useConsumerFlowControl = useConsumerFlowControl;
    }

    public void setRemotePrefetch(int count) {
        remotePrefetch = count;
    }

    public int getRemotePrefetch() {
        return remotePrefetch;
    }

    public static int calcPrefetch(Consumer consumer,  int cprefetch) {
        Destination d = consumer.getFirstDestination();
        Subscription sub = consumer.getSubscription();
        int dprefetch =  -1;
        if (d != null ) {
            dprefetch = (sub == null || !sub.getShared()) ?
                         d.getMaxPrefetch() : d.getSharedConsumerFlowLimit();
        }
        int pref = (dprefetch == -1) ?
                        cprefetch :
                            (cprefetch == -1 ? 
                             cprefetch :
                                 (cprefetch > dprefetch ?
                                  dprefetch : cprefetch));
        return pref;
    }

    public long getLastAckTime() {
        return lastAckTime;
    }

    public void setLastAckTime(long time)
    {
        lastAckTime = time;
        if (parent != null) // deal with subscription
            parent.setLastAckTime(time);
    }

    public int getPrefetch()
    {
        return prefetch;
    }

    public void setSubscription(Subscription sub)
    {
        ackMsgsOnDestroy = false;
        parent = sub;
    }

    /**
     * Triggers loading of any associated destiantions
     */
    public void load() {
        Iterator itr = getDestinations().iterator();
        while (itr.hasNext()) {
            Destination d = (Destination)itr.next();
            try {
                d.load();
            } catch (Exception ex) {}
        }
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String id) {
        creator = id;
    }

    public String getClientID() {
        ConnectionUID cuid = getConnectionUID();
        if (cuid == null) return "<unknown>";

        Connection con = (Connection)Globals.getConnectionManager()
                            .getConnection(cuid);
        return (String)con.getClientData(IMQConnection.CLIENT_ID);
        
    }

    public boolean isDurableSubscriber() {
        if (parent != null)
            return parent.isDurable();
        if (this instanceof Subscription)
            return ((Subscription)this).isDurable();
        return false;
    }

    public boolean getIsFlowPaused() {
        return flowPaused;
    }

    public void msgRetrieved() {
        msgsOut ++;
    }

    public int totalMsgsDelivered()
    {
        return msgsOut;
    }

    public int numPendingAcks()
    {
        Session s = Session.getSession(sessionuid);
        if (s == null) return 0;
        return s.getNumPendingAcks(getConsumerUID());
    }

    public Subscription getSubscription() {
        return parent;
    }

    protected static Selector getSelector(String selstr)
        throws SelectorFormatException
    {
        return Selector.compile(selstr);

    }


    public SubSet getParentList() {
        return parentList;
    }

    transient    Object destroyLock = new Object(); 

    public void destroyConsumer(Set delivered, 
                                boolean destroyingDest) {
        destroyConsumer(delivered, (Map)null, false, destroyingDest, true);
    }
   
    public void destroyConsumer(Set delivered, 
                                boolean destroyingDest, boolean notify) {
        destroyConsumer(delivered, (Map)null, false, destroyingDest, notify); 
    }

    public void destroyConsumer(Set delivered, Map remotePendings, 
                boolean remoteCleanup, boolean destroyingDest, boolean notify) {
        if (DEBUG)
            logger.log(logger.DEBUG, "destroyConsumer : " + this +
                       ", delivered size="+delivered.size());
        synchronized(destroyLock) {
            if (!valid) {
               // already removed
                return;
            }
            valid = false; // we are going into destroy, so we are invalid
        }


        Subscription sub = parent;

        if (sub != null) {
            sub.pause("Consumer.java: destroy " + this);
        }

        pause("Consumer.java: destroy ");

        // clean up hooks to any parent list
        if (parentList != null && plistener != null) {
            parentList.removeEventListener(plistener);
            plistener = null;
        } 

        Set ds = getDestinations();

        SubSet oldParent = null;
        synchronized(plock) {
           oldParent = parentList;
           parentList = null;
        }
        if (parent != null) {
            parent.releaseConsumer(uid);
            parent= null;
            if (notify) {
                try {
                    sendDestroyConsumerNotification(remotePendings, remoteCleanup);
                } catch (Exception ex) {
                    logger.log(Logger.INFO,
                        "Internal Error: sending detach notification for "
                        + uid + " from " + parent, ex);
                }
            }
        } else {
            if ((ds == null || ds.isEmpty()) && !destroyingDest ) {
                // destination already gone
                // can happen if the destination is destroyed 
                logger.log(Logger.DEBUG,"Removing consumer from non-existant destination" + dest);
            } else if (!destroyingDest) {
                Iterator itr = ds.iterator();
                while (itr.hasNext()) {
                    Destination d = null;
                    try {
                        d= (Destination)itr.next();
                        d.removeConsumer(uid, remotePendings, remoteCleanup, notify); 
                    } catch (Exception ex) {
                        logger.logStack(Logger.INFO,"Internal Error: removing consumer "
                              + uid + " from " + d, ex);
                    }
                }
            }
        }
        if (DEBUG)
             logger.log(Logger.DEBUG,"Destroying consumer " + this + "[" +
                  delivered.size() + ":" + msgs.size() + "]" );


        // now clean up and/or requeue messages
        // any consumed messages will stay on the session
        // until we are done

        Set s = new LinkedHashSet(msgs);
        Reason cleanupReason = (ackMsgsOnDestroy ?
               RemoveReason.ACKNOWLEDGED : RemoveReason.UNLOADED);


        delivered.addAll(s);

       // automatically ack any remote  or ack On Destroy messages
        Iterator itr = delivered.iterator();
        while (itr.hasNext()) {
                PacketReference r = (PacketReference)itr.next();
                if (r == null) continue;
                if (ackMsgsOnDestroy || !r.isLocal()) {
                    itr.remove();
                     try {
                         if (r.acknowledged(getConsumerUID(),
                                 getStoredConsumerUID(),
                                 !uid.isUnsafeAck(), r.isLocal())) {
                             Destination d = Destination.getDestination(r.getDestinationUID());
                             if (d != null) {
                                 if (r.isLocal()) {
                                 d.removeMessage(r.getSysMessageID(),
                                      RemoveReason.ACKNOWLEDGED);
                                 } else {
                                 d.removeRemoteMessage(r.getSysMessageID(),
                                              RemoveReason.ACKNOWLEDGED, r);
                                 }
                             }
                         }
                     } catch(Exception ex) {
                         logger.log(Logger.DEBUG,"Broker down Unable to acknowlege"
                            + r.getSysMessageID() + ":" + uid, ex);
                     }
                }
        }
                
      
        msgs.removeAll(s, cleanupReason);

        if (!ackMsgsOnDestroy) {
            if (oldParent != null) {
                ((Prioritized)oldParent).addAllOrdered(delivered);
                delivered.clear(); // help gc
            }
        } 
  
        destroy();

        if (msgs != null && mlistener != null) {
            msgs.removeEventListener(mlistener);
            mlistener = null;
        }

        if (sub != null) {
            sub.resume("Consumer.java: destroyConsumer " + this);
        }
        selstr = null;
        selector = null;
    }

    /**
     * This method is called from ConsumerHandler.
     */
    public void sendCreateConsumerNotification() throws BrokerException {

        // OK, we may have multiple consumers
        // we just want the first one
       
        Destination d = getFirstDestination();

        if ((dest.isWildcard() || (! d.getIsLocal() && ! d.isInternal() && ! d.isAdmin())) &&
            Globals.getClusterBroadcast() != null) {
            Globals.getClusterBroadcast().createConsumer(this);
        }
    }

    public void sendDestroyConsumerNotification(Map remotePendings, boolean remoteCleanup) 
                throws BrokerException {
        // OK, we may have multiple consumers
        // we just want the first one
       
        Destination d = getFirstDestination();

        if ( dest.isWildcard() || (d != null && ! d.getIsLocal() && ! d.isInternal() && ! d.isAdmin() &&
            Globals.getClusterBroadcast() != null)) {
            Globals.getClusterBroadcast().destroyConsumer(this, remotePendings, remoteCleanup);
        }
    }

    protected void destroy() {
        valid = false;
        pause("Consumer.java: destroy()");
        synchronized (consumers){
            consumers.remove(uid);
        }
        wildcardConsumers.remove(uid);
        selector = null; // easier for weak hashmap
        Reason cleanupReason = RemoveReason.UNLOADED;
        if (ackMsgsOnDestroy) {
            cleanupReason = RemoveReason.ACKNOWLEDGED;
        }
        Set s = new HashSet(msgs);

        try {
            synchronized (s) {
                Iterator itr = s.iterator();
                while (itr.hasNext()) {
                    PacketReference pr = (PacketReference)itr.next();
                    if (ackMsgsOnDestroy && pr.acknowledged(getConsumerUID(),
                              getStoredConsumerUID(),
                             !uid.isUnsafeAck(), true))
                    {
                        Destination d=Destination.getDestination(pr.getDestinationUID()); 
                        d.removeMessage(pr.getSysMessageID(),
                            cleanupReason);
                    }
                }
            }
            msgs.removeAll(s, cleanupReason);
        } catch (Exception ex) {
            logger.log(Logger.WARNING,"Internal Error: Problem cleaning consumer " 
                            + this, ex);
        }

        
    }

    transient    Object plock = new Object(); 
    /**
     * the list (if any) that the consumer can pull
     * messages from
     */
    public void setParentList(SubSet set) {
        ackMsgsOnDestroy = false;
        if (parentList != null ) {
            if (plistener != null) {
                parentList.removeEventListener(plistener);
            }
            plistener = null;
        }
        assert  plistener == null;
        synchronized(plock) {
            parentList = set;

            if (parentList != null) {
                plistener = parentList.addEventListener(
                     busylistener, EventType.EMPTY, null);
            } else {
                assert plistener == null;
            }
        }
        checkState(null);
    }


    protected void getMoreMessages(int num) {
        SubSet ss = null;
        synchronized(plock) {
            ss = parentList;
        }
        if (paused || ss == null || ss.isEmpty() || (parent != null &&
             parent.isPaused())) {
                // nothing to do
                return;
        } else {
            // pull
            int count = 0;
            if (ss.isEmpty())  {
                  return;
            }
            while (!isFailover && isActive() &&  !isPaused() && isValid() && ss != null 
               && !ss.isEmpty() && count < num && (parent == null ||
                  !parent.isPaused())) 
            {
                    PacketReference mm = (PacketReference)ss.removeNext();
                    if (mm == null)  {
                        continue;
                    }
                    msgs.add(11-mm.getPriority(), mm);
                    count ++;
                    busy = true;
            }
        }

    }

    void setIsActiveConsumer(boolean active) {
        isFailover = !active;
        checkState(null);
    }

    public boolean getIsFailoverConsumer() {
        return isFailover;
    }

    public boolean getIsActiveConsumer() {
        return !isFailover;
    }

    /**
     * handles transient data when class is deserialized
     */
    private void readObject(java.io.ObjectInputStream ois)
        throws IOException, ClassNotFoundException
    {
        ois.defaultReadObject();
        logger = Globals.getLogger();
        remotePendingResumes = new ArrayList();
        lastDestMetrics = new Hashtable();
        delayNextFetchForRemote = 0;
        localConsumerCreationReady = false;
        parent = null;
        busy = false;
        paused = false;
        flowPaused = false;
        flowCount = 0;
        active = true;
        valid = true;
        destroyLock = new Object();
        plock = new Object(); 
        plistener = null;
        mlistener = null;
        parentList = null;
        prefetch=-1;
        isFailover = false;
        position = 0;
        isSpecialRemote = false;
        parent = null;
        useConsumerFlowControl = false;
        stored_uid = null;
        active = true;
        pauseCnt = 0;
        try {
            selector = getSelector(selstr);
        } catch (Exception ex) {
            logger.log(Logger.ERROR,"Internal Error: bad stored selector["
                  + selstr + "], ignoring",
                  ex);
            selector = null;
        }
        initInterest();
    }

    public boolean isBusy() {
        return busy;
    }

    public DestinationUID getDestinationUID() {
        return dest;
    }

    protected Consumer(ConsumerUID uid) {
        // used for removing a consumer during load problems
        // only
        // XXX - revisit changing protocol to use UID so we
        // dont have to do this 
        this.uid = uid;
    }

    public static Consumer newInstance(ConsumerUID uid) {
        return new Consumer(uid);
    }

    public Consumer(DestinationUID d, String selstr, 
            boolean noLocal, ConnectionUID con_uid)
        throws IOException,SelectorFormatException
    {
        dest = d;
        this.noLocal = noLocal;
        uid = new ConsumerUID();
        uid.setConnectionUID(con_uid);
        this.selstr = selstr;
        selector = getSelector(selstr);
        initInterest();
        logger.log(Logger.DEBUG,"Created new consumer "+ uid +
              " on destination " + d + " with selector " + selstr);
              
    }

    public Consumer(DestinationUID d, String selstr,
            boolean noLocal, ConsumerUID uid)
        throws IOException,SelectorFormatException
    {
        dest = d;
        this.noLocal = noLocal;
        this.uid = uid;
        if (uid == null)
            this.uid = new ConsumerUID();

        this.selstr = selstr;
        selector = getSelector(selstr);
        initInterest();
    }

    public static Consumer newConsumer(DestinationUID destid, String selectorstr,
                                       boolean isnolocal, ConsumerUID consumerid)
                                       throws IOException,SelectorFormatException,
                                       ConsumerAlreadyAddedException {
        synchronized(consumers) {
            if (consumers.get(consumerid) != null) {
                throw new ConsumerAlreadyAddedException(
                    Globals.getBrokerResources().getKString(
                    BrokerResources.I_CONSUMER_ALREADY_ADDED,
                    consumerid, destid)); 
            }
            return new Consumer(destid, selectorstr, isnolocal, consumerid);
        }
    }

    public void localConsumerCreationReady() {
        synchronized(consumers) {
            localConsumerCreationReady = true;
        }
    }

    private boolean isLocalConsumerCreationReady() {
        synchronized(consumers) {
            return localConsumerCreationReady;
        }
    }

    public boolean isValid() {
        return valid;
    }

    protected void initInterest()
    {
        removeListener = new RemoveListener();
        busylistener = new BusyListener();
        evb = new EventBroadcastHelper();
        msgs = new NFLPriorityFifoSet(12, false);
        mlistener = msgs.addEventListener(
             busylistener, EventType.EMPTY, null);
        synchronized(consumers){
            consumers.put(uid, this);
        }
        if (dest.isWildcard())
            wildcardConsumers.add(uid);

    }

    public PacketReference  peekNext() {
        // ok first see if there is anything on msgs
        PacketReference ref = (PacketReference)msgs.peekNext();
        if (ref == null && parentList != null)
            ref = (PacketReference)parentList.peekNext();
        return ref;
    }

    public void setAckMsgsOnDestroy(boolean ack) {
        ackMsgsOnDestroy = ack;
    }

    public ConsumerUID getConsumerUID() {
        return uid;
    }

    public void setStoredConsumerUID(ConsumerUID uid) {
        stored_uid = uid;
    }
    public ConsumerUID getStoredConsumerUID() {
        if (stored_uid == null) {
            return uid;
        }
        return stored_uid;
    }


    public boolean routeMessages(Collection c, boolean toFront) {
        if (toFront) {
            if (!valid)
                return false;
            msgs.addAllToFront(c,0);
            synchronized (destroyLock) {
                msgsToConsumer += c.size();
            }
            checkState(null);
        } else {
            Iterator itr = c.iterator();
            while (itr.hasNext()) {
                routeMessage((PacketReference)itr.next(), false);
            }
        }
        if (!valid) return false;
        return true;
    }
    
    /**
     * called from load transactions, when we need to unroute 
     * a message that has been consumed in a prepared transaction
     */
    public boolean unrouteMessage(PacketReference p)
    {
    	 boolean b = msgs.remove(p);
         if (b) {
             msgsToConsumer --;
         }
         return b;
    }

    /**
     * can be called from cluster dispatch thread for remote messages  
     * on return false, must ensure message not routed
     */
    public boolean routeMessage(PacketReference p, boolean toFront) {
        int position = 0;
        if (!toFront) {
            position = 11 - p.getPriority();
        }

        synchronized(destroyLock) {
            if (!valid) return false;
            msgs.add(position, p);
            msgsToConsumer ++;
        }
        
        checkState(null);
        return true;
    }

    public int size() {
        return msgs.size();
    }

    // messages in this list may be in:
    //   - in msgs;
    //   - in session list
    public int numInProcessMsgs() {
        int cnt =size();
        cnt += numPendingAcks();
        return cnt;
    }

    public void unloadMessages() {
        msgs.clear();
        synchronized (destroyLock) {
            msgsToConsumer = 0;
        }
    }    

    public void attachToConnection(ConnectionUID uid)
    {
        this.conuid = uid;
        this.uid.setConnectionUID(uid);
    }

    public void attachToSession(SessionUID uid) 
    {
        this.sessionuid = uid;
    }

    public void attachToDestination(Destination d)
    {

            if (destinations == null)
                destinations = new HashSet();

            synchronized (destinations){
                this.destinations.add(d);
            }
    }

    public Set getDestinations() {
        HashSet snapshot = null;

            if (this.destinations == null) {
                destinations = new HashSet();
                synchronized (destinations){
                    if (!dest.isWildcard()) {
                        destinations.add(Destination.getDestination(dest));
                    } else {
                        List l = Destination.findMatchingIDs(dest);
                        Iterator itr = l.iterator();
                        while (itr.hasNext()) {
                            DestinationUID duid = (DestinationUID)itr.next();
                            destinations.add(Destination.getDestination(duid));
                        }

                    }
                    snapshot = new HashSet(destinations);
                }
            } else {
                synchronized(destinations){
                    snapshot = new HashSet(destinations);
                }
            }
        return snapshot;
    }

    public Destination getFirstDestination()
    {
        Iterator itr = getDestinations().iterator();
        Destination d = (Destination) (itr.hasNext() ? itr.next() : null);
        return d;
    }

    public SessionUID getSessionUID() {
        return sessionuid;
    }
    public ConnectionUID getConnectionUID()
    {
        return conuid;
    }


    public void delayNextFetchForRemote(long millsecs) {
        delayNextFetchForRemote = millsecs;
    }

    public PacketReference getAndFillNextPacket(Packet p) 
    {
        PacketReference ref = null;

        if (flowPaused || paused) {
            checkState(null);
            return null;
        }
        if (Destination.MAX_DELAY_NEXT_FETCH_MILLISECS > 0 &&
            delayNextFetchForRemote >0 && !paused && valid) {
            long delays = delayNextFetchForRemote/2;
            if (delays > 0) {
                long sleeptime = (delays > Destination.MAX_DELAY_NEXT_FETCH_MILLISECS ?
                                  Destination.MAX_DELAY_NEXT_FETCH_MILLISECS:delays);
                long sleep1 = (Destination.MAX_DELAY_NEXT_FETCH_MILLISECS < 5 ?
                               Destination.MAX_DELAY_NEXT_FETCH_MILLISECS:5);
                long slept = 0;
                while (slept < sleeptime && !paused && valid) {
                    try {
                        Thread.sleep(sleep1);
                        slept +=sleep1;
                    } catch (Exception e) {}
                    delayNextFetchForRemote = 0;
                }
            }
        }
        if (!valid) {
            return null;
        }
        if (!paused &&  msgs.isEmpty()) {
            getMoreMessages(prefetch <=0 ? 1000 : prefetch);
        }

        Packet newpkt = null;

        while (valid && !paused && !msgs.isEmpty()) {
            ref= (PacketReference)msgs.removeNext();
            if (ref == null || ref.isOverrided()) {
                int loglevel = (DEBUG_CLUSTER_MSG) ? Logger.INFO:Logger.DEBUG; 
                logger.log(loglevel,  (ref == null) ? 
                "Consumer ["+getConsumerUID()+"] get message null reference":
                "Consumer ["+getConsumerUID()+"] message requened: "+ref);
                continue;
            }
            newpkt = ref.getPacket();
            if (newpkt == null || !ref.checkRemovalAndSetInDelivery(getStoredConsumerUID()) ) {  
                try {
                    boolean expired = ref.isExpired();
                    String cmt = br.getKString(br.I_RM_EXPIRED_MSG_BEFORE_DELIVER_TO_CONSUMER,
                                 ref.getSysMessageID(), "["+uid+", "+uid.getAckMode()+"]"+dest);
                    String cmtr = br.getKString(br.I_RM_EXPIRED_REMOTE_MSG_BEFORE_DELIVER_TO_CONSUMER,
                                  ref.getSysMessageID(), "["+uid+", "+uid.getAckMode()+"]"+dest);
                    boolean islocal = ref.isLocal();
                    if (ref.markDead(uid, getStoredConsumerUID(), (islocal ? cmt:cmtr), null,
                        (expired ? RemoveReason.EXPIRED_ON_DELIVERY:RemoveReason.REMOVED_OTHER), -1, null)) {
                        boolean removed = false;
                        Destination d = Destination.getDestination(ref.getDestinationUID());
                        if (d != null) {
                            if (ref.isDead()) {
                                removed = d.removeDeadMessage(ref);
                            } else {
                                removed = d.removeMessage(ref.getSysMessageID(),
                                              RemoveReason.REMOVED_OTHER, !expired);
                            }
                            if (removed && expired && d.getVerbose()) {
                                logger.log(logger.INFO, (islocal ? cmt:cmtr));
                            }
                        }
                    }

                } catch (Exception ex) {
                    if (newpkt != null && DEBUG) {
                        logger.logStack(Logger.INFO, 
                        "Unable to cleanup removed message "+ref+" for consumer "+this, ex);
                    }
                }
                ref = null;
                newpkt = null;
                continue;
            }

            break;
        }
        if (!valid) {
            if (DEBUG_CLUSTER_MSG) {
                logger.log(Logger.INFO, "getAndFillNextPacket(): consumer "+this+
                                        " closed, discard ref "+ref);
            }
            return null;
        }
        if (ref == null) {
            checkState(null);
            return null;
        }

        newpkt = ref.getPacket();
        if (newpkt == null) {
            assert false;
            return null;
        }

        if (p != null) {
            try {
                p.fill(newpkt);
            } catch (IOException ex) {
                logger.logStack(Logger.INFO,"Internal Exception processing packet ", ex);
                return null;
            }
            p.setConsumerID(uid.longValue());
            p.setRedelivered(ref.getRedeliverFlag(getStoredConsumerUID()));
            if (ref.isLast(uid)) {
                ref.removeIsLast(uid);
                p.setIsLast(true);
            }
            msgRetrieved();
            if (parent != null) {
                // hey, we pulled one from the durable too
                parent.msgRetrieved();
            }
        } else {
            newpkt.setRedelivered(ref.getRedeliverFlag(getStoredConsumerUID()));
        }

        if (useConsumerFlowControl) {
           if (prefetch != -1) {
              flowCount ++;
           }
           if (!flowPaused && ref.getMessageDeliveredAck(uid)) {
               BrokerAddress addr = ref.getAddress();
               if (addr != null) { // do we have a remove
                   synchronized(remotePendingResumes) {
                       remotePendingResumes.add (ref);
                   }
               } else {
               }
               if (p != null) {
                   p.setConsumerFlow(true);
                }
            } 
            if (prefetch > 0 && flowCount >= prefetch ) {
                if (p != null) {
                    p.setConsumerFlow(true);
                }
                ref.addMessageDeliveredAck(uid);
                BrokerAddress addr = ref.getAddress();
                if (addr != null) { // do we have a remove
                    synchronized(remotePendingResumes) {
                       remotePendingResumes.add (ref);
                    }
                }
                pauseFlowCnt ++;
                flowPaused = true;
            }
        } else if (ref.getMessageDeliveredAck(uid)) {
            HashMap props = null;
            ConnectionUID cuid = getConnectionUID();
            if (cuid != null) {
                IMQConnection con = (IMQConnection)Globals.getConnectionManager().
                                                         getConnection(cuid);
                if (con != null) {
                    props = new HashMap();
                    props.put(Consumer.PREFETCH, 
                              new Integer(con.getFlowCount()));
                }
            }
            try {
                Globals.getClusterBroadcast().acknowledgeMessage(
                    ref.getAddress(), ref.getSysMessageID(),
                    uid, ClusterBroadcast.MSG_DELIVERED, props, false);
            } catch (BrokerException ex) {
                logger.log(Logger.DEBUG,"Can not send DELIVERED ack "
                     + " received ", ex);
            }
            ref.removeMessageDeliveredAck(uid);
       }
       return ref;

    }



    public void purgeConsumer() 
         throws BrokerException
    {
        Reason cleanupReason = RemoveReason.ACKNOWLEDGED;
        Set set = new HashSet(msgs);
        if (set.isEmpty()) return;
        msgs.removeAll(set, cleanupReason);
        Iterator itr = set.iterator();
        while (itr.hasNext()) {
            try {
                PacketReference pr = (PacketReference)itr.next();
                if (pr.acknowledged(getConsumerUID(),
                       getStoredConsumerUID(),
                       !uid.isUnsafeAck(), true))
                {
                     Destination d = Destination.getDestination(pr.getDestinationUID());
                     d.removeMessage(pr.getSysMessageID(), cleanupReason);
                }
            } catch (IOException ex) {
                logger.log(Logger.WARNING,"Internal Error: purging consumer " 
                        + this , ex);
            }
        }
                
    }

    public boolean isWildcard() {
        return dest.isWildcard();
    }

    public void purgeConsumer(Filter f) 
         throws BrokerException
    {
        Reason cleanupReason = RemoveReason.ACKNOWLEDGED;
        Set set = msgs.getAll(f);
        msgs.removeAll(set, cleanupReason);
        Iterator itr = set.iterator();
        while (itr.hasNext()) {
            try {
                PacketReference pr = (PacketReference)itr.next();
                if (pr.acknowledged(getConsumerUID(),
                      getStoredConsumerUID(),
                     !uid.isUnsafeAck(), true))
                {
                     Destination d = Destination.getDestination(pr.getDestinationUID());
                     d.removeMessage(pr.getSysMessageID(), cleanupReason);
                }
            } catch (IOException ex) {
                logger.log(Logger.WARNING,"Internal Error: Problem purging consumer " 
                        + this, ex);
            }
        }
                
    }

    public void activate() {
        active = true;
        checkState(null);
    }

    public void deactive() {
        active = false;
        checkState(null);
    }

    public void pause(String reason) {
        synchronized(msgs) {
            paused = true;
            pauseCnt ++;
            if (DEBUG)
                logger.log(logger.DEBUG,"Pausing consumer " + this 
                      + "[" + pauseCnt + "] " + reason);
        }
        checkState(null);
    }

    public void resume(String reason) {
        synchronized(msgs) {
            pauseCnt --;
            if (pauseCnt <= 0) {
                paused = false;
            }
            if (DEBUG)
               logger.log(logger.DEBUG,"Pausing consumer " + this 
                     + "[" + pauseCnt + "] " + reason);
        }
        checkState(null);
    }

    public void setFalconRemote(boolean notlocal)
    {
        isSpecialRemote = notlocal;
    }

    public boolean isFalconRemote() {
        return isSpecialRemote;
    }

    //not used 
    HashSet remotePendingDelivered = new HashSet();

    transient private ArrayList remotePendingResumes = new ArrayList();
    transient private Hashtable lastDestMetrics = new Hashtable();
    transient private long delayNextFetchForRemote = 0;

    //return 0, yes; 1 no previous sampling, else no
    public int checkIfMsgsInRateGTOutRate(Destination d) {

        Subscription sub = getSubscription();
        if (!Destination.CHECK_MSGS_RATE_FOR_ALL && 
            !(sub != null && sub.getShared() && !sub.isDurable())) {
            return 2;
        }
        if (Destination.CHECK_MSGS_RATE_AT_DEST_CAPACITY_RATIO < 0) {
            return 2;
        }
        float pc =Math.max(Destination.totalCountPercent(),
                         d.destMessagesSizePercent());
        if (pc < (float)Destination.CHECK_MSGS_RATE_AT_DEST_CAPACITY_RATIO) {
            return 2;
        }
        synchronized(lastDestMetrics) {
            DestinationUID did = d.getDestinationUID(); 
            long[] holder = (long[])lastDestMetrics.get(did);
            if (holder == null) {
                holder = new long[6];
                d.checkIfMsgsInRateGTOutRate(holder, true);
                lastDestMetrics.put(did, holder);
                return 1;
            }
            return d.checkIfMsgsInRateGTOutRate(holder, false);
        }
    }

    public long getMsgOutTimeMillis(Destination d) {
        synchronized(lastDestMetrics) {
            DestinationUID did = d.getDestinationUID(); 
            long[] holder = (long[])lastDestMetrics.get(did);
            if (holder == null) {
                holder = new long[6];
                d.checkIfMsgsInRateGTOutRate(holder, true);
                lastDestMetrics.put(did, holder);
                return -1;
            }
            d.checkIfMsgsInRateGTOutRate(holder, false);
            return holder[5];
        }
    }

    /**
     * resume flow not changing flow control
     * from remote broker
     */
    public void resumeFlow() {

        // deal w/ remote flow control 
        synchronized (remotePendingResumes) {
            if (!remotePendingResumes.isEmpty()) {
               Iterator itr = remotePendingResumes.iterator();
               while (itr.hasNext()) {
                   PacketReference ref = (PacketReference)itr.next();
                   //DELIVERED translated to resume flow on remote client
                   try {
                        Globals.getClusterBroadcast().acknowledgeMessage(
                            ref.getAddress(), ref.getSysMessageID(),
                            uid, ClusterBroadcast.MSG_DELIVERED, null, false);
                   } catch (BrokerException ex) {
                        logger.log(Logger.DEBUG,"Can not send DELIVERED ack "
                             + " received ", ex);
                   }
                   itr.remove();
               }
            }
        }
        if (flowPaused) { 
            resumeFlowCnt ++;
            flowCount = 0;
            flowPaused = false;
            checkState(null); 
        }
    }

    public void throttleRemoteFlow(PacketReference ref) {
        synchronized (remotePendingResumes) {
            HashMap props = new HashMap();
            props.put(Consumer.PREFETCH, new Integer(1));
            try {
                Globals.getClusterBroadcast().acknowledgeMessage(
                    ref.getAddress(), ref.getSysMessageID(),
                    uid, ClusterBroadcast.MSG_DELIVERED, props, false);
            } catch (BrokerException ex) {
                remotePendingResumes.add(ref);
                logger.log(Logger.DEBUG,"Can not send DELIVERED ack " + " received ", ex);
            }
        }
    }

    private void resumeRemoteFlow(int id) {
        synchronized (remotePendingResumes) {
            if (!remotePendingResumes.isEmpty()) {
               SubSet p = parentList;
               boolean emptyp = (p == null || p.isEmpty()); 
               int remoteSize = remotePendingResumes.size();

               //estimate remote prefetch
               int prefetchVal = id; 
               if (id > 0) { 
                   if (prefetchVal != 1) {
                       prefetchVal = id/(remoteSize+(emptyp ? 0:1));
                       if (prefetchVal == 0) { 
                           prefetchVal = 1;
                       }
                   }
               }
               int myprefetch = prefetchVal;
               int totalPrefetched = 0;
               BrokerAddress addr = null;
               ArrayList addrs = new ArrayList();
               Destination d = null;
               PacketReference ref = null;
               HashMap props = null;
               long systemrm = -1, destrm = -1, room = -1;
               Iterator itr = remotePendingResumes.iterator();
               while (itr.hasNext()) {
                   ref = (PacketReference)itr.next();
                   addr = ref.getAddress();
                   if (addrs.contains(addr)) {
                       itr.remove();
                       continue;
                   }
                   myprefetch = prefetchVal;
                   systemrm = destrm = room = -1; 
                   props = new HashMap();
                   d = Destination.findDestination(ref.getDestinationUID());
 
                   try { 
                       systemrm = Destination.checkSystemLimit(ref);
                   } catch (Throwable t) {
                       systemrm = 0;
                   }
                   if (d != null) {
                       destrm = d.checkDestinationCapacity(ref);
                   }
                   if (systemrm >= 0 && destrm >= 0) {
                       room = Math.min(systemrm, destrm);
                   } else if(systemrm >= 0) {
                       room = systemrm;
                   } else {
                       room = destrm;
                   }
                   if (room == 0) {
                       room = 1;
                   }
                   myprefetch = prefetchVal;
                   if (room > 0) {
                       myprefetch = (int)(room - totalPrefetched);
                       if (myprefetch <= 0) {
                           myprefetch = 1;
                       } else {
                           int ret = checkIfMsgsInRateGTOutRate(d);
                           if (ret == 0) {
                               myprefetch = 1;
                           } else if (ret == 1) {
                               myprefetch = 1;
                           }
                       }
                       if (prefetchVal > 0) {
                           myprefetch =  Math.min(prefetchVal, myprefetch);
                           if (myprefetch <= 0) { 
                               myprefetch = 1;
                           }
                       }
                   }
                   //DELIVERED translated to resume flow on remote client
                   try {
                        props.put(Consumer.PREFETCH, new Integer(myprefetch));
                        Globals.getClusterBroadcast().acknowledgeMessage(
                            addr, ref.getSysMessageID(),
                            uid, ClusterBroadcast.MSG_DELIVERED, props, false);
                        if (addr != null) {
                            addrs.add(addr);
                        }
                        totalPrefetched += myprefetch;
                        itr.remove();
                   } catch (BrokerException ex) {
                        logger.log(Logger.DEBUG,"Can not send DELIVERED ack "
                             + " received ", ex);
                   }
               }
            }
        }
    }

    public void resumeFlow(int id) {
        resumeRemoteFlow(id);
        setPrefetch(id);
        if (flowPaused) { 
            resumeFlowCnt ++;
            flowCount = 0;
            flowPaused = false;
            checkState(null); 
        }
    }

    public boolean isActive() {
        return active;
    }

    public boolean isPaused() {
        return paused;
    }

    public String getSelectorStr() {
        return selstr;
    }
    public Selector getSelector() {
        return selector;
    }

    public boolean getNoLocal() {
        return noLocal;
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
                "Busy State Changed notifications supported on this class");
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
                "Busy State Changed notifications supported on this class");
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
  
    private void notifyChange(EventType type,  Reason r, 
               Object target,
               Object oldval, Object newval) 
    {
        evb.notifyChange(type,r, target, oldval, newval);
    }

    public void dump(String prefix) {
        if (prefix == null)
            prefix = "";
        logger.log(Logger.INFO,prefix + "Consumer: " + uid + " [paused, active,"
            + "flowPaused, parentBusy, hasMessages, parentSize ] = [" 
            + paused + "," + active + "," + flowPaused + "," +
            getParentBusy() + "," + (msgs == null || !msgs.isEmpty()) + ","  
            + (parentList == null ? 0 : parentList.size()) + "]");
        logger.log(Logger.INFO,prefix +"Busy state [" + uid + "] is " + busy);
        if (msgs == null)
            logger.log(Logger.INFO, "msgs is null");
        else
            logger.log(Logger.INFO, msgs.toDebugString());
        if (parentList == null)
            logger.log(Logger.INFO, "parentList is null");
        else
            logger.log(Logger.INFO, parentList.toDebugString());
    }

    public static Hashtable getAllDebugState() {
        Hashtable ht = new Hashtable();
        ht.put("FlowControlAllowed", String.valueOf(C_FLOW_CONTROL_ALLOWED));
        ht.put("ConsumerCnt", String.valueOf(consumers.size()));
        Iterator itr = getAllConsumers();
        while (itr.hasNext()) {
           Consumer c = (Consumer)itr.next();
           ht.put("Consumer["+c.getConsumerUID().longValue()+"]",
                c.getDebugState());
        }
        return ht;
    }
    public Hashtable getDebugState() {
        Hashtable ht = new Hashtable();
        ht.put("ConsumerUID", String.valueOf(uid.longValue()));
        ht.put("Broker", (uid.getBrokerAddress() == null ? "NONE" 
                   : uid.getBrokerAddress().toString()));
        ht.put("msgsToConsumer", String.valueOf(msgsToConsumer));
        ht.put("StoredConsumerUID", String.valueOf(getStoredConsumerUID().longValue()));
        ht.put("ConnectionUID", (conuid == null ? "none" : String.valueOf(conuid.longValue())));
        ht.put("type", "CONSUMER");
        ht.put("valid", String.valueOf(valid));
        ht.put("paused", String.valueOf(paused));
        ht.put("pauseCnt", String.valueOf(pauseCnt));
        ht.put("noLocal", String.valueOf(noLocal));
        ht.put("destinationUID", dest.toString());
        ht.put("busy", String.valueOf(busy));
        if (parent != null)
            ht.put("Subscription", String.valueOf(parent.
                     getConsumerUID().longValue()));
        ht.put("isSpecialRemote", String.valueOf(isSpecialRemote));
        ht.put("ackMsgsOnDestroy", String.valueOf(ackMsgsOnDestroy));
        ht.put("position", String.valueOf(position));
        ht.put("active", String.valueOf(active));
        ht.put("flowCount", String.valueOf(flowCount));
        ht.put("flowPaused", String.valueOf(flowPaused));
        ht.put("pauseFlowCnt", String.valueOf(pauseFlowCnt));
        ht.put("resumeFlowCnt", String.valueOf(resumeFlowCnt));
        ht.put("useConsumerFlowControl", String.valueOf(useConsumerFlowControl));
        ht.put("selstr", (selstr == null ? "none" : selstr));
        SubSet tmpparentList = null;
        Object tmpplistener = null;
        synchronized(plock) {
            tmpparentList = parentList;
            tmpplistener = plistener;
        }
        if (tmpparentList == null) {
            ht.put("parentList", "null");
        } else {
            ht.put("parentList.isEmpty", tmpparentList.isEmpty());
            if (Destination.DEBUG_LISTS) {
                ht.put("parentList.size", tmpparentList.size());
                ht.put("parentList", tmpparentList.toDebugString());
            }
        }
        if (tmpplistener == null) {
            ht.put("plistener", "null");
        } else {
            ht.put("plistener", tmpplistener.toString());
        }
        ht.put("prefetch", String.valueOf(prefetch));
        ht.put("remotePrefetch", String.valueOf(remotePrefetch));
        ht.put("parentBusy", String.valueOf(getParentBusy()));
        ht.put("hasMessages", String.valueOf(!msgs.isEmpty()));
        ht.put("msgsSize", String.valueOf(msgs.size()));
        if (Destination.DEBUG_LISTS) {
            ht.put("msgs", msgs.toDebugString());
        }
        ht.put("isFailover", String.valueOf(isFailover));
        ht.put("localConsumerCreationReady", String.valueOf(localConsumerCreationReady));
        Vector v1 = new Vector();
        ArrayList vals = null;
        synchronized(remotePendingResumes) {
            vals = new ArrayList(remotePendingResumes);
        }
        Iterator itr = vals.iterator();
        while (itr.hasNext()) {
            PacketReference ref = (PacketReference)itr.next();
            String val = ref.getAddress()+": "+ref;
            v1.add(val);
        }
        ht.put("remotePendingResumes", v1);
        Vector v2 = new Vector();
        synchronized(lastDestMetrics) {
            itr = lastDestMetrics.keySet().iterator();
            while (itr.hasNext()) {
                DestinationUID did = (DestinationUID)itr.next();
                long[] holder = (long[])lastDestMetrics.get(did);
                String val = did+": "+"ins=" +holder[0]+"|outs="+holder[1]+"|time="+holder[2]+
                             "|inr="+holder[3]+"|outr="+holder[4]+"|outt="+holder[5];
                v2.add(val);
            }
        }
        ht.put("lastDestMetrics", v2);
        ht.put("delayNextFetchForRemote", String.valueOf(delayNextFetchForRemote));
        return ht;
    }

    public Vector getDebugMessages(boolean full) {
        Vector ht = new Vector();
        synchronized(msgs) {
            Iterator itr = msgs.iterator();
            while (itr.hasNext()) {
                PacketReference pr = (PacketReference)itr.next();
                ht.add( (full ? pr.getPacket().dumpPacketString() :
                     pr.getPacket().toString()));
            }
        }
        return ht;
       
    }

    private void checkState(Reason r) {
        // XXX - LKS look into adding an assert that a lock
        // must be held when this is called
        boolean isbusy = false;
        boolean notify = false;
        synchronized (msgs) {
            isbusy = !paused && active && !flowPaused &&
                ( (getParentBusy() && !isFailover) || !msgs.isEmpty());
            notify = isbusy != busy;
            busy = isbusy;
        }
        if (notify) {
            notifyChange(EventType.BUSY_STATE_CHANGED,
                r, this, Boolean.valueOf(!isbusy), Boolean.valueOf(isbusy));
        }
    }


    /**
     * Return a concises string representation of the object.
     * 
     * @return    a string representation of the object.
     */
    public String toString() {
        String str = "Consumer - "+ dest  + ":" + getConsumerUID();
        return str;
    }

    public void debug(String prefix) {
        if (prefix == null)
            prefix = "";
        logger.log(Logger.INFO,prefix + toString() );
        String follow = prefix + "\t";
        logger.log(Logger.INFO, follow + "Selector = " + selector);
        logger.log(Logger.INFO, follow + "msgs = " + msgs.size());
        logger.log(Logger.INFO, follow + "parentList = " + (parentList == null ? 0 : parentList.size()));
        logger.log(Logger.INFO, follow + "parent = " + parent);
        logger.log(Logger.INFO, follow + "valid = " + valid);
        logger.log(Logger.INFO, follow + "active = " + active);
        logger.log(Logger.INFO, follow + "paused = " + paused);
        logger.log(Logger.INFO, follow + "pauseCnt = " + pauseCnt);
        logger.log(Logger.INFO, follow + "noLocal = " + noLocal);
        logger.log(Logger.INFO, follow + "busy = " + busy);
        logger.log(Logger.INFO, follow + "flowPaused = " + flowPaused);
        logger.log(Logger.INFO, follow + "prefetch = " + prefetch);
        logger.log(Logger.INFO,follow + msgs.toDebugString());
    }


    public void setCapacity(int size) {
        msgs.setCapacity(size);
    }
    public void setByteCapacity(long size) {
        msgs.setByteCapacity(size);
    }
    public int capacity() {
        return msgs.capacity();
    }
    public long byteCapacity()
    {
        return msgs.byteCapacity();
    }


    private static Map consumers = Collections.synchronizedMap(new HashMap());

    protected static Set wildcardConsumers = Collections.synchronizedSet(new HashSet());

    public static void clearAllConsumers()
    {
        consumers.clear();
        wildcardConsumers.clear();
    }

    public static Iterator getAllConsumers() {
        return getAllConsumers(true);
    }
    public static Iterator getAllConsumers(boolean all) {
        synchronized (consumers) {
            HashSet cs = new HashSet(consumers.values());
            if (all) {
                return cs.iterator();
            }
            Consumer co = null;
            Iterator itr = cs.iterator();
            while (itr.hasNext()) {
                co = (Consumer)itr.next();
                if (co instanceof Subscription) {
                    continue;
                }
                if (!co.isLocalConsumerCreationReady()) {
                    itr.remove();
                }
            }
            return cs.iterator();
        }
    }

    public static Iterator getWildcardConsumers() {
        synchronized (consumers) {
            return (new HashSet(wildcardConsumers)).iterator();
        }
    }

    public static int getNumConsumers() {
        return (consumers.size());
    }

    public static int getNumWildcardConsumers() {
        return wildcardConsumers.size();
    }

    public static Consumer getConsumer(ConsumerUID uid)
    {
        synchronized(consumers) {
            Consumer c = (Consumer)consumers.get(uid);
            return c;
        }
    }

    public static Consumer getConsumer(String creator)
    {
        if (creator == null) return null;

        synchronized(consumers) {
            Iterator itr = consumers.values().iterator();
            while (itr.hasNext()) {
                Consumer c = (Consumer)itr.next();
                if (creator.equals(c.getCreator()))
                    return c;
            }
        }
        return null;
    }

     public void setLockPosition(int position) {
         lockPosition = position;
     }
     public int getLockPosition() {
         return lockPosition;
     }

     public void recreationRequested() {
         requestedRecreation = true;
     }
     protected boolean tobeRecreated() {
         return requestedRecreation;
     }
}

