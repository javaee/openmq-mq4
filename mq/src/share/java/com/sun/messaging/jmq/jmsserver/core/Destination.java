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
 * @(#)Destination.java	1.320 11/26/07
 */ 

package com.sun.messaging.jmq.jmsserver.core;


import com.sun.messaging.jmq.jmsserver.DMQ;
import com.sun.messaging.jmq.util.DestType;
import com.sun.messaging.jmq.util.selector.*;
import com.sun.messaging.jmq.util.DestState;
import com.sun.messaging.jmq.io.DestMetricsCounters;
import com.sun.messaging.jmq.io.PacketType;

import com.sun.messaging.jmq.util.DestLimitBehavior;
import com.sun.messaging.jmq.util.ClusterDeliveryPolicy;
import com.sun.messaging.jmq.util.DestScope;
import com.sun.messaging.jmq.util.timer.*;
import com.sun.messaging.jmq.util.GoodbyeReason;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsservice.BrokerEvent;
import com.sun.messaging.jmq.jmsserver.BrokerStateHandler;
import com.sun.messaging.jmq.jmsserver.service.ConnectionUID;
import com.sun.messaging.jmq.jmsserver.service.Connection;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQBasicConnection;
import com.sun.messaging.jmq.jmsserver.service.ConnectionManager;
import com.sun.messaging.jmq.jmsserver.license.LicenseBase;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.util.ConflictException;
import com.sun.messaging.jmq.jmsserver.util.ConsumerAlreadyAddedException;
import com.sun.messaging.jmq.jmsserver.config.*;
import com.sun.messaging.jmq.jmsserver.util.lists.AddReason;
import com.sun.messaging.jmq.jmsserver.util.lists.RemoveReason;
import com.sun.messaging.jmq.jmsserver.util.ConflictException;
import com.sun.messaging.jmq.jmsserver.util.DestinationNotFoundException;
import com.sun.messaging.jmq.jmsserver.util.FeatureUnavailableException;
import com.sun.messaging.jmq.jmsserver.util.TransactionAckExistException;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.data.handlers.RefCompare;
import com.sun.messaging.jmq.jmsserver.data.TransactionList;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.data.TransactionAcknowledgement;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.persist.LoadException;
import com.sun.messaging.jmq.jmsserver.persist.ChangeRecordInfo;
import com.sun.messaging.jmq.jmsserver.service.HAMonitorService;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.util.SizeString;
import com.sun.messaging.jmq.util.admin.DestinationInfo;
import com.sun.messaging.jmq.io.Status;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;

import com.sun.messaging.jmq.jmsserver.management.agent.Agent;

import com.sun.messaging.jmq.util.lists.*;

import java.util.*;
import java.io.*;
import java.lang.*;

/**
 * This class represents a destination (topic or queue name)
 */
public abstract class Destination implements java.io.Serializable,
         com.sun.messaging.jmq.util.lists.EventListener
{

protected String INITIALIZEBY = "";

    private static boolean DEBUG_CLUSTER = Globals.getConfig().getBooleanProperty(
                                           Globals.IMQ + ".cluster.debug.ha") ||
                                           Globals.getConfig().getBooleanProperty(
                                           Globals.IMQ + ".cluster.debug.txn") ||
                                           Globals.getConfig().getBooleanProperty(
                                           Globals.IMQ + ".cluster.debug.msg");
    public static String DEBUG_LISTS_PROP = Globals.IMQ+".lists.debug";
    public static boolean DEBUG_LISTS = Globals.getConfig().getBooleanProperty(DEBUG_LISTS_PROP);

 
    public static final int LOAD_COUNT = Globals.getConfig().getIntProperty(
              Globals.IMQ + ".destination.verbose.cnt", 10000);

    static final long serialVersionUID = 4399175316523022128L;

    public static final boolean PERSIST_SYNC = Globals.getConfig().getBooleanProperty(
                 Globals.IMQ + ".persist.file.sync.enabled", false);
    private static boolean DEBUG = DEBUG_CLUSTER;

    public static final boolean EXPIRE_DELIVERED_MSG = Globals.getConfig().getBooleanProperty(
                 Globals.IMQ + ".destination.expireDeliveredMessages", false);

    public static final boolean PURGE_DELIVERED_MSG = Globals.getConfig().getBooleanProperty(
                 Globals.IMQ + ".destination.purgeDeliveredMessages", false);

    public static final boolean NO_PRODUCER_FLOW = Globals.getConfig().
                 getBooleanProperty(Globals.IMQ + ".noProducerFlow", false);

    /**
     * maximum size of a batch of messages/producer
     */
    public static final int DEFAULT_MAX_PRODUCER_BATCH = 1000;

    /**
     * default maximum size of a destination
     */
    public static final int DEFAULT_DESTINATION_SIZE=100000;

    /**
     * default maximum number of producers/destination
     */
    public static final int DEFAULT_MAX_PRODUCERS = 100;

    /**
     * default consumer prefetch value 
     */
    public static final int DEFAULT_PREFETCH = 1000;
    public static final int ALL_DESTINATIONS_MASK = 0;
    public static final int TEMP_DESTINATIONS_MASK = DestType.DEST_TEMP;
    public static final int UNLIMITED=-1;
    public static final int NONE=0;

    private static final  String AUTO_QUEUE_STR 
              = Globals.IMQ + ".autocreate.queue";
    private static final  String AUTO_TOPIC_STR 
              = Globals.IMQ + ".autocreate.topic";
    private static final  String DST_REAP_STR 
              = Globals.IMQ + ".autocreate.reaptime";
    private static final  String MSG_REAP_STR 
              = Globals.IMQ + ".message.expiration.interval";

    public static final String CHECK_MSGS_RATE_AT_DEST_CAPACITY_RATIO_PROP =
        Globals.IMQ + ".cluster.prefetch.checkMsgRateAtCapacityRatio";

    public static int CHECK_MSGS_RATE_AT_DEST_CAPACITY_RATIO = Globals.getConfig().
        getIntProperty(CHECK_MSGS_RATE_AT_DEST_CAPACITY_RATIO_PROP, 5);

    public static final String CHECK_MSGS_RATE_FOR_ALL_PROP =
        Globals.IMQ + ".cluster.prefetch.checkMsgRateAll";

    public static boolean CHECK_MSGS_RATE_FOR_ALL = Globals.getConfig().
        getBooleanProperty(CHECK_MSGS_RATE_FOR_ALL_PROP, false);

    public static final String MAX_DELAY_NEXT_FETCH_MILLISECS_PROP =
        Globals.IMQ + ".cluster.prefetch.delayNextMsgFetchMaxMilliSeconds";

    public static int MAX_DELAY_NEXT_FETCH_MILLISECS = Globals.getConfig().
        getIntProperty(MAX_DELAY_NEXT_FETCH_MILLISECS_PROP, 1000);

    private static final long DEFAULT_TIME = 120;

    private static boolean ALLOW_QUEUE_AUTOCREATE =
          Globals.getConfig().getBooleanProperty(
             AUTO_QUEUE_STR, true);

    private static boolean ALLOW_TOPIC_AUTOCREATE =
          Globals.getConfig().getBooleanProperty(
             AUTO_TOPIC_STR, true);

    public static long AUTOCREATE_EXPIRE =
         Globals.getConfig().getLongProperty(DST_REAP_STR,
             DEFAULT_TIME) * 1000;

    public static long MESSAGE_EXPIRE =
         Globals.getConfig().getLongProperty(MSG_REAP_STR,
             DEFAULT_TIME) * 1000;

    public static final int MAX_PRODUCER_BATCH =
            Globals.getConfig().getIntProperty(
                Globals.IMQ + ".producer.maxBatch",
                DEFAULT_MAX_PRODUCER_BATCH);

    public static final int MAX_PRODUCER_BYTES_BATCH = -1;

    transient protected boolean destvalid = true;
    transient protected boolean startedDestroy =false;

    transient Set BehaviorSet = null;

    protected transient Logger logger = Globals.getLogger();
    protected transient BrokerResources br = Globals.getBrokerResources();

    protected transient BrokerMonitor bm = null;

    protected transient boolean stored = false;
    protected transient boolean neverStore = false;
    protected transient SimpleNFLHashMap destMessages = null;
    private transient HashMap destMessagesInRemoving =  null;
    private transient Object _removeMessageLock =  null;
    private boolean dest_inited = false;

    private transient int refCount = 0;

    private static boolean CAN_MONITOR_DEST = false;
    private static boolean CAN_USE_LOCAL_DEST = false;

    static {
        if (Globals.getLogger().getLevel() <= Logger.DEBUG) DEBUG = true;
        if (DEBUG) {
           Globals.getLogger().log(Logger.INFO, "Syncing message store: " + PERSIST_SYNC);
        }

        if (NO_PRODUCER_FLOW)
           Globals.getLogger().log(Logger.INFO, "Producer flow control is turned off ");

        try {
            LicenseBase license = Globals.getCurrentLicense(null);
            CAN_MONITOR_DEST = license.getBooleanProperty(
                                license.PROP_ENABLE_MONITORING, false);
            CAN_USE_LOCAL_DEST = license.getBooleanProperty(
                                license.PROP_ENABLE_LOCALDEST, false);
        } catch (BrokerException ex) {
            CAN_MONITOR_DEST = false;
            CAN_USE_LOCAL_DEST = false;
        }
    }


    /**
     * metrics counters
     */
    protected int expiredCnt = 0;
    protected int purgedCnt = 0;
    protected int ackedCnt = 0;
    protected int discardedCnt = 0;
    protected int overflowCnt = 0;
    protected int errorCnt = 0;
    protected int rollbackCnt = 0;

    /**
     * size of a destination when it is unloaded
     */
    transient int size = 0;
    transient int remoteSize = 0;

    /**
     * bytes of a destination when it is unloaded
     */
    transient long bytes = 0;
    transient long remoteBytes = 0;
    transient boolean loaded = false;
    protected transient SimpleNFLHashMap consumers = new SimpleNFLHashMap();
    protected transient SimpleNFLHashMap producers = new SimpleNFLHashMap();

    private static MQTimer timer = Globals.getTimer();
    transient DestReaperTask destReaper = null;

    protected DestinationUID uid = null;
    protected int type = -1;
    protected transient int state = DestState.UNKNOWN;
    protected int scope = DestScope.CLUSTER;
    protected int limit = DestLimitBehavior.REJECT_NEWEST;
    protected ConnectionUID id = null; // only for temp destinations
    protected SizeString msgSizeLimit = null; 
    protected int countLimit = 0;
    protected SizeString memoryLimit = null;


    private static final String AUTO_MAX_NUM_MSGS=Globals.IMQ+
                 ".autocreate.destination.maxNumMsgs";
    private static final String AUTO_MAX_TOTAL_BYTES=Globals.IMQ+
                 ".autocreate.destination.maxTotalMsgBytes";
    private static final String AUTO_MAX_BYTES_MSG=Globals.IMQ+
                 ".autocreate.destination.maxBytesPerMsg";
    public static final String AUTO_MAX_NUM_PRODUCERS=Globals.IMQ+
                 ".autocreate.destination.maxNumProducers";
    private static final String AUTO_LOCAL_ONLY=Globals.IMQ+
                 ".autocreate.destination.isLocalOnly";
    private static final String AUTO_LIMIT_BEHAVIOR=Globals.IMQ+
                 ".autocreate.destination.limitBehavior";

    protected static int defaultMaxMsgCnt= Globals.getConfig().
                  getIntProperty(AUTO_MAX_NUM_MSGS, DEFAULT_DESTINATION_SIZE);

    protected static int defaultProducerCnt= Globals.getConfig().
                  getIntProperty(AUTO_MAX_NUM_PRODUCERS, DEFAULT_MAX_PRODUCERS);

    private static final long _defbytes = 1024*10*1024;
    // XXX - LKS back out fix to message sizes
    //private static final long _defbytes = 1024*10;
    protected static SizeString defaultMaxMsgBytes= Globals.getConfig().
                  getSizeProperty(AUTO_MAX_TOTAL_BYTES, _defbytes);

    private static final long _defMbytes = 10*1024;
    // XXX - LKS back out fix to message sizes
    // private static final long _defMbytes = 10;
    protected static SizeString defaultMaxBytesPerMsg= Globals.getConfig().
                  getSizeProperty(AUTO_MAX_BYTES_MSG, _defMbytes);

    protected static boolean defaultIsLocal= Globals.getConfig().
            getBooleanProperty(AUTO_LOCAL_ONLY, false);

    protected static int defaultLimitBehavior= 
                DestLimitBehavior.getStateFromString(
                     Globals.getConfig().
                         getProperty(AUTO_LIMIT_BEHAVIOR,
                         "REJECT_NEWEST"));

    protected int maxConsumerLimit = UNLIMITED; 
    protected int maxProducerLimit = defaultProducerCnt;
    protected int maxPrefetch = DEFAULT_PREFETCH;


    protected transient int producerMsgBatchSize = MAX_PRODUCER_BATCH;
    protected transient long producerMsgBatchBytes =  -1;


    private long clientReconnectInterval = 0;


    private transient ReconnectReaperTask reconnectReaper = null;
    private static int reconnectMultiplier= Globals.getConfig().
                  getIntProperty(Globals.IMQ+
                 ".reconnect.interval", 5);


    private transient ProducerFlow producerFlow = new ProducerFlow();

    // DEAD MESSAGE QUEUE PROPERTIES

    public static final String USE_DMQ_STR = Globals.IMQ +
                   ".autocreate.destination.useDMQ";

    public static final String TRUNCATE_BODY_STR = Globals.IMQ +
                   ".destination.DMQ.truncateBody";

    public static final String LOG_MSGS_STR = Globals.IMQ +
                   ".destination.logDeadMsgs";


    public static boolean defaultUseDMQ = 
                Globals.getConfig().getBooleanProperty(USE_DMQ_STR, true);

    public static final boolean defaultTruncateBody = 
                Globals.getConfig().getBooleanProperty(
                       TRUNCATE_BODY_STR, false);

    public static final boolean defaultVerbose = 
                Globals.getConfig().getBooleanProperty(
                      LOG_MSGS_STR, false );

    private static Queue deadMessageQueue = null;
    private boolean unloadMessagesAtStore = false;
    public static final String DMQ_NAME="mq.sys.dmq";

    private static boolean autocreateUseDMQ = defaultUseDMQ;
    boolean useDMQ = autocreateUseDMQ;
    static boolean storeBodyWithDMQ = !defaultTruncateBody;
    static boolean verbose = defaultVerbose;

    boolean isDMQ = false;

    boolean validateXMLSchemaEnabled = false;
    String XMLSchemaUriList = null;
    boolean reloadXMLSchemaOnFailure = false;

    private transient boolean clusterNotifyFlag = false;

    private transient Map<Integer, ChangeRecordInfo> currentChangeRecordInfo = 
        Collections.synchronizedMap(new HashMap<Integer, ChangeRecordInfo>());

    public ChangeRecordInfo getCurrentChangeRecordInfo(int type) {
        return currentChangeRecordInfo.get(Integer.valueOf(type));
    }

    public void setCurrentChangeRecordInfo(int type, ChangeRecordInfo cri) {
        currentChangeRecordInfo.put(Integer.valueOf(type), cri);
    }

    public PacketReference peekNext() {
        return null;
    }

    public void setUseDMQ(boolean use) 
        throws BrokerException
    {
        if (use && isDMQ)
            throw new BrokerException(br.getKString(
                            BrokerResources.X_DMQ_USE_DMQ_INVALID));
	Boolean oldVal = Boolean.valueOf(this.useDMQ);
        this.useDMQ = use;

        notifyAttrUpdated(DestinationInfo.USE_DMQ, 
			oldVal, Boolean.valueOf(this.useDMQ));
    }
    public boolean getUseDMQ() {
        return useDMQ;
    }
    public static void storeBodyInDMQ(boolean store) {
        storeBodyWithDMQ = store;
    }
    public static boolean getStoreBodyInDMQ() {
        return storeBodyWithDMQ;
    }
    public static void setVerbose(boolean v) {
        verbose = v;
    }
    public static boolean getVerbose() {
        return verbose;
    }

    public static Queue getDMQ() {
        return deadMessageQueue;
    }

    // used only as a space holder when deleting corrupted destinations
    protected Destination(DestinationUID uid) {
         this.uid = uid;
    }

    private static synchronized Queue createDMQ() 
         throws BrokerException, IOException
    {
        DestinationUID uid = DestinationUID.getUID(
                    DMQ_NAME, true);
        
        Queue dmq = null;
        dmq = (Queue) destinationList.get(uid);
        try {
            if (dmq == null) {
                Globals.getLogger().log(Logger.INFO, BrokerResources.I_DMQ_CREATING_DMQ);
                dmq =(Queue)Destination.createDestination(DMQ_NAME, 
                      DestType.DEST_TYPE_QUEUE | DestType.DEST_DMQ,
                      true, false, null, false, false);
                dmq.maxProducerLimit = 0;
                dmq.scope=(Globals.getHAEnabled() ? DestScope.CLUSTER
                           :DestScope.LOCAL);
                dmq.msgSizeLimit= null;
                dmq.setLimitBehavior(DestLimitBehavior.REMOVE_OLDEST);
                dmq.memoryLimit = new SizeString(1024*10);
                dmq.countLimit = 1000;
                dmq.setCapacity(1000);
                dmq.maxPrefetch=1000;
                // deal with remaining properties
                dmq.isDMQ=true;
                dmq.useDMQ=false;
                dmq.update();
            }
        } catch (BrokerException ex) {
            if (ex.getStatusCode() == Status.CONFLICT) {
                // another broker created this while we were loading
                Globals.getLogger().log(Logger.DEBUG,"Another broker has created the DMQ, reloading");
                dmq = (Queue)Globals.getStore().getDestination(uid);
            } else {
                throw ex;
            }
        }
        dmq.load(true, null, null);
        return dmq;
    }


    public static final String TEMP_CNT="JMQ_SUN_JMSQ_TempRedeliverCnt";

    /**
     * this method is called when a message has
     * been completely acked and is dead
      */
    public boolean removeDeadMessage(PacketReference ref)
        throws IOException, BrokerException
    {
        return removeDeadMessage(ref, ref.getDeadComment(),
            ref.getDeadException(), ref.getDeadDeliverCnt(),
            ref.getDeadReason(), ref.getDeadBroker()); 
    }

    public static boolean removeDeadMessage(SysMessageID sysid,
        String comment, Throwable exception, int deliverCnt,
         Reason r, String broker) 
        throws IOException, BrokerException
    {
        PacketReference ref = get(sysid);
        Destination d = ref.getDestination();
        return d.removeDeadMessage(ref, comment, exception,
                                   deliverCnt, r, broker);
    }

    public boolean removeDeadMessage(PacketReference ref,
        String comment, Throwable exception, int deliverCnt,
         Reason r, String broker) 
        throws IOException, BrokerException
    {

        if (DEBUG) {
            logger.log(Logger.DEBUG,"Calling removeDeadMessage on " + ref
                 + " [" + comment + "," + exception+ "," + 
                 deliverCnt + "," + r + "]");
        }   
        if (ref.isInvalid()) {
            logger.log(Logger.DEBUG, "Internal Error: message is already dead");
            return false;
        }

        Destination d = ref.getDestination();
        if (d == deadMessageQueue) {
            throw new RuntimeException("Already dead");
        }

        Hashtable m = new Hashtable();

        if (comment != null)
            m.put(DMQ.UNDELIVERED_COMMENT, comment);

        if (deliverCnt != -1)
            m.put(TEMP_CNT, new Integer(deliverCnt));

        if (exception!= null)
            m.put(DMQ.UNDELIVERED_EXCEPTION, exception);

        if (broker != null)
            m.put(DMQ.DEAD_BROKER, broker);
        else
            m.put(DMQ.DEAD_BROKER, Globals.getMyAddress().toString());

        // remove the old message
        if (r == null)
            r = RemoveReason.ERROR;

        RemoveMessageReturnInfo ret = _removeMessage(ref.getSysMessageID(), r, m,
                                                     null, !ref.isExpired());
        return ret.removed;
    }

    public int seqCnt = 0;

    /**
     * place the message in the DMQ.<P>
     * called from Destination and Consumer.
     */
    void markDead(PacketReference pr, Reason reason,
        Hashtable props)
        throws BrokerException
    {
        
        Packet p = pr.getPacket();
        if (p == null) {
            logger.log(Logger.DEBUG,"Internal Error: null packet for DMQ");
            return;
        }
        Hashtable packetProps = null;
        try {
            packetProps = p.getProperties();
            if (packetProps == null)
                packetProps = new Hashtable();
        } catch (Exception ex) {
            logger.logStack(Logger.DEBUG,"could not get props ", ex);
            packetProps = new Hashtable();
        }

        boolean useVerbose = false;

        Object o = packetProps.get(DMQ.VERBOSE);
        if (o != null) {
            if (o instanceof Boolean) {
                useVerbose = ((Boolean)o).booleanValue();
            } else if (o instanceof String) {
                useVerbose = Boolean.valueOf((String)o).booleanValue();
            } else {
                logger.log(Logger.WARNING,
                       BrokerResources.E_INTERNAL_BROKER_ERROR,
                      "Unknown type for verbose " + o.getClass());
                useVerbose=verbose;
            }
        } else {
            useVerbose = verbose;
        }

        if (isDMQ) {
            if (DEBUG || useVerbose) {
                logger.log(Logger.INFO, BrokerResources.I_DMQ_REMOVING_DMQ_MSG,
                     pr.getSysMessageID(),
                     DestinationUID.getUID(p.getDestination(),
                            p.getIsQueue()).toString());
            }
            return;
        }

        // OK deal with various flags
        boolean useDMQforMsg = false;

        o = packetProps.get(DMQ.PRESERVE_UNDELIVERED);
        if (o != null) {
            if (o instanceof Boolean) {
                useDMQforMsg = ((Boolean)o).booleanValue();
            } else if (o instanceof String) {
                useDMQforMsg = Boolean.valueOf((String)o).booleanValue();
            } else {
                logger.log(Logger.WARNING,
                    BrokerResources.E_INTERNAL_BROKER_ERROR,
                    "Unknown type for preserve undelivered " +
                       o.getClass());
                useDMQforMsg=useDMQ;
            }
        } else {
            useDMQforMsg = useDMQ;
        }

        long receivedTime = pr.getTime();
        long senderTime = pr.getTimestamp();
        long expiredTime = pr.getExpireTime();

        if (!useDMQforMsg) {
            if (DEBUG || useVerbose) {
                String args[] = { pr.getSysMessageID().toString(),
                    pr.getDestinationUID().toString(),
                    lookupReasonString(reason, receivedTime,
                       expiredTime, senderTime) };
                logger.log(Logger.INFO, BrokerResources.I_DMQ_REMOVING_MSG, args); 
            }
            if (!pr.isLocal()) {
                Globals.getClusterBroadcast().acknowledgeMessage(
                                              pr.getAddress(),
                                              pr.getSysMessageID(),
                                              pr.getQueueUID(), 
                                              ClusterBroadcast.MSG_DEAD,
                                              props, true /*wait for ack*/);
           }
           return;
        }

        boolean truncateBody = false;
        o = packetProps.get(DMQ.TRUNCATE_BODY);
        if (o != null) {
            if (o instanceof Boolean) {
                truncateBody = ((Boolean)o).booleanValue();
            } else if (o instanceof String) {
                truncateBody = Boolean.valueOf((String)o).booleanValue();
            } else {
                logger.log(Logger.WARNING,
                      BrokerResources.E_INTERNAL_BROKER_ERROR,
                      "Unknown type for preserve undelivered " +
                       o.getClass());
                truncateBody=!storeBodyWithDMQ;
            }
        } else {
            truncateBody = !storeBodyWithDMQ;
        }

        if (props == null) {
            props = new Hashtable();
        }
        Integer cnt = (Integer)props.remove(TEMP_CNT);
        if (cnt != null) {
            // set as a header property
            props.put("JMSXDeliveryCount", cnt);
        } else { // total deliver cnt ?
        }

        if (pr.isLocal())  {
            props.putAll(packetProps);
        } else {
            // reason for the other side
            props.put("REASON", new Integer(reason.intValue()));
        }

        if (props.get(DMQ.UNDELIVERED_COMMENT) == null) {
            props.put(DMQ.UNDELIVERED_COMMENT, lookupReasonString(reason,
                  receivedTime, expiredTime, senderTime));
        }

        props.put(DMQ.UNDELIVERED_TIMESTAMP,
             new Long(System.currentTimeMillis()));

        props.put(DMQ.BODY_TRUNCATED,
             Boolean.valueOf(truncateBody));

        String reasonstr = null;

        if (reason == RemoveReason.EXPIRED || 
            reason == RemoveReason.EXPIRED_BY_CLIENT || 
            reason == RemoveReason.EXPIRED_ON_DELIVERY) {
            props.put(DMQ.UNDELIVERED_REASON,
                  DMQ.REASON_EXPIRED);
        } else if (reason == RemoveReason.REMOVED_LOW_PRIORITY) {
            props.put(DMQ.UNDELIVERED_REASON,
                 DMQ.REASON_LOW_PRIORITY);
        } else if (reason == RemoveReason.REMOVED_OLDEST) {
            props.put(DMQ.UNDELIVERED_REASON,
                 DMQ.REASON_OLDEST);
        } else if (reason == RemoveReason.UNDELIVERABLE) {
            props.put(DMQ.UNDELIVERED_REASON,
                 DMQ.REASON_UNDELIVERABLE);
        } else {
            props.put(DMQ.UNDELIVERED_REASON,
                 DMQ.REASON_ERROR);
        }
        if (pr.getAddress() != null)
            props.put(DMQ.BROKER, pr.getAddress().toString());
        else
            props.put(DMQ.BROKER, Globals.getMyAddress().toString());

        String deadbkr = (String)packetProps.get(DMQ.DEAD_BROKER);

        if (deadbkr != null)
            props.put(DMQ.DEAD_BROKER, deadbkr);
        else
            props.put(DMQ.DEAD_BROKER, Globals.getMyAddress().toString());

        if (!pr.isLocal()) {
            Globals.getClusterBroadcast().
                acknowledgeMessage(pr.getAddress(),
                pr.getSysMessageID(), pr.getQueueUID(), 
                ClusterBroadcast.MSG_DEAD, props, true /*wait for ack*/);
            return; // done

        }

        // OK ... now create the packet
        Packet newp = new Packet();

        // first make sure we have the room to put it on the
        // queue ... if we dont, an exception will be thrown
        // from queue Message
        boolean route = false;
        PacketReference ref = null;
        try {
            newp.generateSequenceNumber(false);
            newp.generateTimestamp(false);
            newp.fill(p);
            newp.setProperties(props);

            if (truncateBody) {
                newp.setMessageBody(new byte[0]);
            }
    
            ref = PacketReference.createReference(
                   newp, deadMessageQueue.getDestinationUID(), null);
            ref.overrideExpireTime(0);
            ref.clearExpirationInfo();
            ref.setTimestamp(System.currentTimeMillis());
            synchronized (deadMessageQueue) {
                ref.setSequence(deadMessageQueue.seqCnt++);
            }
            routeMoveAndForwardMessage(pr, ref, deadMessageQueue);
        } catch (Exception ex) {
            // depending on the type, we either ignore or throw out
            if (reason == RemoveReason.UNDELIVERABLE ||
                reason ==  RemoveReason.ERROR) {
                    if (ex instanceof BrokerException) {
                        throw (BrokerException) ex;
                    } 
                    throw new BrokerException( 
                        br.getKString( BrokerResources.X_DMQ_MOVE_INVALID), ex);
            }
            if (DEBUG || useVerbose) {
                logger.logStack(Logger.WARNING, BrokerResources.W_DMQ_ADD_FAILURE,
                     pr.getSysMessageID().toString(), ex);
            }

        }
        if (  (DEBUG || useVerbose) && useDMQforMsg ) {
             String args[] = { pr.getSysMessageID().toString(),
                      pr.getDestinationUID().toString(),
                      lookupReasonString(reason,
                  receivedTime, expiredTime, senderTime) };
             logger.log(Logger.INFO, BrokerResources.I_DMQ_MOVING_TO_DMQ,
                    args); 
        }

        ref.unload();
        
    }

    /**
     * replaces the body of the message, adds the addProps (if not null)
     * and returns a new SysMessageID
     *
     */
    protected PacketReference _replaceMessage(SysMessageID old, Hashtable addProps,
                          byte[] data)
        throws BrokerException, IOException
    {
        PacketReference ref = get(old);
        long oldbsize = ref.byteSize();

        ArrayList subs = new ArrayList();
        Consumer c = null;
        Iterator itr = getConsumers();
        while (itr.hasNext()) {
            c = (Consumer)itr.next();
            if (c instanceof Subscription) {
                if (c.unrouteMessage(ref)) {
                    subs.add(c);
                }
            }
        }

        SysMessageID newid = ref.replacePacket(addProps, data);
        destMessages.remove(old);
        destMessages.put(newid, ref);
        synchronized(this.getClass()) {
            totalbytes += (ref.byteSize() - oldbsize);
        }
        removePacketList(old, ref.getDestinationUID());
        packetlistAdd(newid, ref.getDestinationUID());

        Subscription sub = null;
        itr = subs.iterator();
        while (itr.hasNext()) {
            sub = (Subscription)itr.next();
            sub.routeMessage(ref, false);
        }

        return ref;
    }

    public SysMessageID replaceMessage(SysMessageID old, Hashtable addProps,
                          byte[] data)
        throws BrokerException, IOException
    {
        return _replaceMessage(old, addProps, data).getSysMessageID();
    }

    public String replaceMessageString(SysMessageID old, Hashtable addProps,
                          byte[] data)
        throws BrokerException, IOException
    {
        return _replaceMessage(old, addProps, data).getSysMessageIDString();
    }

    public static void routeMoveAndForwardMessage(PacketReference oldRef, 
                  PacketReference newRef, Destination target)
	throws IOException, BrokerException
    {
        boolean route= target.queueMessage(newRef, false);
        if (route) {
            // we have space ... move the message in a single command
            Set s = target.routeAndMoveMessage(oldRef, newRef);
            if (s != null) {
                target.forwardMessage(s, newRef);
            }
        }
    }
    public abstract Set routeAndMoveMessage(PacketReference oldRef, 
                  PacketReference newRef)
	throws IOException, BrokerException;
    

    public void setReconnectInterval(long val) 
    {
        clientReconnectInterval = val*reconnectMultiplier;
    }

    public void clientReconnect() {
        synchronized(this) {
            if (reconnectReaper != null) {
                reconnectReaper.cancel();
                reconnectReaper = null;
             }
        }
    }

    private void updateProducerBatch(boolean notifyProducers)
    {
        int oldsize = producerMsgBatchSize;
        long oldbytes = producerMsgBatchBytes;

        notifyProducers = notifyProducers && 
                    limit == DestLimitBehavior.FLOW_CONTROL;
    
        if (limit == DestLimitBehavior.FLOW_CONTROL) {
            producerMsgBatchSize = calcProducerBatchCnt(
                                 destMessages.capacity(),
                                 maxProducerLimit);
            producerMsgBatchBytes = calcProducerBatchBytes(
                                 destMessages.byteCapacity(),
                                 maxProducerLimit);
        } else {
            producerMsgBatchSize = MAX_PRODUCER_BATCH;
            producerMsgBatchBytes =  -1;
        }
        if (notifyProducers && (oldsize != producerMsgBatchSize ||
            oldbytes != producerMsgBatchBytes) ) {
            producerFlow.updateAllProducers(DEST_UPDATE, "update batch");
        }
    }



    



    private static int calcProducerBatchCnt(int destSize,
                 int producers)
    {

        if (destSize == -1) return MAX_PRODUCER_BATCH;

        int p = producers;
        if (p <= 0) {
            p = DEFAULT_MAX_PRODUCERS;
        }

        int val = destSize/p;

        if (val <= 0) val = 1;
            
        if (val > MAX_PRODUCER_BATCH)
            return MAX_PRODUCER_BATCH;

        return val;
    }

    private static long calcProducerBatchBytes(long destSize,
        int producers)
    {
        if (destSize == -1) return -1;

        int p = producers;
        if (p <= 0) {
            p = DEFAULT_MAX_PRODUCERS;
        }

        long val = destSize/p;

        if (val <= 0) val = 1;
            
        if (MAX_PRODUCER_BYTES_BATCH != -1 && val > MAX_PRODUCER_BYTES_BATCH)
            return MAX_PRODUCER_BYTES_BATCH;

        return val;
    }

    class RemoveBehaviorListener implements
             com.sun.messaging.jmq.util.lists.EventListener
        {

            Set orderedSet = null;
            Reason r = null;
            public RemoveBehaviorListener(Set orderedSet, Reason r) {
                this.orderedSet = orderedSet;
                this.r = r;
            }
 
            public void eventOccured(EventType type,  Reason reason,
                Object target, Object orig_value, Object cur_value, 
                Object userdata) 
            {
                assert type == EventType.SET_CHANGED_REQUEST;
                boolean full = destMessages.isFull();
                if (full && cur_value != null) {
                    long tbytes = ((Sized)cur_value).byteSize();
                    while (true) {
                        Iterator itr = null;
                        synchronized (orderedSet) {
                            itr = new LinkedHashSet(orderedSet).iterator();
                        }
                        if (!itr.hasNext()) {
                            break;
                        }
                        Object n = itr.next();
                        if (n == null) {
                            continue;
                        }
                        try {
                            Destination.this.removeMessage(
                                 ((PacketReference)n).getSysMessageID(), 
                                 r);
                        } catch (Exception ex) {
                            logger.logStack(Logger.INFO,
                                BrokerResources.E_INTERNAL_ERROR, ex);
                            itr.remove();
                            continue;
                        }
                        if ((destMessages.capacity()== -1 || 
                                     destMessages.freeSpace() > 0 ) && 
                            (destMessages.byteCapacity() == -1 ||  
                                     destMessages.freeBytes() > tbytes)) {
                            break;
                        }
                    }
                }
            }
        };

    class FlowListener implements com.sun.messaging.jmq.util.lists.EventListener
        {
            public void eventOccured(EventType type,  Reason reason,
                Object target, Object orig_value, Object cur_value, 
                Object userdata) 
            {

                if (reason instanceof RemoveReason)
                    return;
                assert type == EventType.FULL;
                if (reason != AddReason.LOADED) {
                    assert cur_value instanceof Boolean;
                    boolean shouldStop = destMessages.isFull();
                    if (shouldStop) {
                        logger.log(Logger.DEBUG, "Destination " 
                             + Destination.this + " is full, "
                             + " all producers should be stopped");

                         // for misbehaving producers, we may want to
                         // force a stop
                         producerFlow.updateAllProducers(DEST_PAUSE, 
                             "Destination Full");
                    } else {

                        logger.log(Logger.DEBUG, "Destination " 
                             + Destination.this + " is not full, "
                             + " some producers should be stopped");

                         producerFlow.checkResumeFlow(null, true);

                    }
                }

            }
        };

    transient Object behaviorListener = null;


    private boolean sendClusterUpdate()
    {
        return !isInternal() && !isAdmin();
    }


    protected void handleLimitBehavior(int limit) {
        if (limit == DestLimitBehavior.FLOW_CONTROL) {
            destMessages.enforceLimits(false);
            FlowListener rl = new FlowListener();
            if (behaviorListener != null) {
                 destMessages.removeEventListener(behaviorListener);
                 behaviorListener = null;
            }
            behaviorListener = destMessages.addEventListener(rl,  EventType.FULL,
                   null);
            producerFlow.updateAllProducers(DEST_BEHAVIOR_CHANGE, "behavior change");
        } else if (limit == DestLimitBehavior.REMOVE_OLDEST) {
            Set s = destMessages.subSet(new OldestComparator());
            RemoveBehaviorListener rl = new RemoveBehaviorListener(s,
                          RemoveReason.REMOVED_OLDEST);
            if (behaviorListener != null) {
                 destMessages.removeEventListener(behaviorListener);
                 behaviorListener = null;
            }
            behaviorListener = destMessages.addEventListener(rl,  EventType.SET_CHANGED_REQUEST,
                   null);
            destMessages.enforceLimits(false);
        } else if (limit == DestLimitBehavior.REJECT_NEWEST) {
            destMessages.enforceLimits(true);
            if (behaviorListener != null) {
                 destMessages.removeEventListener(behaviorListener);
                 behaviorListener = null;
            }
        } else if (limit == DestLimitBehavior.REMOVE_LOW_PRIORITY) {
            destMessages.enforceLimits(false);
            Set s = destMessages.subSet(new LowPriorityComparator());
            RemoveBehaviorListener rl = new RemoveBehaviorListener(s,
                    RemoveReason.REMOVED_LOW_PRIORITY);
            if (behaviorListener != null) {
                 destMessages.removeEventListener(behaviorListener);
                 behaviorListener = null;
            }
            behaviorListener = destMessages.addEventListener(rl,  EventType.SET_CHANGED_REQUEST,
                   null);
        }
    }

    static class DestReaperTask extends TimerTask
    {
        DestinationUID uid = null;
        private boolean canceled = false;
        Logger logger = Globals.getLogger();

        public  DestReaperTask(DestinationUID uid) {
            this.uid = uid;
        }
        public synchronized boolean cancel() {
            canceled = true;
            return super.cancel();
        }
        public void run() {
            synchronized(this) {
                if (!canceled) {
                    canceled = true;
                } else {
                    return;
                }
            }

            try {
                Destination d = Destination.getDestination(uid);
                if (d == null) return;

                // Re-verify that the destination can be removed
                synchronized(d) {
                    if (!d.shouldDestroy()) {
                        return;
                    }
                }

                synchronized (d.destinationList) {
                    if (d.getRefCount() > 0) {
                        return;
                    }
                    int level = (DestType.isAdmin(d.getType()) 
                           ? Logger.DEBUG : Logger.INFO);
                    logger.log(level,
                         BrokerResources.I_AUTO_DESTROY, 
                          uid.getLocalizedName(), String.valueOf(AUTOCREATE_EXPIRE/1000));
                    d.destvalid = false;
                    d =Destination.removeDestination(uid, false,
                          Globals.getBrokerResources().getString(
                              BrokerResources.M_AUTO_REAPED));
                }
            } catch (Exception ex) {
                logger.logStack(Logger.WARNING,
                           BrokerResources.X_REMOVE_DESTINATION_FAILED,
                           uid.getLocalizedName(), ex);
            }
        }
    }

    static class ReconnectReaperTask extends TimerTask
    {
        DestinationUID uid = null;
        private boolean canceled = false;
        private long time = 0;

        public  ReconnectReaperTask(DestinationUID uid, long time) {
            this.uid = uid;
            this.time = time;
        }
        public synchronized boolean cancel() {
            canceled = true;
            return super.cancel();
        }
        public void run() {
            synchronized(this) {
                Globals.getLogger().log(Logger.DEBUG,"Destroying temp destination "
                      + uid + " inactive for " + (time/1000)
                      + " seconds");
                if (!canceled) {
                    try {
                        Destination.removeDestination(uid, false,
                             Globals.getBrokerResources().getString(
                               BrokerResources.M_RECONNECT_TIMEOUT));
                    } catch (Exception ex) {
                        if (BrokerStateHandler.shuttingDown) {
                            Globals.getLogger().log(Logger.INFO,
                               BrokerResources.X_REMOVE_DESTINATION_FAILED,
                               uid.getLocalizedName(), ex);
                        } else {
                            Globals.getLogger().logStack(Logger.WARNING,
                               BrokerResources.X_REMOVE_DESTINATION_FAILED,
                               uid.getLocalizedName(), ex);
                        }
                    }
                }
            }
        }
    }

    transient MsgExpirationReaper expireReaper = new MsgExpirationReaper();

    class MsgExpirationReaper
    {
        SortedSet messages = null;
        TimerTask mytimer = null;

        public  MsgExpirationReaper() {
            messages = new TreeSet(ExpirationInfo.getComparator());
        }

        public synchronized void addExpiringMessage(ExpirationInfo ei) {
            messages.add(ei);
            if (mytimer == null) {
                addTimer();
            }
        }

        public synchronized void removeMessage(ExpirationInfo ei) {
            boolean rem  = messages.remove(ei);
            //assert rem;
            if (rem && messages.isEmpty()) {
                removeTimer();
            }
        }

        public synchronized void destroy() {
            if (mytimer != null) {
                removeTimer();
            }
            messages.clear();            
        }
 
        void addTimer()
        {
            assert Thread.holdsLock(this);
            assert mytimer == null;
            mytimer = new MyExpireTimerTask();
            try {
                timer.schedule(mytimer,MESSAGE_EXPIRE,MESSAGE_EXPIRE);
            } catch (IllegalStateException ex) {
                logger.log(Logger.INFO, 
                    BrokerResources.E_INTERNAL_BROKER_ERROR,
                    "Canceling message expiration on " + this, ex);
            }
        }

        void removeTimer()
        {
            assert Thread.holdsLock(this);
            try {
                if (mytimer != null)
                    mytimer.cancel();
            } catch (IllegalStateException ex) {
                logger.logStack(Logger.DEBUG,"timer canceled ", ex);
            }
            mytimer = null;
        }

        class MyExpireTimerTask extends TimerTask
        {
            public void run() {
                long currentTime = System.currentTimeMillis();
                int removedCount = 0;
                int indeliveryCount = 0;

                LinkedHashSet removed = new LinkedHashSet();
                DestinationUID duid = Destination.this.uid;
                synchronized(MsgExpirationReaper.this) {
                    Iterator itr = messages.iterator();
                    while (itr.hasNext()) {
                        ExpirationInfo ei = (ExpirationInfo)itr.next();
                        if (ei.getExpireTime() > currentTime) {
                            break;
                        }
                        removed.add(ei);
                   }
                }

                // we dont want to do this inside the loop because
                // removeExpiredMessage can generate a callback which
                // can generate a deadlock .. bummer
                Iterator itr = removed.iterator();
                while (itr.hasNext()) {
                    ExpirationInfo ei = (ExpirationInfo)itr.next();
                    try {
                        ei.incrementReapCount();
                        RemoveMessageReturnInfo ret = Destination.removeExpiredMessage(duid, ei.id);
                        if (ret.removed) {
                           removeMessage(ei);
                           removedCount++;
                        } else if (ret.indelivery) {
                            indeliveryCount++;
                        } else if (ei.getReapCount() > 1) {
                           removeMessage(ei);
                           removedCount++;
                        }
                    } catch (Exception ex) {
                        logger.logStack(Logger.WARNING, ex.getMessage(), ex);
                    }
                    
                }
                if (removedCount > 0) {
                    logger.log(Logger.INFO, BrokerResources.I_REMOVE_DSTEXP_MSGS,
                               String.valueOf(removedCount), duid.getLocalizedName());
                }
                if (indeliveryCount > 0) {
                    logger.log(Logger.INFO, BrokerResources.I_NUM_MSGS_INDELIVERY_NOT_EXPIRED_FROM_DEST,
                               String.valueOf(indeliveryCount), duid.getLocalizedName());
                }
                removed.clear();
            }
        }
    }


    class DestFilter implements Filter
     {
        public boolean matches(Object o) {
            return uid.equals(((PacketReference)o).getDestinationUID());
        }
        public boolean equals(Object o) {
             return super.equals(o);
        }
        public int hashCode() {
             return super.hashCode();
        }
     }

    protected transient Filter filter = new DestFilter();
    protected transient DestMetricsCounters dmc = new DestMetricsCounters();

    private synchronized void initialize() {
        try {
          if (stored) {
              int oldsize = size;
              long oldbytes = bytes;
              HashMap data = Globals.getStore().getMessageStorageInfo(this);
              size = ((Integer)data.get(DestMetricsCounters.CURRENT_MESSAGES)).intValue();
              bytes = ((Long)data.get(DestMetricsCounters.CURRENT_MESSAGE_BYTES)).longValue();
              size += remoteSize;
              bytes += remoteBytes;
              if (!isAdmin() && (getIsDMQ() || !isInternal())) {
                  synchronized(this.getClass()) {
                      totalcnt = (totalcnt - oldsize) + size;
                      totalbytes = (totalbytes - oldbytes) + bytes; 
                  }
              }
          }
        } catch (Exception ex) {
            logger.logStack(Logger.INFO,
            br.getKString(br.E_GET_MSG_METRICS_FROM_STORE_FAIL, this), ex);
        }
        dest_inited = true;
    }

    // used during upgrade
    public void initializeOldDestination() {
        overridePersistence(true);
        stored = true;
        dest_inited = false;
        loaded = false;
    }

    public boolean getIsDMQ() {
        return isDMQ;
    }


    /**
     * handles transient data when class is deserialized
     */
    private void readObject(java.io.ObjectInputStream ois)
        throws IOException, ClassNotFoundException
    {
        ois.defaultReadObject();
        logger = Globals.getLogger();
        br = Globals.getBrokerResources();
        currentChangeRecordInfo = Collections.synchronizedMap(
                           new HashMap<Integer, ChangeRecordInfo>());
        producerFlow = new ProducerFlow();
        isDMQ = DestType.isDMQ(type);
        if (!isDMQ)
            expireReaper = new MsgExpirationReaper();
        dest_inited = false;
        loaded = false;
        destvalid = true;
        destMessages = new SimpleNFLHashMap();
        destMessagesInRemoving = new HashMap();
        _removeMessageLock = new Object();
        consumers = new SimpleNFLHashMap();
        producers = new SimpleNFLHashMap();
        if (maxConsumerLimit > UNLIMITED)
            consumers.setCapacity(maxConsumerLimit);
        if (maxProducerLimit > UNLIMITED)
            producers.setCapacity(maxProducerLimit);
        filter = new DestFilter() ;
        unloadfilter = new UnloadFilter();
        dmc = new DestMetricsCounters();
        stored = true;
        setMaxPrefetch(maxPrefetch);

        // when loading a stored destination, we must 
        // set the behavior first OR we will not be notified
        // that the destination is full IF it already exceeds
        // its limits

        handleLimitBehavior(limit);
        if (memoryLimit != null)
            setByteCapacity(memoryLimit);
        if (countLimit > 0)
            setCapacity(countLimit);
        if (msgSizeLimit != null)
            setMaxByteSize(msgSizeLimit);

        updateProducerBatch(false);
        if (clientReconnectInterval > 0) {
            synchronized(this) {
                if (clientReconnectInterval > 0) {
                    reconnectReaper = new ReconnectReaperTask(
                        getDestinationUID(), clientReconnectInterval);
                    try {
                    timer.schedule(reconnectReaper, clientReconnectInterval);
                    } catch (IllegalStateException ex) {
                        logger.log(Logger.INFO,
                          BrokerResources.E_INTERNAL_BROKER_ERROR,
                                "Can not reschedule task, timer has "
                              + "been canceled, the broker is probably "
                              + "shutting down", ex);
                    }
                 }
            }
        }
        logger.log(Logger.DEBUG,"Loading Stored destination " + this + " connectionUID=" + id);
    }

    protected void initMonitor()
        throws IOException
    {
        if (DestType.isInternal(type)) {
            if (!DestType.destNameIsInternalLogging(getDestinationName())) {
                if (!CAN_MONITOR_DEST) {
                    throw new IOException(
                                Globals.getBrokerResources().getKString(
                                BrokerResources.X_FEATURE_UNAVAILABLE,
                                   Globals.getBrokerResources().getKString(
                                          BrokerResources.M_MONITORING), getName()));
                }
                try {
                    bm = new BrokerMonitor(this);
                } catch (IllegalArgumentException ex) {
                    logger.log(Logger.INFO, 
                       BrokerResources.E_INTERNAL_BROKER_ERROR,
                        "Unknown Monitor destination " 
                             + getDestinationName(), ex);
                } catch (BrokerException ex) {
                    logger.log(Logger.INFO, 
                       BrokerResources.E_INTERNAL_BROKER_ERROR,
                        "Monitor destination Error  " 
                             + getDestinationName(), ex);
                }
            }
        }
    }

    protected void initVar() {
    }

    protected Destination(String destination, int type, 
            boolean store, ConnectionUID id, boolean autocreate) 
        throws FeatureUnavailableException, BrokerException,
               IOException
    {

        this.uid = new DestinationUID( 
                   destination,DestType.isQueue(type));
        initVar();

        if (this.uid.isWildcard()) {
             throw new RuntimeException("Do not create wildcards");
        }
        this.id = id;
        producers.setCapacity(maxProducerLimit);
        consumers.setCapacity(maxConsumerLimit);
        destMessages = new SimpleNFLHashMap();
        destMessagesInRemoving = new HashMap();
        _removeMessageLock = new Object();
        destMessages.enforceLimits(true);
        if (autocreate) {
            if (!DestType.isAdmin(type)) {
                if (defaultMaxMsgCnt > 0)
                    setCapacity(defaultMaxMsgCnt);
                setByteCapacity(defaultMaxMsgBytes);
                setMaxByteSize(defaultMaxBytesPerMsg);
                setLimitBehavior(defaultLimitBehavior);
                if (defaultIsLocal)
                    setScope(DestScope.LOCAL);
                }
            if (!DestType.isAdmin(type) && !canAutoCreate(DestType.isQueue(type),type) && !BrokerMonitor.isInternal(destination)) {
                throw new BrokerException(
                        Globals.getBrokerResources().getKString(
                        BrokerResources.W_DST_NO_AUTOCREATE,
                         getName()),
                        BrokerResources.W_DST_NO_AUTOCREATE,
                        (Throwable) null,
                        Status.FORBIDDEN);
            } else  {
                int level = (DestType.isAdmin(type) ? Logger.DEBUG :
                     Logger.INFO);
                logger.log(level,
                       BrokerResources.I_AUTO_CREATE, getName());
            }
            this.type = (type | DestType.DEST_AUTO);
        } else {
            int level = (DestType.isAdmin(type) ? Logger.DEBUG :
                     Logger.INFO);
            this.type = type;
            if ((type & DestType.DEST_TEMP) == DestType.DEST_TEMP)
                logger.log(level,
                       BrokerResources.I_DST_TEMP_CREATE,
                         (id == null ? "<none>" : id.toString()),
                       getName());
            else
            logger.log(level,
                  BrokerResources.I_DST_ADMIN_CREATE, getName());
        }
        if ((type & DestType.DEST_DMQ) == 0 && BrokerMonitor.isInternal(destination)) {
            if (DestType.isQueue(type)) {
                throw new BrokerException("Internal Exception: "
                        + "Only topics are supported for monitoring");
            }
            this.type = (type | DestType.DEST_INTERNAL);
            setScope(scope);
            try {
                if (!DestType.destNameIsInternalLogging(getDestinationName())) {
                    if (!CAN_MONITOR_DEST) {
                        throw new BrokerException(
                            br.getKString(
                                BrokerResources.X_FEATURE_UNAVAILABLE,
                                   Globals.getBrokerResources().getKString(
                                          BrokerResources.M_MONITORING), getName()),
                                BrokerResources.X_FEATURE_UNAVAILABLE,
                                (Throwable) null,
                                Status.FORBIDDEN);
                       
                    }
                    bm = new BrokerMonitor(this);
                }
            } catch (IllegalArgumentException ex) {
                throw new BrokerException(
                    br.getKString(
                        BrokerResources.W_UNKNOWN_MONITOR,
                         getName()),
                        BrokerResources.W_UNKNOWN_MONITOR,
                        (Throwable) ex,
                        Status.BAD_REQUEST);
            }
        }
        
        loaded = true;
        if (!store) {
            neverStore = true;
            overridePersistence(false);
        }

        // NOW ATTACH ANY WILDCARD PRODUCERS OR CONSUMERS
        Iterator itr = Consumer.getWildcardConsumers();
        while (itr.hasNext()) {
            ConsumerUID cuid = (ConsumerUID) itr.next();
            Consumer c = Consumer.getConsumer(cuid);
            if (c == null){
                logger.log(Logger.INFO,"Consumer [" + cuid + "] for destination [" + this.getName() + "] already destroyed.");
                continue;
            }
            DestinationUID wuid = c.getDestinationUID();
            // compare the uids
            if (DestinationUID.match(getDestinationUID(), wuid)) {
                try {
                // attach the consumer
                    if (c.getSubscription() != null) {
                        addConsumer(c.getSubscription(), false /* XXX- TBD*/);
                    } else {
                        // if this destination was just added we may do
                        // this twice but thats OK because we are just
                        // adding to a hashset

                        c.attachToDestination(this);
                    }


                } catch (SelectorFormatException ex) {
                   //LKS TBD
                }
            }
        }
        handleLimitBehavior(limit);
        updateProducerBatch(false);
        state = DestState.RUNNING;
    }

    public boolean isLoaded() {
        return loaded;
    }

    public DestinationUID getDestinationUID() {
        return uid;
    }

    public void pauseDestination(int type) {
        assert type != DestState.UNKNOWN;
        assert type != DestState.RUNNING;
        assert type <= DestState.PAUSED;
        assert state == DestState.RUNNING;
	int oldstate = state;
	boolean pauseCon = false, pauseProd = false;
	boolean resumeCon = false, resumeProd = false;

	/*
	 * If requested state matches existing, return right away
	 */
	if (oldstate == type)  {
	    return;
	}

	if (oldstate == DestState.RUNNING)  {
            if (type == DestState.PRODUCERS_PAUSED || 
                type == DestState.PAUSED) {
		/*
		 * Old state = RUNNING, new state = PRODUCERS_PAUSED or PAUSED
		 *  - pause producers
		 */
		pauseProd = true;
	    }
            if (type == DestState.CONSUMERS_PAUSED ||
                type == DestState.PAUSED) {
		/*
		 * Old state = RUNNING, new state = CONSUMERS_PAUSED or PAUSED
		 *  - pause consumers
		 */
		pauseCon = true;
	    }
	}  else if (oldstate == DestState.PAUSED)  {
	    if (type == DestState.CONSUMERS_PAUSED)  {
		/*
		 * Old state = PAUSED, new state = CONSUMERS_PAUSED
		 *  - resume producers
		 */
		resumeProd = true;
	    } else if (type == DestState.PRODUCERS_PAUSED)  {
		/*
		 * Old state = PAUSED, new state = PRODUCERS_PAUSED
		 *  - resume consumers
		 */
		resumeCon = true;
	    }
	} else if (oldstate == DestState.CONSUMERS_PAUSED)  {
	    if (type == DestState.PAUSED)  {
		/*
		 * Old state = CONSUMERS_PAUSED, new state = PAUSED
		 *  - pause producers
		 */
		pauseProd = true;
	    } else if (type == DestState.PRODUCERS_PAUSED)  {
		/*
		 * Old state = CONSUMERS_PAUSED, new state = PRODUCERS_PAUSED
		 *  - resume consumers
		 *  - pause producers
		 */
		resumeCon = true;
		pauseProd = true;
	    }
	} else if (oldstate == DestState.PRODUCERS_PAUSED)  {
	    if (type == DestState.PAUSED)  {
		/*
		 * Old state = PRODUCERS_PAUSED, new state = PAUSED
		 *  - pause consumers
		 */
		pauseCon = true;
	    } else if (type == DestState.CONSUMERS_PAUSED)  {
		/*
		 * Old state = PRODUCERS_PAUSED, new state = CONSUMERS_PAUSED
		 *  - pause consumers
		 *  - resume producers
		 */
		pauseCon = true;
		resumeProd = true;
	    }
	}

        state = type;

	if (resumeProd)  {
             producerFlow.updateAllProducers(DEST_RESUME, "Destination is resumed");
	}
	if (resumeCon)  {
           synchronized(consumers) {
               Iterator itr = consumers.values().iterator();
               while (itr.hasNext()) {
                   Consumer c = (Consumer)itr.next();
                   c.resume("Destination.RESUME");
               }
           }
	}

        if (pauseProd) {
             producerFlow.updateAllProducers(DEST_PAUSE, "Destination is paused");
        }
        if (pauseCon) {
           synchronized(consumers) {
               Iterator itr = consumers.values().iterator();
               while (itr.hasNext()) {
                   Object o = itr.next();
                   Consumer c = (Consumer)o;
                   c.pause("Destination PAUSE");
               }
           }
        }

	Agent agent = Globals.getAgent();
	if (agent != null)  {
	    agent.notifyDestinationPause(this, type);
	}
    }

    public boolean isPaused() {
        return (state > DestState.RUNNING &&
               state <= DestState.PAUSED);
    }

    public void resumeDestination() {
        assert (state > DestState.RUNNING &&
               state <= DestState.PAUSED);
        int oldstate = state;
        state = DestState.RUNNING;
        if (oldstate == DestState.PRODUCERS_PAUSED ||
            oldstate == DestState.PAUSED) {
             producerFlow.updateAllProducers(DEST_RESUME, "Destination is resumed");
        }
        if (oldstate == DestState.CONSUMERS_PAUSED ||
            oldstate == DestState.PAUSED) {
           synchronized(consumers) {
               Iterator itr = consumers.values().iterator();
               while (itr.hasNext()) {
                   Consumer c = (Consumer)itr.next();
                   c.resume("Destination.RESUME");
               }
           }
        }

	Agent agent = Globals.getAgent();
	if (agent != null)  {
	    agent.notifyDestinationResume(this);
	}
        
    }

    /**
     * Compact the message file.
     */
    public void compact() throws BrokerException {
	Globals.getStore().compactDestination(this);
	Agent agent = Globals.getAgent();
	if (agent != null)  {
	    agent.notifyDestinationCompact(this);
	}
    }

    transient long lastMetricsTime;
    transient int msgsIn = 0;
    transient int msgsOut = 0;
    transient int lastMsgsIn = 0;
    transient int lastMsgsOut = 0;
    transient long msgBytesIn = 0;
    transient long msgBytesOut = 0;
    transient long lastMsgBytesIn = 0;
    transient long lastMsgBytesOut = 0;

    transient int msgsInInternal = 0;
    transient int msgsOutInternal = 0;
    transient long msgsInOutLastResetTime = 0;


    public static void resetAllMetrics() {
         Iterator itr = getAllDestinations();
         while (itr.hasNext()) {
             Destination d = (Destination)itr.next();
             d.resetMetrics();
         }
    }
    public void resetMetrics() {
        synchronized(dmc) {
            expiredCnt = 0;
            purgedCnt = 0;
            ackedCnt = 0;
            discardedCnt = 0;
            overflowCnt = 0;
            errorCnt = 0;
            msgsIn = 0;
            msgsOut = 0;
            lastMsgsIn = 0;
            lastMsgsOut = 0;
            msgBytesIn = 0;
            msgBytesOut = 0;
            lastMsgBytesIn = 0;
            lastMsgBytesOut = 0;
            destMessages.reset();
            consumers.reset();
        }
    }
    public DestMetricsCounters getMetrics() {
        synchronized(dmc) {

            long currentTime = System.currentTimeMillis();
            long timesec = (currentTime - lastMetricsTime)/
                      1000;

    
	    // time metrics was calculated
	    dmc.timeStamp = currentTime;

            // total messages sent to the destination
            dmc.setMessagesIn(msgsIn);
    
            // total messages sent from the destination
            dmc.setMessagesOut(msgsOut);
    
            // largest size of destination since broker started
            // retrieved from destination
            dmc.setHighWaterMessages((int)destMessages.highWaterCount());
    
            // largest bytes of destination since broker started
            // retrieved from destination
            dmc.setHighWaterMessageBytes(destMessages.highWaterBytes());
    
            // largest message size
            dmc.setHighWaterLargestMsgBytes(
                destMessages.highWaterLargestMessageBytes());
    
            // current # of active consumers 
            dmc.setActiveConsumers(consumers.size());
            dmc.setNumConsumers(consumers.size());
    
            // current # of failover consumers 
            // only applies to queues
            dmc.setFailoverConsumers((int)0);
    
            // max # of active consumers
            dmc.setHWActiveConsumers(consumers.highWaterCount());
            dmc.setHWNumConsumers(consumers.highWaterCount());
    
            // max # of failover consumers
            dmc.setHWFailoverConsumers((int)0);
    
            // avg active consumer
            dmc.setAvgActiveConsumers((int)consumers.averageCount());
            dmc.setAvgNumConsumers((int)consumers.averageCount());
    
            // avg failover consumer
            dmc.setAvgFailoverConsumers((int)(int)0);
    
            // total messages bytes sent to the destination
            dmc.setMessageBytesIn(msgBytesIn);
    
            // total messages bytes sent from the destination
            dmc.setMessageBytesOut(msgBytesOut);
    
            // current size of the destination
            dmc.setCurrentMessages(destMessages.size());
    
            // current size (in bytes) of the destination
            dmc.setCurrentMessageBytes(destMessages.byteSize());
    
            // avg size of the destination
            dmc.setAverageMessages((int)destMessages.averageCount());
    
            // avg size (in bytes) of the destination
            dmc.setAverageMessageBytes((long)destMessages.averageBytes());

	    // get disk usage info
            if (isStored()) {

	        try {
		    if (Globals.getStore().getStoreType().equals(
			Store.FILE_STORE_TYPE)) {

			HashMap map = Globals.getStore().getStorageInfo(this);
			Object obj = null;
			if ((obj = map.get(dmc.DISK_RESERVED)) != null) {
		            dmc.setDiskReserved(((Long)obj).longValue());
			}
			if ((obj = map.get(dmc.DISK_USED)) != null) {
		            dmc.setDiskUsed(((Long)obj).longValue());
			}
			if ((obj = map.get(dmc.DISK_UTILIZATION_RATIO)) != null)
			{
		            dmc.setUtilizationRatio(((Integer)obj).intValue());
			}
		    }
	        } catch (BrokerException e) {
		    logger.log(Logger.ERROR, e.getMessage(), e);
	        }
            }

            dmc.setExpiredMsgCnt(expiredCnt);
            dmc.setPurgedMsgCnt(purgedCnt);
            dmc.setAckedMsgCnt(ackedCnt);
            dmc.setDiscardedMsgCnt(discardedCnt);
            dmc.setRejectedMsgCnt(overflowCnt + errorCnt);
            dmc.setRollbackMsgCnt(rollbackCnt);

            lastMetricsTime = currentTime;
            lastMsgsIn = msgsIn;
            lastMsgsOut = msgsOut;
            lastMsgBytesIn = msgBytesIn;
            lastMsgBytesOut = msgBytesOut;
    
            return dmc;        

        }
    }

    //return 0, yes; 1 no previous sampling, else no
    public int checkIfMsgsInRateGTOutRate(long[] holder, boolean sampleOnly) {
        if (sampleOnly) {
            synchronized(this) {
                holder[0] = msgsInInternal;
                holder[1] = msgsOutInternal;
            }
            holder[2] = System.currentTimeMillis();
            holder[3] = -1;
            holder[4] = -1;
            holder[5] = -1;
            return 1;
        }
        long myins = holder[0];
        long myouts = holder[1];
        long mylastTimeStamp = holder[2];
        long myinr = holder[3];
        long myoutr = holder[4];
        long currtime = System.currentTimeMillis();
        if ((currtime - mylastTimeStamp) < 1000L) {
            if (myinr < 0 || myoutr < 0) {
                return 1; 
            }
            return (myinr > myoutr ? 0:2);
        }
        long myoutt = holder[5];

        holder[2] = currtime;
        synchronized(this) {
            holder[0] = msgsInInternal;
            holder[1] = msgsOutInternal;
        }
        if (msgsInOutLastResetTime >= mylastTimeStamp) {
            return 1;
        }
        long mt = holder[2] - mylastTimeStamp;
        long st = mt/1000L;
        if (st <= 0) {
            return 1;
        }
        long outdiff = holder[1] - myouts; 
        holder[3] = (holder[0] - myins)/st;
        holder[4] = outdiff/st;
        if (outdiff > 0) {
            holder[5] = mt/outdiff;
        }
        if (holder[3] < 0 || holder[4] < 0) {
            return 1;
        }
        return ((holder[3] > holder[4]) ? 0:2);
    }


    protected void decrementDestinationSize(PacketReference ref) {
        long objsize = ref.byteSize();
        boolean local = ref.isLocal();
        boolean persistent = ref.isPersistent();

        synchronized (this) {
            size --;
            bytes -= objsize;
            if (!local) {
                remoteSize --;
                remoteBytes -= objsize;
            }
            if (!isAdmin() && (getIsDMQ() || !isInternal())) {
                synchronized(this.getClass()) {
                   totalbytes -= objsize;
                   totalcnt --;
                   if (!persistent && !getIsDMQ()) {
                        totalcntNonPersist --;
                   }
                }
            }
        }
    }

    protected void incrementDestinationSize(PacketReference ref) {
        long objsize = ref.byteSize();
        boolean local = ref.isLocal();
        boolean persistent = ref.isPersistent();

        synchronized(this) {
            size ++;
            bytes += objsize;
            if (!local) {
                remoteSize ++;
                remoteBytes += objsize;
            }
            if (!isAdmin() && (getIsDMQ() || !isInternal())) {
                synchronized(this.getClass()) {
                    totalbytes += objsize;
                    totalcnt ++;
                    if (!persistent && !getIsDMQ()) {
                        totalcntNonPersist ++;
                    }
                }
            }
        }
    }

    public int getState() {
        return state;
    }

    protected void setState(int state) {
        this.state = state;
    }

    public void setIsLocal(boolean isLocal) 
        throws BrokerException
    {
        int scopeval = 0;
        if (isLocal) {
            scopeval = DestScope.LOCAL;
        } else {
            scopeval = DestScope.CLUSTER;
        }
        setScope(scopeval);
    }

    public void setScope(int scope)
        throws BrokerException
    {
        if (!CAN_USE_LOCAL_DEST && scope == DestScope.LOCAL) {
            throw new BrokerException(
                br.getKString(
                    BrokerResources.X_FEATURE_UNAVAILABLE,
                    Globals.getBrokerResources().getKString(
                        BrokerResources.M_LOCAL_DEST), getName()),
                    BrokerResources.X_FEATURE_UNAVAILABLE,
                    (Throwable) null,
                    Status.FORBIDDEN);
        }
        this.scope = scope;
    }

    public int getScope() {
        return this.scope;
    }

    public boolean getIsLocal() {
        return (scope  == DestScope.LOCAL) ;
    }

    public void setLimitBehavior(int behavior) 
        throws BrokerException
    {
        if (isDMQ && behavior == DestLimitBehavior.FLOW_CONTROL) {
            throw new BrokerException(
                br.getKString(BrokerResources.X_DMQ_INVAID_BEHAVIOR));
        }

	Integer oldVal = new Integer(this.limit);

        this.limit = behavior;
        handleLimitBehavior(limit);

        notifyAttrUpdated(DestinationInfo.DEST_LIMIT, 
			oldVal, new Integer(this.limit));
    }

    public int getLimitBehavior() {
        return this.limit;
    }

    public void setClusterDeliveryPolicy(int policy) {
        throw new UnsupportedOperationException(
             " cluster delivery policy not supported for this type of destination ");
    }

    public int getClusterDeliveryPolicy() {
        return ClusterDeliveryPolicy.NA;
    }


    public boolean isStored() {
        return !neverStore || stored;
    }

    public synchronized boolean store() 
        throws BrokerException, IOException
    {
        if (neverStore || stored) return false;
        Globals.getStore().storeDestination(this, PERSIST_SYNC);
        stored = true;
        return stored;
    }

    public boolean shouldSync() {
        return PERSIST_SYNC;
    }

    public void update()  
        throws BrokerException, IOException
    {
        update(true);
    }

    public void update(boolean notify) 
        throws BrokerException, IOException
    {
        boolean should_notify =
            !getIsDMQ() && notify
            && sendClusterUpdate() && !isTemporary();

        if (should_notify) {
            Globals.getClusterBroadcast().recordUpdateDestination(this);
        }

        if (!neverStore  && stored)  {
            Globals.getStore().updateDestination(this, PERSIST_SYNC);
        }
        updateProducerBatch(true);

        if (should_notify) {
            Globals.getClusterBroadcast().updateDestination(this);
        }
            
    }

    public static final String SCOPE_PROPERTY="scope";
    public static final String MAX_CONSUMERS="max_consumers";
    public static final String MAX_PRODUCERS="max_producers";
    public static final String MAX_PREFETCH="max_prefetch";
    public static final String MAX_MESSAGES="max_messages";
    public static final String MAX_BYTES="max_bytes";
    public static final String MAX_MSG_BYTES="max_msg_bytes";
    public static final String BEHAVIOUR="behaviour";
    public static final String STATE="state";
    public static final String NAME="name";
    public static final String IS_QUEUE="queue";
    public static final String IS_INTERNAL="internal";
    public static final String IS_AUTOCREATED="autocreated";
    public static final String IS_TEMPORARY="temporary";
    public static final String IS_ADMIN="admin";
    public static final String IS_LOCAL="local";
    public static final String REAL_TYPE="type";
    public static final String USE_DMQ="useDMQ";
    public static final String VALIDATE_XML_SCHEMA_ENABLED="validateXMLSchemaEnabled";
    public static final String XML_SCHEMA_URI_LIST="XMLSchemaUriList";
    public static final String RELOAD_XML_SCHEMA_ON_FAILURE="reloadXMLSchemaOnFailure";

    /**
     * used to retrieve properties for sending to
     * remote brokers or for admin support
     */

    public HashMap getDestinationProperties()
    {  
        HashMap m = new HashMap();
        getDestinationProps(m);
        return m;
    }

    protected void getDestinationProps(Map m) {
        m.put(NAME, getDestinationName());
        m.put(IS_QUEUE,Boolean.valueOf(isQueue()));
        m.put(IS_INTERNAL,Boolean.valueOf(isInternal()));
        m.put(IS_AUTOCREATED,Boolean.valueOf(isAutoCreated()));
        m.put(IS_TEMPORARY,Boolean.valueOf(isTemporary()));
        m.put(IS_ADMIN,Boolean.valueOf(isAdmin()));
        m.put(IS_LOCAL,Boolean.valueOf(getIsLocal()));
        m.put(REAL_TYPE,new Integer(type));
        m.put(SCOPE_PROPERTY,new Integer(scope));
        m.put(MAX_CONSUMERS,new Integer(maxConsumerLimit));
        m.put(MAX_PRODUCERS,new Integer(maxProducerLimit));
        m.put(MAX_PREFETCH,new Integer(maxPrefetch));
        m.put(MAX_MESSAGES,new Integer(countLimit));
        m.put(USE_DMQ,Boolean.valueOf(useDMQ));
        if (memoryLimit != null)
            m.put(MAX_BYTES,new Long(memoryLimit.getBytes()));
        if (msgSizeLimit != null)
            m.put(MAX_MSG_BYTES,new Long(msgSizeLimit.getBytes()));
        m.put(BEHAVIOUR,new Integer(limit));
        m.put(STATE,new Integer(scope));
        m.put(VALIDATE_XML_SCHEMA_ENABLED,Boolean.valueOf(validateXMLSchemaEnabled));
        if (XMLSchemaUriList != null) {
            m.put(XML_SCHEMA_URI_LIST, XMLSchemaUriList);
        }
        m.put(RELOAD_XML_SCHEMA_ON_FAILURE, reloadXMLSchemaOnFailure);
    }

    /**
     * used to update the destination from
     * remote brokers or for admin support
     */
    public void setDestinationProperties(Map m)
        throws BrokerException
    {
        if (DEBUG)
            logger.log(Logger.DEBUG,"Setting destination properties for "
               + this +" to " + m);
        if (m.get(MAX_CONSUMERS) != null) {
           try {
               setMaxConsumers(((Integer)m.get(MAX_CONSUMERS)).intValue());
           } catch (BrokerException ex) {
               logger.log(Logger.INFO, BrokerResources.E_INTERNAL_ERROR, ex);
           }
        }
        if (m.get(MAX_PRODUCERS) != null) {
           try {
               setMaxProducers(((Integer)m.get(MAX_PRODUCERS)).intValue());
           } catch (BrokerException ex) {
               logger.log(Logger.INFO, BrokerResources.E_INTERNAL_ERROR, ex);
           }
        }
        if (m.get(MAX_PREFETCH) != null) {
            setMaxPrefetch(((Integer)m.get(MAX_PREFETCH)).intValue());
        }
        if (m.get(MAX_MESSAGES) != null) {
            setCapacity(((Integer)m.get(MAX_MESSAGES)).intValue());
        }
        if (m.get(MAX_BYTES) != null) {
            SizeString ss = new SizeString();
            ss.setBytes(((Long)m.get(MAX_BYTES)).longValue());
            setByteCapacity(ss);
        }
        if (m.get(MAX_MSG_BYTES) != null) {
            SizeString ss = new SizeString();
            ss.setBytes(((Long)m.get(MAX_MSG_BYTES)).longValue());
            setMaxByteSize(ss);
        }
        if (m.get(BEHAVIOUR) != null) {
            setLimitBehavior(((Integer)m.get(BEHAVIOUR)).intValue());
        }
        if (m.get(IS_LOCAL) != null) {
            setIsLocal(((Boolean)m.get(IS_LOCAL)).booleanValue());
        }
        if (m.get(USE_DMQ) != null) {
            setUseDMQ(((Boolean)m.get(USE_DMQ)).booleanValue());
        }
        try {
            update(false);
        } catch (Exception ex) {
            logger.log(Logger.WARNING,
                BrokerResources.E_INTERNAL_BROKER_ERROR,
                "Unable to update destination " + getName(), ex);
        }

    }

    public static Hashtable getAllDebugState() {
        Hashtable ht = new Hashtable();
        ht.put("TABLE", "All Destinations");
        ht.put("maxMsgSize", (individual_max_size == null ? "null":
                      individual_max_size.toString()));
        ht.put("maxTotalSize", (max_size == null ? "null" :
                     max_size.toString()));
        ht.put("maxCount", String.valueOf(message_max_count));
        ht.put("totalBytes", String.valueOf(totalbytes));
        ht.put("totalCnt", String.valueOf(totalcnt));
        ht.put("totalCntNonPersist", String.valueOf(totalcntNonPersist));
        ht.put("sync", String.valueOf(PERSIST_SYNC));
        ht.put("allProducerFlow", String.valueOf(!NO_PRODUCER_FLOW));
        ht.put("autoCreateTopics", String.valueOf(ALLOW_TOPIC_AUTOCREATE));
        ht.put("autoCreateQueue", String.valueOf(ALLOW_QUEUE_AUTOCREATE));
        ht.put("messageExpiration", String.valueOf(MESSAGE_EXPIRE));
        ht.put("producerBatch", String.valueOf(MAX_PRODUCER_BATCH));
        ht.put("QueueSpecific", Queue.getAllDebugState());
        ht.put("msgCnt", (packetlist == null ? "null" :
                   String.valueOf(packetlist.size())));
        Hashtable destInfo = new Hashtable();
        if (destinationList != null) {
            ArrayList dlist = null;
            synchronized (destinationList) {
                dlist = new ArrayList(destinationList.keySet());
            }
            ht.put("destinationCnt", String.valueOf(dlist.size()));
            Iterator itr = dlist.iterator();
            while (itr.hasNext()) {
                DestinationUID duid = (DestinationUID)itr.next();
                Destination d = Destination.getDestination(duid);
                if (d == null) {
                    destInfo.put(duid.getLocalizedName(),"Unknown");
                } else {
                    destInfo.put(duid.getLocalizedName(),
                    d.getDebugState());
                }
            }
        } else {
            ht.put("destinationCnt", "null");
        }
        ht.put("destinations", destInfo);
        return ht;
    }


    public Hashtable getDebugState() {
        Hashtable ht = new Hashtable();
        ht.put("TABLE", "Destination["+uid.toString()+"]");
        getDestinationProps(ht);
        ht.putAll(getMetrics());
        ht.put("Consumers", String.valueOf(consumers.size()));

        Iterator itr = consumers.values().iterator();

        List pfers = null;
        synchronized (destMessages) {
            pfers = new ArrayList(destMessages.values());
        }
        while (itr.hasNext()) {
            Consumer con = (Consumer)itr.next();
            ConsumerUID cuid = con.getConsumerUID();
            ConsumerUID sid = con.getStoredConsumerUID();

            // Format: match[delivered,unacked]
            // OK -> get all messages
            int total = pfers.size();
            int match = 0;
            int delivered = 0;
            int ackno = 0;
            for (int i=0; i < total; i ++) {
                PacketReference ref = (PacketReference)pfers.get(i);
                if (ref.matches(sid)) {
                    match ++;
                    if (ref.isAcknowledged(sid))
                        ackno ++;
                    if (ref.isDelivered(sid))
                        delivered ++;
                }
            }
            String ID = match + " of " + total 
              + "[ d=" + delivered
              + ", a=" + ackno + "]";
            
            ht.put("Consumer[" +
                    String.valueOf(cuid.longValue())+ "]" , ID);
        }

        Set s = null;
        synchronized(producers) {
            s = new HashSet(producers.keySet());
        }
        itr = s.iterator();
        Vector v = new Vector();
        while (itr.hasNext()) {
            ProducerUID cuid = (ProducerUID)itr.next();
            v.add(String.valueOf(cuid.longValue()));
        }
        ht.put("Producers", v);
        ht.put("_stored", String.valueOf(stored));
        ht.put("_neverStore", String.valueOf(neverStore));
        ht.put("_destvalid", String.valueOf(destvalid));
        ht.put("_loaded", String.valueOf(loaded));
        ht.put("_state", DestState.toString(state));
        ht.put("producerMsgBatchSize", String.valueOf(producerMsgBatchSize));
        ht.put("producerMsgBatchBytes", String.valueOf(producerMsgBatchBytes));
        if (reconnectReaper != null)
            ht.put("_reconnectReaper",reconnectReaper.toString());
        ht.put("_clientReconnectInterval", String.valueOf(clientReconnectInterval));
        ht.put("TrueType", DestType.toString(type));
        if (id != null)
            ht.put("ConnectionUID", String.valueOf(id.longValue()));
        ht.put("activeProducerCount", String.valueOf(
                producerFlow.activeProducerCnt()));
        ht.put("pausedProducerCount", String.valueOf(
                producerFlow.pausedProducerCnt()));
        ht.put("pausedProducerSet", producerFlow.getDebugPausedProducers());
        ht.put("activeProducerSet", producerFlow.getDebugActiveProducers());
        List sysids = null;
        ht.put("size", Integer.valueOf(size));
        ht.put("bytes", Long.valueOf(bytes));
        ht.put("remoteSize", Long.valueOf(remoteSize));
        ht.put("remoteBytes", Long.valueOf(remoteBytes));
        synchronized (destMessages) {         
            if (destMessages != null) {
                ht.put("destMessagesSize", String.valueOf(destMessages.size()));
                sysids = new ArrayList(destMessages.keySet());
            } 
        }
        if (sysids != null) {
            itr = sysids.iterator();
            v = new Vector();
            PacketReference ref = null;
            String refs = "null";
            while (itr.hasNext()) {
                SysMessageID sysid = (SysMessageID)itr.next();
                ref = (PacketReference)destMessages.get(sysid);
                refs = "null";
                if (ref != null) {
                    refs = "local="+ref.isLocal()+",invalid="+ref.isInvalid()+
                           ",destroyed="+ref.isDestroyed()+",overrided="+ref.isOverrided()+
                           ",overriding="+ref.isOverriding()+",locked="+(ref.checkLock(false)==null); 
                }
                v.add(sysid.toString()+"  ref="+refs);
            }
            ht.put("Messages", v);
        }
        return ht;
        
        
    }

    public Hashtable getDebugMessages(boolean full) {
        if (!loaded ) {
            try {
                load();
            } catch (Exception ex) {}
        }

        Vector vt = new Vector();
        try {
            Iterator itr = null;
            synchronized(destMessages) {
            itr = new HashSet(destMessages.values()).iterator();
            }
            while (itr.hasNext()) {
                PacketReference pr = (PacketReference)itr.next();
                Hashtable pht = pr.getDebugState();
                pht.put("ID", pr.getSysMessageID().toString());
                if (full) { 
                    pht.put("PACKET", pr.getPacket().dumpPacketString("        "));
                }
                vt.add(pht);
            }
        } catch (Throwable ex) {
            logger.log(Logger.DEBUG,"Error getting debugMessages ",
                 ex);
        }
        Hashtable ht = new Hashtable();
        ht.put("  ", vt);
        return ht;
        
    }

    public SysMessageID[] getSysMessageIDs() throws BrokerException  {
        return (getSysMessageIDs(null, null));
    }

    public SysMessageID[] getSysMessageIDs(Long startMsgIndex, Long maxMsgsRetrieved) 
					throws BrokerException  {
	SysMessageID ids[] = new SysMessageID[0];
	String errMsg;

        if (!loaded ) {
            load();
        }

	/*
	 * Check/Setup array params
	 */
	long numMsgs = destMessages.size();

	/*
	 * If destination is empty, return empty array.
	 */
	if (numMsgs == 0)  {
	    return (ids);
	}

	if (startMsgIndex == null)  {
	    startMsgIndex = new Long(0);
	} else if ((startMsgIndex.longValue() < 0) ||
	                (startMsgIndex.longValue() > (numMsgs - 1)))  {
	    errMsg = " Start message index needs to be in between 0 and "
	                + (numMsgs - 1);
            throw new BrokerException(errMsg);
	}

	if (maxMsgsRetrieved == null)  {
	    maxMsgsRetrieved = new Long(numMsgs - startMsgIndex.longValue());
	} else if (maxMsgsRetrieved.longValue() < 0)  {
            errMsg = " Max number of messages retrieved value needs to be greater than 0.";
            throw new BrokerException(errMsg);
	}

	long maxIndex = startMsgIndex.longValue() + maxMsgsRetrieved.longValue();

        SortedSet s = new TreeSet(new RefCompare());

        try {
            Iterator itr = new HashSet(destMessages.values()).iterator();
            while (itr.hasNext()) {
                PacketReference pr = (PacketReference)itr.next();
                s.add(pr);
            }
        } catch (Throwable ex) {
            logger.log(Logger.DEBUG,"Error getting msg IDs ",
                 ex);
        }

	ArrayList idsAl = new ArrayList();

	long i = 0;
        Iterator itr = s.iterator();
        while (itr.hasNext()) {
	    PacketReference pr = (PacketReference)itr.next();

	    if ( (i >= startMsgIndex.longValue()) &&
		 (i < maxIndex) )  {
                SysMessageID id = pr.getSysMessageID();
	        idsAl.add(id);
	    }

	    if (i >= maxIndex)  {
		break;
	    }
	    
	    ++i;
	}

	ids = (SysMessageID[])idsAl.toArray(ids);

	return (ids);
    }


    public String getName() {
        return uid.getLocalizedName();
    }    

    public String getDestinationName() {
        return uid.getName();
    }

    public ConnectionUID getConnectionUID() {
        return id;
    }

    public boolean isAutoCreated() {
        return ((type & DestType.DEST_AUTO) > 0);
    }

    public boolean isTemporary() {

        return ((type & DestType.DEST_TEMP) > 0);
    }

    public boolean isQueue() {
        return ((type & DestType.DEST_TYPE_QUEUE) > 0);
    }

    public int getType() {
        return type;
    }

    public Collection getAllMessages()
        throws UnsupportedOperationException
    {
        return destMessages.values();
    }


    public void purgeDestination() throws BrokerException {
        purgeDestination(false);
        
    }

    public void purgeDestination(boolean noerrnotfound) throws BrokerException
    {
       if (!loaded) {
           load(noerrnotfound);
       }

       try {
           Set s = null;
           synchronized(destMessages) {
           s = new HashSet(destMessages.keySet());
           }
           long removedCount = 0L;
           long indeliveryCount = 0L;
           RemoveMessageReturnInfo ret = null;
           SysMessageID sysid = null;
           Iterator itr = s.iterator();
           while (itr.hasNext()) {
               sysid = (SysMessageID)itr.next();
               ret = _removeMessage(sysid, RemoveReason.PURGED, null, null, true);
               if (ret.removed) {
                   removedCount++;
               } else if (ret.indelivery) {
                   indeliveryCount++;
               }
           } 
           logger.log(logger.INFO, br.getKString(br.I_NUM_MSGS_PURGED_FROM_DEST,
                                       removedCount, uid.getLocalizedName()));
           if (indeliveryCount > 0) {
           logger.log(logger.INFO, br.getKString(br.I_NUM_MSGS_INDELIVERY_NOT_PURGED_FROM_DEST,
                                       indeliveryCount, uid.getLocalizedName()));
           }

	       Agent agent = Globals.getAgent();
	       if (agent != null)  {
	           agent.notifyDestinationPurge(this);
	       }
       } catch (Exception ex) {
           if (BrokerStateHandler.shuttingDown) {
               logger.log(Logger.INFO,
                   BrokerResources.E_PURGE_DST_FAILED, getName(), ex);
           } else {
               logger.logStack(Logger.WARNING,
                   BrokerResources.E_PURGE_DST_FAILED, getName(), ex);
           }

           if (ex instanceof BrokerException) throw (BrokerException)ex;

           throw new BrokerException(br.getKString(
               BrokerResources.E_PURGE_DST_FAILED, getName()), ex);
       }
    }

    public void purgeDestination(Filter criteria) throws BrokerException {
        if (!loaded ) {
            load();
        }

        Map m = destMessages.getAll(criteria);
        Iterator itr = m.keySet().iterator();
        while (itr.hasNext()) {
            try {
                removeMessage((SysMessageID)itr.next(), RemoveReason.PURGED);
            } catch (Exception ex) {
                logger.logStack(Logger.INFO,
                    BrokerResources.E_PURGE_DST_FAILED, getName(), ex);
            }
        }
    }

    public Map getAll(Filter f) {
        if (!loaded ) {
            try {
                load();
            } catch (Exception ex) {}
        }

        return destMessages.getAll(f);
    }


    public int size()
        throws UnsupportedOperationException
    {
        if (!loaded) {
            return size;
        }
        return destMessages.size();
    }


    public long byteSize()
        throws UnsupportedOperationException
    {
        if (!loaded) {
            return bytes;
        }
        return destMessages.byteSize();
    }

    public int getRemoteSize() {
        int cnt = 0;
        Set msgs =  null;
        synchronized(destMessages) {
            msgs = new HashSet(destMessages.values());
        }
        Iterator itr = msgs.iterator();
        while (itr.hasNext()) {
            PacketReference ref = (PacketReference)itr.next();
            if (!ref.isLocal()) {
                cnt++;
            }
        }
        return cnt;
    }

    public long getRemoteBytes() {
        long rbytes = 0;
        Set msgs =  null;
        synchronized(destMessages) {
            msgs = new HashSet(destMessages.values());
        }
        Iterator itr = msgs.iterator();
        while (itr.hasNext()) {
            PacketReference ref = (PacketReference)itr.next();
            if (!ref.isLocal()) {
                rbytes += ref.getSize();
            }
        }
        return rbytes;
    }


    public int txnSize() {
        Set msgs = null;
        synchronized(destMessages) {
        msgs = new HashSet(destMessages.values());
        }
        Iterator itr = msgs.iterator();
        int cnt = 0;
        TransactionList tl = Globals.getTransactionList();
        while (itr.hasNext()) {
            PacketReference ref = (PacketReference)itr.next();
            TransactionUID tid = ref.getTransactionID();
            if (tid == null) continue;
            if (tl.retrieveState(tid) == null) continue;
            cnt ++;
        }
        return cnt;
    }

    public long txnByteSize() {
        Set msgs = null;
        synchronized(destMessages) {
        msgs = new HashSet(destMessages.values());
        }
        Iterator itr = msgs.iterator();
        long size = 0;
        TransactionList tl = Globals.getTransactionList();
        while (itr.hasNext()) {
            PacketReference ref = (PacketReference)itr.next();
            TransactionUID tid = ref.getTransactionID();
            if (tid == null) continue;
            if (tl.retrieveState(tid) == null) continue;
            size += ref.getSize();
        }
        return size;
    }

    public long checkDestinationCapacity(PacketReference ref) {
        long room = -1;
        int maxc = destMessages.capacity();
        if (maxc > 0) {
            room = maxc - destMessages.size();
            if (room < 0) {
                room = 0;
            }
        }
        long maxb = destMessages.byteCapacity();
        if (maxb > 0) {
            long cnt = (maxb - destMessages.byteSize())/ref.byteSize();
            if (cnt < 0) {
                cnt = 0;
            }
            if (cnt < room) {
                room = cnt;
            }
        }
        return room;
    }

    public float destMessagesSizePercent() {
        int maxc = destMessages.capacity();
        if (maxc <=0 ) {
            return (float)0;
        }
        return ((float)destMessages.size()/(float)maxc)*100;
    }

    public abstract int getUnackSize();

     /**
     * Maximum number of messages stored in this
     * list at any time since its creation.
     *
     * @return the highest number of messages this set
     * has held since it was created.
     */
   public long getHighWaterBytes() {
        return destMessages.highWaterBytes();
    }

    /**
     * Maximum number of bytes stored in this
     * list at any time since its creation.
     *
     * @return the largest size (in bytes) of
     *  the objects in this list since it was
     *  created.
     */
    public int getHighWaterCount() {
        return destMessages.highWaterCount();
    }

    /**
     * The largest message 
     * which has ever been stored in this destination.
     *
     * @return the number of bytes of the largest
     *  message ever stored on this destination.
     */
    public long highWaterLargestMessageBytes() {
        return destMessages.highWaterLargestMessageBytes();
    }

    /**
     * Average number of bytes stored in this
     * destination at any time since the broker started.
     *
     * @return the largest size (in bytes) of
     *  the objects in this destination since it was
     *  created.
     */
    public double getAverageBytes() {
        return destMessages.averageBytes();
    }


    /**
     * Average number of messages stored in this
     * list at any time since its creation.
     *
     * @return the average number of messages this set
     * has held since it was created.
     */
    public float getAverageCount() {
        return destMessages.averageCount();
    }

    /**
     * The average message size (which implements Sizeable)
     * of messages which has been stored in this list.
     *
     * @return the number of bytes of the average
     *  message stored on this list.
     */
    public double averageMessageBytes() {
        return destMessages.averageMessageBytes();
    }

    public SizeString getMaxByteSize()
    {
        return msgSizeLimit;
    }

    public int getCapacity()
    {
        return countLimit;
    }

    public SizeString getByteCapacity()
    {
        return memoryLimit;
    }

    public void setMaxByteSize(SizeString limit)
        throws UnsupportedOperationException
    {

        if (DEBUG) {
            logger.log(Logger.DEBUG, 
                "attempting to set Message Size Limit to " +
                limit + " for destination " + this);
        }

	Long oldVal;

	if (this.msgSizeLimit == null)  {
	    oldVal = new Long(Limitable.UNLIMITED_BYTES);
	} else  {
	    oldVal = new Long(this.msgSizeLimit.getBytes());
	}
	if (oldVal.longValue() == 0)  {
	    oldVal = new Long(Limitable.UNLIMITED_BYTES);
	}

        this.msgSizeLimit = limit;
        long bytes = 0;
        if (limit == null) {
            bytes = Limitable.UNLIMITED_BYTES;
        } else {
            bytes = limit.getBytes();
        }
        if (bytes == 0) { // backwards compatibiity
            bytes = Limitable.UNLIMITED_BYTES;
        }
        destMessages.setMaxByteSize(bytes);

        notifyAttrUpdated(DestinationInfo.MAX_MESSAGE_SIZE, 
			oldVal, new Long(bytes));
    }

    public void setCapacity(int limit)
        throws UnsupportedOperationException
    {
        if (DEBUG) {
            logger.log(Logger.DEBUG, 
                "attempting to set Message Count Limit to " +
                limit + " for destination " + this);
        }

	Long oldVal = new Long (this.countLimit);

        if (limit == 0)  { // backwards compatibility
            limit = Limitable.UNLIMITED_CAPACITY;
        }
        this.countLimit = limit;
        destMessages.setCapacity(limit);
        // make sure we update the batch size after a limit change
        updateProducerBatch(false);

        notifyAttrUpdated(DestinationInfo.MAX_MESSAGES, 
				oldVal, new Long(this.countLimit));
    }

    public void setByteCapacity(SizeString limit)
        throws UnsupportedOperationException
    {
        if (DEBUG) {
            logger.log(Logger.DEBUG, 
                "attempting to set Message Bytes Limit to " +
                limit + " for destination " + this);
        }

	Long oldVal;

	if (this.memoryLimit == null)  {
	    oldVal = new Long(Limitable.UNLIMITED_BYTES);
	} else  {
	    oldVal = new Long(this.memoryLimit.getBytes());
	}
	if (oldVal.longValue() == 0)  {
	    oldVal = new Long(Limitable.UNLIMITED_BYTES);
	}

        this.memoryLimit = limit;
        long bytes = 0;
        if (limit == null) {
            bytes = Limitable.UNLIMITED_BYTES;
        } else {
            bytes = limit.getBytes();
        }
        if (bytes == 0) { // backwards compatibiity
            bytes = Limitable.UNLIMITED_BYTES;
        }
        destMessages.setByteCapacity(bytes);
        updateProducerBatch(false);

        notifyAttrUpdated(DestinationInfo.MAX_MESSAGE_BYTES, 
				oldVal, new Long(bytes));
    }

    public int getMaxActiveConsumers() {
        return UNLIMITED;
    }

    public int getMaxFailoverConsumers() {
        return NONE;
    }

    public void setMaxProducers(int cnt) 
        throws BrokerException
    {
        if (isDMQ) {
            throw new BrokerException(
                br.getKString(BrokerResources.X_DMQ_INVAID_PRODUCER_CNT));
        }
        if (cnt == 0) {
            throw new BrokerException(
                br.getKString(
                    BrokerResources.X_BAD_MAX_PRODUCER_CNT,
                     getName()),
                    BrokerResources.X_BAD_MAX_PRODUCER_CNT,
                    (Throwable) null,
                    Status.ERROR);
        }

	Integer oldVal = new Integer(maxProducerLimit);

        maxProducerLimit = (cnt < -1 ? -1:cnt);
        producers.setCapacity(maxProducerLimit);

        notifyAttrUpdated(DestinationInfo.MAX_PRODUCERS, 
			oldVal, new Integer(maxProducerLimit));
    }

    public int getMaxProducers() {
        return maxProducerLimit;
    }

    public int getAllActiveConsumerCount() {
        int cnt = 0;
        synchronized (consumers) {
            if (consumers.size() == 0) return 0;
            Iterator itr = consumers.values().iterator();
            Consumer c = null;
            while (itr.hasNext()) {
                c = (Consumer)itr.next();
                if (c instanceof Subscription) {
                    cnt += ((Subscription)c).getChildConsumers().size();
                } else  {
                    cnt++;
                }
            }
        }
        return cnt;
    }

    public int getActiveConsumerCount() {
        return getConsumerCount();
    }

    public Set getActiveConsumers() {
       Set set = new HashSet();
       synchronized (consumers) {
           Iterator itr = consumers.values().iterator();
           while (itr.hasNext()) {
               Consumer con = (Consumer)itr.next();
               set.add(con);
           }
       }
       return set;
    }

    public Set getFailoverConsumers() {
        return new HashSet();
    }

    public int getFailoverConsumerCount() {
        return NONE;
    }

    public void setMaxConsumers(int count) 
        throws BrokerException
    {
        if (count == 0) {
            throw new BrokerException(
                br.getKString(
                    BrokerResources.X_BAD_MAX_CONSUMER_CNT,
                    getName()),
                    BrokerResources.X_BAD_MAX_CONSUMER_CNT,
                    (Throwable) null,
                    Status.ERROR);
        }
        maxConsumerLimit = (count < -1 ? -1:count);
        consumers.setCapacity(maxConsumerLimit);
    }


    public void setMaxActiveConsumers(int cnt) 
        throws BrokerException
    {


        throw new UnsupportedOperationException("setting max active consumers not supported on this destination type");
    }
    public void setMaxFailoverConsumers(int cnt) 
        throws BrokerException
    {
        throw new UnsupportedOperationException("setting max failover consumers not supported on this destination type");
    }




    public int hashCode() {
        return uid.hashCode();
    }

    public boolean equals(Object o)
    {   if (o instanceof Destination) {
            if ( uid == ((Destination)o).uid)
                return true;
            return uid.equals(((Destination)o).uid);
        }
        return false;
    }
    

    public boolean queueMessage(PacketReference pkt, boolean trans) 
        throws BrokerException {
        return queueMessage(pkt, trans, true);
    }

    public boolean queueMessage(PacketReference pkt, 
                                boolean trans, 
                                boolean enforcelimit) 
        throws BrokerException {
        if (!valid) {
            throw new BrokerException(
               br.getKString(
                    BrokerResources.I_DST_SHUTDOWN_DESTROY, getName()));
        }
        synchronized(this) {
            msgsIn +=1;
            msgBytesIn += pkt.byteSize();
            msgsInInternal +=1;
            if (msgsInInternal >= Integer.MAX_VALUE) {
                msgsInOutLastResetTime = System.currentTimeMillis(); 
                msgsInInternal = 0;
                msgsOutInternal = 0;
            }
        }
        try {
            boolean check = !isAdmin() && !isInternal();
            boolean ok = addNewMessage((check && enforcelimit), pkt);
            if (!ok && !isDMQ) {
               // expired
               // put on dead message queue
               if (!isInternal()) {
                  pkt.setDestination(this);
                  markDead(pkt, RemoveReason.EXPIRED, null);
                  removePacketList(pkt.getSysMessageID(), this.getDestinationUID());
               }
               return false;
            }
        } catch (BrokerException ex) {
            pkt.destroy();
            throw ex;
        }
        pkt.setDestination(this);
        try {
            if (!valid) {
                pkt.destroy();
                throw new BrokerException(
                   br.getKString(
                        BrokerResources.I_DST_SHUTDOWN_DESTROY, getName()));
            }
            if (overrideP) {
                pkt.overridePersistence(overridePvalue);
            }
            putMessage(pkt, AddReason.QUEUED, false, enforcelimit);
            if (overrideTTL) {
                pkt.overrideExpireTime(System.currentTimeMillis() +
                   overrideTTLvalue);
            }
            ExpirationInfo ei = pkt.getExpiration();
            if (expireReaper != null && ei != null) {
                if (!valid) {
                    RuntimeException ex = new RuntimeException("Destination "
                       + this + " destroyed");
                    ex.fillInStackTrace();
                    logger.logStack(Logger.DEBUG,"Removing message to "
                       + "invalid dst", ex);
                    removeMessage(pkt.getSysMessageID(), null);
                    throw ex;
                }
                if (expireReaper == null) {
                    RuntimeException ex = new RuntimeException("No Reaper");
                    ex.fillInStackTrace();
                    logger.logStack(Logger.INFO,"Internal Error, Unknown "
                      + " destination " + this + " isValid= " + isValid(), ex );
                    return true;
                }
                expireReaper.addExpiringMessage(ei);
            }
        } catch (IllegalStateException ex) { // message exists
            throw new BrokerException(
                br.getKString(
                    BrokerResources.X_MSG_EXISTS_IN_DEST,
                          pkt.getSysMessageID(),
                          this.toString()),
                    BrokerResources.X_MSG_EXISTS_IN_DEST,
                    (Throwable) ex,
                    Status.NOT_MODIFIED);
        } catch (OutOfLimitsException ex) {
            removeMessage(pkt.getSysMessageID(), RemoveReason.OVERFLOW);
            Object lmt = ex.getLimit();
            boolean unlimited = false; 
            if (lmt == null) {
            } else if (lmt instanceof Integer) {
                unlimited = ((Integer)ex.getLimit()).intValue() <= 0;
            } else if (lmt instanceof Long) {
                unlimited = ((Long)ex.getLimit()).longValue() <= 0;
            }
            String args[] = {pkt.getSysMessageID().toString(), 
                             getName(),
                             (unlimited ?
                                br.getString(BrokerResources.M_UNLIMITED) :
                                ex.getLimit().toString()),
				ex.getValue().toString()};
            String id = BrokerResources.X_INTERNAL_EXCEPTION;
            int status = Status.RESOURCE_FULL;
            switch (ex.getType()) {
                case OutOfLimitsException.CAPACITY_EXCEEDED:
                    id = BrokerResources.X_DEST_MSG_CAPACITY_EXCEEDED;
                    break;
                case OutOfLimitsException.BYTE_CAPACITY_EXCEEDED:
                    id = BrokerResources.X_DEST_MSG_BYTES_EXCEEDED;
                    break;
                case OutOfLimitsException.ITEM_SIZE_EXCEEDED:
                    id = BrokerResources.X_DEST_MSG_SIZE_EXCEEDED;
                    status = Status.ENTITY_TOO_LARGE;
                    break;
            }
                          
            throw new BrokerException(
                br.getKString(id, args),
                    id,
                    (Throwable) ex,
                    status);
        } catch (IllegalArgumentException ex) {
            removeMessage(pkt.getSysMessageID(), RemoveReason.ERROR);
            throw ex;
        }
        return true;
    }

    public abstract ConsumerUID[] calculateStoredInterests(PacketReference sys)
    throws BrokerException, SelectorFormatException;


    public abstract Set routeNewMessage(PacketReference sys)
         throws BrokerException, SelectorFormatException;

    /* called from transaction code */
    public abstract void forwardOrphanMessage(PacketReference sys,
                  ConsumerUID consumer)
        throws BrokerException;

    /* called from transaction code */
    public abstract void forwardOrphanMessages(Collection syss,
                  ConsumerUID consumer)
        throws BrokerException;

    public abstract void forwardMessage(Set consumers, PacketReference sys)
         throws BrokerException;

    /**
     * only called when loading a transaction
     * LKS-XXX need to rethink if there is a cleaner way
     * to manage this
     */
    protected abstract ConsumerUID[] routeLoadedTransactionMessage(
           PacketReference ref)
          throws BrokerException, SelectorFormatException;
    
    
    public abstract void unrouteLoadedTransactionAckMessage(PacketReference ref, ConsumerUID consumer)
    throws BrokerException;
    

    public void putMessage(PacketReference ref, Reason r)
        throws IndexOutOfBoundsException, 
        IllegalArgumentException, IllegalStateException {

        putMessage(ref, r, false, true);
    }

    public void putMessage(PacketReference ref, Reason r, boolean override)
        throws IndexOutOfBoundsException, 
        IllegalArgumentException, IllegalStateException {

        putMessage(ref, r, override, true);
    }

    public void putMessage(PacketReference ref, Reason r, 
                           boolean override, boolean enforcelimit) 
        throws IndexOutOfBoundsException, 
        IllegalArgumentException, IllegalStateException {

        if (!override) {
            destMessages.put(ref.getSysMessageID(), ref, r, override);
            _messageAdded(ref, r, false);
            return;
        }
        boolean overrideRemote = false;
        synchronized(destMessages) {
            PacketReference oldref = (PacketReference)destMessages.get(
                                                  ref.getSysMessageID());
            if (oldref != null && oldref != ref && !oldref.isLocal()) {
                oldref.overrided();
                ref.overriding();
                overrideRemote = true;
             }
             boolean elsave = true;
             if (!enforcelimit) {
                 elsave = destMessages.getEnforceLimits();
                 destMessages.enforceLimits(true);
             }
             destMessages.put(ref.getSysMessageID(), ref, r, override);
             if (!enforcelimit) {
                 destMessages.enforceLimits(elsave);
             }
        }
        _messageAdded(ref, r, overrideRemote);
    }

    private void unputMessage(PacketReference ref, Reason r)
        throws IndexOutOfBoundsException, IllegalArgumentException
    {
        Object o = destMessages.remove(ref.getSysMessageID(), r);
        _messageRemoved(ref, ref.byteSize(), r, (o != null));
    }


    public boolean removeRemoteMessage(SysMessageID id, 
                         Reason r, PacketReference remoteRef, boolean wait) 
                         throws BrokerException {
        RemoveMessageReturnInfo ret = _removeMessage(id, r, null, remoteRef, wait);
        return ret.removed;
    }

    public boolean removeRemoteMessage(SysMessageID id, 
                         Reason r, PacketReference remoteRef) 
                         throws BrokerException {
        RemoveMessageReturnInfo ret = _removeMessage(id, r, null, remoteRef, true);
        return ret.removed;
    }

    public boolean removeMessage(SysMessageID id, Reason r, boolean wait)
        throws BrokerException
    {
        RemoveMessageReturnInfo ret = _removeMessage(id, r, null, null, wait);
        return ret.removed;
    }

    public boolean removeMessage(SysMessageID id, Reason r)
        throws BrokerException
    {
        RemoveMessageReturnInfo ret = _removeMessage(id, r, null, null, true);
        return ret.removed;
    }

    public boolean removeMessage(SysMessageID id, Reason r,
           Hashtable dmqProps)
        throws BrokerException
    {
       RemoveMessageReturnInfo ret = _removeMessage(id, r, dmqProps, null, true);
       return ret.removed;
    }

    static class RemoveMessageReturnInfo {
        boolean removed = false;
        boolean indelivery  = false;
    }

    private RemoveMessageReturnInfo _removeMessage(SysMessageID id, Reason r,
            Hashtable dmqProps, PacketReference remoteRef, boolean wait)
            throws BrokerException
    {
        RemoveMessageReturnInfo ret = new RemoveMessageReturnInfo();

        PacketReference ref = null;

        // LKS-XXX revisit if it is really necessary to load the
        // message before removing it 
        if (!loaded ) {
            load();
        }

        // OK .. first deal w/ Lbit
        // specifically .. we cant remove it IF the Lbit
        // is set
        ref = (PacketReference)destMessages.get(id);
        if (ref == null) {
            // message already gone 
            removePacketList(id, getDestinationUID());
            logger.log(Logger.DEBUG, "Reference already gone for " + id);
            return ret;
        }

        if (remoteRef != null && remoteRef != ref) {
            logger.log(((DEBUG_CLUSTER||DEBUG) ? Logger.INFO:Logger.DEBUG), 
                       "Reference for "+id+" is overrided, not remove");
            remoteRef.setInvalid();
            return ret;
        }

        ExpirationInfo ei = ref.getExpiration();

        if (r == RemoveReason.EXPIRED || r == RemoveReason.PURGED) {
            if (!ref.checkDeliveryAndSetInRemoval()) {

                if (r == RemoveReason.EXPIRED && !EXPIRE_DELIVERED_MSG) {
                    logger.log(((DEBUG_CLUSTER||DEBUG) ? Logger.INFO:Logger.DEBUG),
                        "Message "+ref.getSysMessageID()+" is not "+r+
                        " because it has been delivered to client");
                    if (ei != null) {
                        ei.clearReapCount();
                    }
                    ret.indelivery = true;
                    return ret;
                }
                if (r == RemoveReason.PURGED && !PURGE_DELIVERED_MSG) {
                    logger.log(((DEBUG_CLUSTER||DEBUG) ? Logger.INFO:Logger.DEBUG),
                        "Message "+ref.getSysMessageID()+" is not "+r+
                        " because it has been delivered to client");
                    ret.indelivery = true;
                    return ret;
                }
            }
        }

        synchronized(ref) {
            if (ref.getLBitSet()) {
                ref.setInvalid();
                if (r == RemoveReason.EXPIRED && ei != null) {
                    ei.clearReapCount();
                }
                logger.log(Logger.DEBUG,"LBit set for " + id);
                return ret;
            }
        }

        synchronized(destMessagesInRemoving) {
            if (destMessagesInRemoving.get(id) != null && !wait) {
                logger.log(((DEBUG_CLUSTER||DEBUG) ? Logger.INFO:Logger.DEBUG),
                           "Reference "+id +" is being removed by another thread ");
                return ret;
            } 
            destMessagesInRemoving.put(id, id);
        }

        try {

            synchronized(_removeMessageLock) {
                if (destMessages.get(id) == null) {
                    logger.log(((DEBUG_CLUSTER||DEBUG) ? Logger.INFO:Logger.DEBUG),
                           "Reference has already been removed for " + id);
                    return ret;
                }

            // handle DMQ
            // OK we need to move the message TO the DMQ before removing it
            // OK .. if we arent the DMQ and we want to use the DMQ 
            if (!isInternal() &&
                (r == RemoveReason.EXPIRED ||
                 r == RemoveReason.EXPIRED_BY_CLIENT ||
                 r == RemoveReason.EXPIRED_ON_DELIVERY ||
                 r == RemoveReason.REMOVED_LOW_PRIORITY ||
                 r == RemoveReason.REMOVED_OLDEST ||
                 r == RemoveReason.ERROR ||
                 r == RemoveReason.UNDELIVERABLE)) { 
                 markDead(ref, r, dmqProps);
            }

            // OK really remove the message
            ref.setInvalid();
            ref = (PacketReference) destMessages.remove(id, r);

            } //synchronized(_removeMessageLock)

            assert ref != null;

            if (ref == null) {
                logger.log(((DEBUG_CLUSTER||DEBUG) ? Logger.INFO:Logger.DEBUG),
                           "Reference has already gone for " + id);
                return ret;
            }

            long l = ref.byteSize();

            // clears out packet, must happen after DMQ
            _messageRemoved(ref, ref.byteSize(), r, true);

            ref.destroy();

            synchronized(this) {
                msgsOut += 1;
                msgBytesOut += ref.byteSize();
                msgsOutInternal += 1;
                if (msgsOutInternal >= Integer.MAX_VALUE) { 
                    msgsInOutLastResetTime = System.currentTimeMillis();
                    msgsInInternal = 0;
                    msgsOutInternal = 0;
                }
                // remove from the global lists
                if (ei != null && r != RemoveReason.EXPIRED) {
                    if (ei != null && expireReaper != null) {
                        expireReaper.removeMessage(ei);
                    }
                }              
            }

            ret.removed = true;
            return ret;

        } finally {
           destMessagesInRemoving.remove(id);
        }
    }

    public String lookupReasonString(Reason r, long arrivalTime,
            long expireTime, long senderTime)
    {
         String reason = null;
         if (r == RemoveReason.EXPIRED || 
             r == RemoveReason.EXPIRED_ON_DELIVERY || 
             r == RemoveReason.EXPIRED_BY_CLIENT) {
             String args[] = { getDestinationUID().toString(),
                               (new Long(expireTime)).toString(),
                               (new Long(arrivalTime)).toString(),
                               (new Long(senderTime)).toString() };
             if (r ==  RemoveReason.EXPIRED) {
                 if (arrivalTime != 0 && expireTime != 0 &&
                     expireTime <= arrivalTime) {
                     reason = br.getKString(
                              BrokerResources.M_DMQ_ARRIVED_EXPIRED, args);
                 } else {
                     reason = br.getKString(
                              BrokerResources.M_DMQ_MSG_EXPIRATION, args);
                 }
             } else if (r == RemoveReason.EXPIRED_ON_DELIVERY) {
                 reason = br.getKString(BrokerResources.M_MSG_EXPIRED_ON_DELIVERY, args);
             } else if (r == RemoveReason.EXPIRED_BY_CLIENT) {
                 reason = br.getKString(BrokerResources.M_MSG_EXPIRED_BY_CLIENT, args);
             } else {
                 reason = br.getKString(BrokerResources.M_DMQ_MSG_EXPIRATION, args);
             }
         } else if (r == RemoveReason.REMOVED_LOW_PRIORITY ||
                    r == RemoveReason.REMOVED_OLDEST) {
             String countLimitStr = (countLimit <= 0 ? 
                    Globals.getBrokerResources().getString(
                    BrokerResources.M_UNLIMITED) :
                    String.valueOf(countLimit));
             String sizeLimitStr = (memoryLimit == null ||
                    memoryLimit.getBytes() <= 0 ? 
                    Globals.getBrokerResources().getString(
                    BrokerResources.M_UNLIMITED) :
                    memoryLimit.toString());
             String args[] = { getDestinationUID().toString(),
                     countLimitStr, sizeLimitStr };

             reason =  br.getKString(BrokerResources.M_DMQ_MSG_LIMIT, args);
         } else if (r == RemoveReason.UNDELIVERABLE) {
             reason = br.getKString(
                       BrokerResources.M_DMQ_MSG_UNDELIVERABLE,
                       getDestinationUID().toString());
         } else {
             reason = br.getKString(
                       BrokerResources.M_DMQ_MSG_ERROR,
                       getDestinationUID().toString());
         }

        return reason;
    }


    public void primaryInterestChanged(Consumer interest) {
        // interest has moved from failover to primary
    }


    private ConnectionUID getConnectionUID(Consumer intr) {
        return intr.getConsumerUID().getConnectionUID();
    }

    public Consumer addConsumer(Consumer interest, boolean notify) 
                    throws BrokerException, SelectorFormatException {
        return addConsumer(interest, notify, null);
    }

    /**
     * @param conn the client connection 
     */
    public Consumer addConsumer(Consumer interest, boolean notify, Connection conn)
        throws BrokerException, SelectorFormatException
    {

        synchronized(consumers) {
            if (consumers.get(interest.getConsumerUID()) != null) {
                throw new ConsumerAlreadyAddedException(
                    br.getKString(BrokerResources.I_CONSUMER_ALREADY_ADDED,
                    interest.getConsumerUID(), this.toString()));
            }
        }

        if (isInternal() && !BrokerMonitor.ENABLED) {
            throw new BrokerException(
               br.getKString(
                    BrokerResources.X_MONITORING_DISABLED, getName()));
        }
        interest.attachToDestination(this);

        interest.addRemoveListener(destMessages);

        if (!loaded && interest.isActive()) {
            load();
        }
        
        synchronized (consumers) {
            if (maxConsumerLimit != UNLIMITED &&
                maxConsumerLimit <= consumers.size()) {
                throw new BrokerException(
                    br.getKString(
                        BrokerResources.X_CONSUMER_LIMIT_EXCEEDED,
                             getName(), String.valueOf(maxConsumerLimit)),
                        BrokerResources.X_CONSUMER_LIMIT_EXCEEDED,
                        (Throwable) null,
                        Status.CONFLICT);
            }
            consumers.put(interest.getConsumerUID(), interest);
            if (bm != null && consumers.size() == 1) {
                bm.start();
            }
            if (state == DestState.CONSUMERS_PAUSED ||
                state == DestState.PAUSED) {
                interest.pause("Destination PAUSE2");
            }

        } 
        synchronized(this) {
            if (destReaper != null) {
                destReaper.cancel();
                destReaper = null;
            }
            clientReconnect();
        }

        // send a message IF we are on a monitor destination
        if (bm != null)
            bm.updateNewConsumer(interest);

        return null;
    }

    public void removeConsumer(ConsumerUID interest, boolean notify) 
        throws BrokerException {
        removeConsumer(interest, null, false, notify);
    }

    public void removeConsumer(ConsumerUID interest, Map remotePendings,
                               boolean remoteCleanup, boolean notify) 
                               throws BrokerException
    {
        Consumer c = null;
        synchronized(consumers) {
            c = (Consumer)consumers.remove(interest);
            synchronized (this) {
                if (bm != null && consumers.size() == 0) {
                    bm.stop();
                }
                if (shouldDestroy()) {
                    if (destReaper != null) {
                        destReaper.cancel();
                        destReaper = null;
                    }
                    destReaper = new DestReaperTask(uid);
                    try {
                        timer.schedule(destReaper, AUTOCREATE_EXPIRE);
                    } catch (IllegalStateException ex) {
                       logger.log(Logger.DEBUG,"Can not reschedule task, "
                            + "timer has been canceled, the broker " +
                            " is probably shutting down", ex);
                    }
                }
            }
        }
        if (c != null) {
            // remove consumer interest in REMOVE of messages
            c.removeRemoveListener(destMessages);
        }
        if (c != null && sendClusterUpdate() && notify) {
            Globals.getClusterBroadcast().destroyConsumer(c, remotePendings, remoteCleanup);
        }
    }

    protected void notifyConsumerAdded(Consumer c, Connection conn) {
        synchronized(consumers) {
            BrokerAddress ba = c.getConsumerUID().getBrokerAddress();
            if (ba == null || ba == Globals.getMyAddress()) {
                Globals.getConnectionManager().
                    getConsumerInfoNotifyManager().consumerAdded(this, conn);
            } else {
                Globals.getConnectionManager().
                    getConsumerInfoNotifyManager().remoteConsumerAdded(this);
            }
        }
    }

    protected void notifyConsumerRemoved() {
        synchronized(consumers) {
            Globals.getConnectionManager().
                    getConsumerInfoNotifyManager().consumerRemoved(this);
        }
    }

    public boolean addProducer(Producer producer)   
        throws BrokerException
    {
        if (isInternal()) {
            throw new BrokerException(
               br.getKString(
                    BrokerResources.X_MONITOR_PRODUCER, getName()));
        }
        if (maxProducerLimit !=  UNLIMITED &&
            producers.size() >= maxProducerLimit ) {
            throw new BrokerException(
                br.getKString(
                    BrokerResources.X_PRODUCER_LIMIT_EXCEEDED,
                         getName(), String.valueOf(maxProducerLimit)),
                    BrokerResources.X_PRODUCER_LIMIT_EXCEEDED,
                    (Throwable) null,
                    Status.CONFLICT);
        }
        synchronized(this) {
            if (destReaper != null) {
                destReaper.cancel();
                destReaper = null;
            }
        }
        // technically, we can wait until we get a producer, but
        // then we have to deal with ordering
        if (!loaded) {
            load();
        }

        try {
            synchronized (producers) {
                producers.put(producer.getProducerUID(), producer);
            }
        } catch (IndexOutOfBoundsException ex) {
            throw new BrokerException(
                br.getKString(
                    BrokerResources.X_PRODUCER_LIMIT_EXCEEDED,
                         getName(), String.valueOf(maxProducerLimit)),
                    BrokerResources.X_PRODUCER_LIMIT_EXCEEDED,
                    (Throwable) ex,
                    Status.CONFLICT);
        }
        producerFlow.addProducer(producer);
        boolean active =  producerFlow.checkResumeFlow(producer, false);
        logger.log(Logger.DEBUGHIGH,"Producer " + producer + " is " + active);

        return active;
    }

    public void removeProducer(ProducerUID producerUID) {
        Producer p = null;
        synchronized (producers) {
            p = (Producer)producers.remove(producerUID);
        }
        if (p == null) return; // nothing to do

        producerFlow.removeProducer(p);
        producerFlow.checkResumeFlow(p, false);

        synchronized (this) {
            if (shouldDestroy()) {
                if (destReaper != null) {
                    destReaper.cancel();
                    destReaper = null;
                }
                destReaper = new DestReaperTask(uid);
                try {
                    timer.schedule(destReaper, AUTOCREATE_EXPIRE);
                } catch (IllegalStateException ex) {
                   logger.log(Logger.DEBUG,"Can not reschedule task, "
                       + "timer has been canceled, "
                       + "the broker is probably shutting down", ex);
                }
            }
        }
    }

    private void dumpStoredSet(Set s) {
        // DEBUG only - no reason to localize
        logger.log(Logger.INFO,"DEBUG: Dumping order");
        int i =0;
        Iterator itr = s.iterator();
        while (itr.hasNext()) {
            PacketReference n = (PacketReference)itr.next();
            logger.log(Logger.INFO, n.getPriority() +
                 " : " + n.getTime() + " :" + n.getSequence()
                 + "  " + n.getSysMessageID() + " : " + n.getPacket().getTimestamp());
        }
    }
 
    public abstract void sort(Comparator c);

    public static void remoteCheckMessageHomeChange(PacketReference ref, BrokerAddress broker) {
        if (ref.getAddress() != null && ref.getAddress().equals(broker)) return;

        Set destroyConns = new HashSet();
        Hashtable cc =  ref.getRemoteConsumerUIDs(); 
        Consumer consumer = null;
        ConsumerUID cuid = null;
        Iterator citr = cc.keySet().iterator();
        while (citr.hasNext()) {
            cuid = (ConsumerUID)citr.next();
            consumer =  Consumer.getConsumer(cuid);
            if (consumer == null) continue;
            if (consumer.tobeRecreated()) {
                destroyConns.add(cc.get(cuid));
            }
        }
        destroyConnections(destroyConns, GoodbyeReason.MSG_HOME_CHANGE,
          GoodbyeReason.toString(GoodbyeReason.MSG_HOME_CHANGE)+"["+ref.getAddress()+":"+broker+"]");
    }

    public static void remoteCheckTakeoverMsgs(Map msgs, String brokerid) throws BrokerException {
        Set destroyConns = new HashSet();
        Iterator msgitr =  msgs.keySet().iterator();
        while (msgitr.hasNext()) {
            SysMessageID sysid = SysMessageID.get((String)msgitr.next());
            PacketReference ref = (PacketReference)Destination.get(sysid);
            if (ref == null) continue;
            Iterator cnitr = ref.getRemoteConsumerUIDs().values().iterator();
            while (cnitr.hasNext()) {
                destroyConns.add(cnitr.next());
            }
        }
        destroyConnections(destroyConns, GoodbyeReason.BKR_IN_TAKEOVER,
          GoodbyeReason.toString(GoodbyeReason.BKR_IN_TAKEOVER)+":"+brokerid);
    }

    public static void destroyConnections(Set destroyConns, int reason, String reasonstr) {

        ConnectionManager cm = Globals.getConnectionManager();
        Iterator cnitr = destroyConns.iterator();
        while (cnitr.hasNext()) {
            IMQBasicConnection conn = (IMQBasicConnection)cm.getConnection(
                                      (ConnectionUID)cnitr.next());
            if (conn == null) continue;
            Globals.getLogger().log(Logger.INFO, 
                "Destroying connection " + conn + " because "+reasonstr);
            if (DEBUG) conn.dump();
            conn.destroyConnection(true, reason, reasonstr);
            conn.waitForRelease(Globals.getConfig().getLongProperty(
                 Globals.IMQ+"."+conn.getService().getName()+".destroy_timeout", 30)*1000);
        }
    }

    public synchronized static void loadTakeoverMsgs(Map msgs, List txns, Map txacks)
         throws BrokerException
    {
        Map m = new HashMap();
        Logger logger = Globals.getLogger();

        Map ackLookup = new HashMap();

        // ok create a hashtable for looking up txns
        if (txacks != null) {
            Iterator itr = txacks.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry entry = (Map.Entry)itr.next();
                TransactionUID tuid = (TransactionUID)entry.getKey();
                List l = (List)entry.getValue();
                Iterator litr = l.iterator();
                while (litr.hasNext()) {
                    TransactionAcknowledgement ta =
                        (TransactionAcknowledgement)litr.next();
                    String key = ta.getSysMessageID() +":" +
                                 ta.getStoredConsumerUID();
                    ackLookup.put(key, tuid);
                }
            }
         }

        // Alright ...
        //    all acks fail once takeover begins
        //    we expect all transactions to rollback
        //    here is the logic:
        //        - load all messages
        //        - remove any messages in open transactions
        //        - requeue all messages
        //        - resort (w/ load comparator)
        //
        //
        // OK, first get msgs and sort by destination
        HashMap openMessages = new HashMap();
        Iterator itr = msgs.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry me = (Map.Entry)itr.next();
            String msgID = (String)me.getKey();
            String dst = (String)me.getValue();
            DestinationUID dUID = new DestinationUID(dst);
            Packet p = null;
            try {
                p = Globals.getStore().getMessage(dUID, msgID);
            } catch (BrokerException ex) {
                // Check if dst even exists!
                if (ex.getStatusCode() == Status.NOT_FOUND) {
                    Destination d = Destination.getDestination(dUID);
                    if (d == null) {
                        String args[] = {
                            msgID, dst, Globals.getBrokerResources().getString(
                                BrokerResources.E_DESTINATION_NOT_FOUND_IN_STORE, dst)};
                        logger.log(Logger.ERROR,
                              BrokerResources.W_CAN_NOT_LOAD_MSG,
                              args, ex);
                    }
                }
                throw ex;
            }

            dUID = DestinationUID.getUID(p.getDestination(), p.getIsQueue());
            PacketReference pr = PacketReference.createReference(p, dUID, null);

            // mark already stored and make packet a SoftReference to
            // prevent running out of memory if dest has lots of msgs
            pr.setLoaded();
            logger.log(Logger.DEBUG,"Loading message " + pr.getSysMessageID() 
                   + " on " + pr.getDestinationUID());
           
            // check transactions
            TransactionUID tid = pr.getTransactionID();
            if (tid != null) {
                // see if in transaction list
                if (txns.contains(tid)) {
                    // open transaction 
                    TransactionState ts = Globals.getTransactionList()
                             .retrieveState(pr.getTransactionID());
                    if (ts != null && 
                        ts.getState() != TransactionState.ROLLEDBACK && 
                        ts.getState() != TransactionState.COMMITTED) {
                        // in transaction ... 
                        logger.log(Logger.DEBUG, "Processing open transacted message " +
                               pr.getSysMessageID() + " on " + tid + 
                               "["+TransactionState.toString(ts.getState())+"]");
                        openMessages.put(pr.getSysMessageID(), tid);
                    }  else if (ts != null && ts.getState() == TransactionState.ROLLEDBACK) {
                        pr.destroy();
                        continue;
                    } else {
                    }
                }
            }
            packetlistAdd(pr.getSysMessageID(), pr.getDestinationUID());

            Set l = null;
            if ((l = (Set)m.get(dUID)) == null) {
                l = new TreeSet(new RefCompare());
                m.put(dUID, l);
            }
            l.add(pr);
        }

        // OK, handle determining how to queue the messages

        // first add all messages

        Iterator dsts = m.entrySet().iterator();
        while (dsts.hasNext()) {
            Map.Entry entry = (Map.Entry)dsts.next();
            DestinationUID dst = (DestinationUID) entry.getKey();
            Set l = (Set)entry.getValue();
            Destination d = Destination.getDestination(dst);

            if (d == null) { // create it 
                try {
                d = Destination.getDestination(dst.getName(), 
                     (dst.isQueue()? DestType.DEST_TYPE_QUEUE:
                          DestType.DEST_TYPE_TOPIC) , true, true);
                } catch (IOException ex) {
                     throw new BrokerException(
                         Globals.getBrokerResources().getKString(
                         BrokerResources.X_CANT_LOAD_DEST, d.getName()));
                }
            } else {
                synchronized(d) {
                    if (d.isLoaded()) {
                        // Destination has already been loaded so just called
                        // initialize() to update the size and bytes variables
                        d.initialize();
                    }
                    d.load(l);
                }
            }
            logger.log(Logger.INFO,
                BrokerResources.I_LOADING_DST,
                   d.getName(), String.valueOf(l.size()));

            // now we're sorted, process
            Iterator litr = l.iterator();
            try {
                while (litr.hasNext()) {
    
                    PacketReference pr = (PacketReference)litr.next();
                    try {
                        // ok allow overrun
                        boolean el = d.destMessages.getEnforceLimits();

                        d.destMessages.enforceLimits(false);

                        pr.lock();
                        d.putMessage(pr, AddReason.LOADED, true);
                        // turn off overrun
                        d.destMessages.enforceLimits(el);
                    } catch (IllegalStateException ex) {
                        // thats ok, we already exists
                        String args[] = { pr.getSysMessageID().toString(),
                            pr.getDestinationUID().toString(),
                            ex.getMessage() };
                        logger.logStack(Logger.WARNING,
                              BrokerResources.W_CAN_NOT_LOAD_MSG,
                               args, ex);
                        continue;
                    } catch (OutOfLimitsException ex) {
                        String args[] = { pr.getSysMessageID().toString(),
                            pr.getDestinationUID().toString(),
                            ex.getMessage() };
                        logger.logStack(Logger.WARNING,
                              BrokerResources.W_CAN_NOT_LOAD_MSG,
                               args, ex);
                        continue;
                    } 
              }
              // then resort the destination
              d.sort(new RefCompare());
           } catch (Exception ex) {
           }
        }

        // now route

        dsts = m.entrySet().iterator();
        while (dsts.hasNext()) {
            Map.Entry entry = (Map.Entry)dsts.next();
            DestinationUID dst = (DestinationUID) entry.getKey();
            Set l = (Set)entry.getValue();
            Destination d = Destination.getDestination(dst);

            // now we're sorted, process
            Iterator litr = l.iterator();
            try {
                while (litr.hasNext()) {
                    PacketReference pr = (PacketReference)litr.next();
                    TransactionUID tuid = (TransactionUID)openMessages.get(pr.getSysMessageID());
                    if (tuid != null) {
                        Globals.getTransactionList().addMessage(tuid,
                                                     pr.getSysMessageID(), true);
                        pr.unlock();
                        continue;
                    }

                    ConsumerUID[] consumers = Globals.getStore().
                            getConsumerUIDs(dst, pr.getSysMessageID());
    
                    if (consumers == null) consumers = new ConsumerUID[0];

                    if (consumers.length == 0 &&
                        Globals.getStore().hasMessageBeenAcked(dst, pr.getSysMessageID())) {
                        logger.log(Logger.INFO,
                            Globals.getBrokerResources().getString(
                                BrokerResources.W_TAKEOVER_MSG_ALREADY_ACKED,
                                pr.getSysMessageID()));
                        d.unputMessage(pr, RemoveReason.ACKNOWLEDGED);
                        pr.destroy();
                        pr.unlock();
                        continue;
                    }

                    if (consumers.length > 0) {
                        pr.setStoredWithInterest(true);
                    } else {
                        pr.setStoredWithInterest(false);
                    }

                    int states[] = null;

                    if (consumers.length == 0) {
                        // route the message, it depends on the type of
                        // message 
                        try {
                            consumers = d.routeLoadedTransactionMessage(pr);
                        } catch (Exception ex) {
                            logger.log(Logger.INFO,"Internal Error "
                               + "loading/routing transacted message, " 
                               + "throwing out message " + 
                               pr.getSysMessageID(), ex);
                        }
                        states = new int[consumers.length];
                        for (int i=0; i < states.length; i ++)  
                            states[i] = Store.INTEREST_STATE_ROUTED;
                        try {
                            Globals.getStore().storeInterestStates(
                                  d.getDestinationUID(),
                                  pr.getSysMessageID(),
                                  consumers, states, true, null);
                            pr.setStoredWithInterest(true);
                        } catch (Exception ex) {
                            // message already routed
                            StringBuffer debuf = new StringBuffer();
                            for (int i = 0; i < consumers.length; i++) {
                                if (i > 0) debuf.append(", ");
                                debuf.append(consumers[i]);
                            }
                            logger.log(logger.WARNING,
                                BrokerResources.W_TAKEOVER_MSG_ALREADY_ROUTED,
                                pr.getSysMessageID(), debuf.toString(), ex);
                        }
                    } else {
                        states = new int[consumers.length];
    
                        for (int i = 0; i < consumers.length; i ++) {
                            states[i] = Globals.getStore().getInterestState(
                                        dst, pr.getSysMessageID(), consumers[i]);
                        }
                    }

                    pr.update(consumers, states, false);

                    // OK deal w/ transsactions
                    // LKS - XXX
                    ExpirationInfo ei = pr.getExpiration();
                    if (ei != null && d.expireReaper != null) {
                        d.expireReaper.addExpiringMessage(ei);
                    }
                    List consumerList = new ArrayList(Arrays.asList(
                                         consumers));

                    // OK ... see if we are in txn
                    Iterator citr = consumerList.iterator();
                    while (citr.hasNext()) {
                        logger.log(Logger.DEBUG," Message " 
                             + pr.getSysMessageID() + " has " 
                             + consumerList.size() + " consumers ");
                        ConsumerUID cuid = (ConsumerUID)citr.next();
                        String key = pr.getSysMessageID() +
                                    ":" + cuid;
                        TransactionList tl = Globals.getTransactionList();
                        TransactionUID tid = (TransactionUID) ackLookup.get(key);
                        if (DEBUG) {
                        logger.log(logger.INFO, "loadTakeoverMsgs: lookup "+key+" found tid="+tid);
                        }
                        if (tid != null) {
                            boolean remote = false;
                            TransactionState ts = tl.retrieveState(tid);
                            if (ts == null) {
                                ts = tl.getRemoteTransactionState(tid);
                                remote = true;
                            }
                            if (DEBUG) {
                            logger.log(logger.INFO, "tid="+tid+" has state="+
                                       TransactionState.toString(ts.getState()));
                            }
                            if (ts != null && 
                                ts.getState() != TransactionState.ROLLEDBACK &&
                                ts.getState() != TransactionState.COMMITTED) {
                                // in transaction ... 
                                if (DEBUG) {
                                    logger.log(Logger.INFO, 
                                    "loadTakeoverMsgs: Open transaction ack ["+key +"]"+
                                     (remote?"remote":"")+", TUID="+tid);
                                }
                                if (!remote) {
                                    try {
                                    tl.addAcknowledgement(tid, pr.getSysMessageID(),
                                                          cuid, cuid, true, false);
                                    } catch (TransactionAckExistException e) {

                                    //can happen if takeover tid's remote txn after restart
                                    //then txn ack would have already been loaded
                                    logger.log(Logger.INFO, 
                                               Globals.getBrokerResources().getKString(
                                               BrokerResources.I_TAKINGOVER_TXN_ACK_ALREADY_EXIST,
                                                       "["+pr.getSysMessageID()+"]"+cuid+":"+cuid, 
                                                       tid+"["+TransactionState.toString(ts.getState())+"]"));

                                    }
                                    tl.addOrphanAck(tid, pr.getSysMessageID(), cuid);

                                } 
                                citr.remove();
                                logger.log(Logger.INFO,"Processing open ack " +
                                      pr.getSysMessageID() + ":" + cuid + " on " + tid);
                                continue;
                            } else if (ts != null &&
                                       ts.getState() == TransactionState.COMMITTED) {
                                logger.log(Logger.INFO, "Processing committed ack "+
                                    pr.getSysMessageID() + ":"+cuid + " on " +tid);
                                if (pr.acknowledged(cuid, cuid, false, true)) {
                                    d.unputMessage(pr, RemoveReason.ACKNOWLEDGED);
                                    pr.destroy();
                                    continue;
                                }
                                citr.remove();   
                                continue;
                            }
                        }
                    }
                    // route msgs not in transaction 
                    if (DEBUG) {
                    StringBuffer buf = new StringBuffer();
                    ConsumerUID cid = null;
                    for (int j = 0; j <consumerList.size(); j++) {
                        cid = (ConsumerUID)consumerList.get(j);
                        buf.append(cid);
                        buf.append(" ");
                    }
                    logger.log(Logger.INFO, "non-transacted: Routing Message " 
                          + pr.getSysMessageID() + " to " 
                          + consumerList.size() + " consumers:"+buf.toString());
                    }
                    pr.unlock();
                    d.routeLoadedMessage(pr, consumerList);
                    if (d.destReaper != null) {
                        d.destReaper.cancel();
                        d.destReaper = null;
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public synchronized  void load() 
        throws BrokerException
    {
        load(false, null, null);
    }

    public synchronized  void load(boolean noerrnotfound) 
        throws BrokerException
    {
        load(false, null, null, null, null, noerrnotfound);
    }

    public synchronized  void load(Set takeoverMsgs) 
        throws BrokerException
    {
        load(false, null, null, null, takeoverMsgs, false);
    }

    protected synchronized  Map load(boolean neverExpire,
            Map preparedAcks,  Map transactionStates) 
        throws BrokerException
    {
        return load(neverExpire, preparedAcks, transactionStates, null, null, false);
    }

    public synchronized  LinkedHashMap load(boolean neverExpire,
            Map preparedAcks,  Map transactionStates, Map committingTrans, 
            Set takeoverMsgs, boolean noerrnotfound) throws BrokerException {

        if (loaded) {
            return null;
        }

        logger.log(Logger.INFO, BrokerResources.I_LOADING_DESTINATION,
                   toString(),  String.valueOf(size));
        LinkedHashMap preparedTrans = null;
        boolean enforceLimit = true;
        Set deadMsgs = new HashSet();

        int maxloadcnt = size;
        int curcnt = 0;

        try {
            enforceLimit = destMessages.getEnforceLimits();
            destMessages.enforceLimits(false);

            Store store = Globals.getStore();
            Enumeration msgs = null;
            try {
                msgs = store.messageEnumeration(this);
            } catch (DestinationNotFoundException e) {
                if (noerrnotfound) {
                    logger.log(Logger.INFO, br.getKString(
                           BrokerResources.I_LOAD_DST_NOTFOUND_INSTORE,
                           getName(), e.getMessage()));
                    return null;
                }
                throw e;
            }

            SortedSet s = null;
            try { // no other store access should occur in this block

            HAMonitorService haMonitor = Globals.getHAMonitorService();
            boolean takingoverCheck = (takeoverMsgs == null && 
                                 Globals.getHAEnabled() && haMonitor != null &&
                                 haMonitor.checkTakingoverDestination(this));
            s = new TreeSet(new RefCompare());
            while (msgs.hasMoreElements()) {
                Packet p = (Packet)msgs.nextElement();
                PacketReference pr =PacketReference.createReference(p, uid, null);
                if (takeoverMsgs != null && takeoverMsgs.contains(pr)) {
                    pr = null;
                    continue;
                }
                if (takingoverCheck && haMonitor.checkTakingoverMessage(p)) {
                    pr = null;
                    continue;
                }
                if (neverExpire)
                    pr.overrideExpireTime(0);
                // mark already stored and make packet a SoftReference to
                // prevent running out of memory if dest has lots of msgs
                pr.setLoaded(); 
                if (DEBUG) {
                    logger.log(Logger.INFO,"Loaded Message " + p +
                         " into destination " + this);
                }
                try {
                    if (!isDMQ && !addNewMessage(false, pr)) {
                        // expired
                        deadMsgs.add(pr);
                    }
                } catch (Exception ex) {
                    String args[] = { pr.getSysMessageID().toString(),
                        pr.getDestinationUID().toString(),
                        ex.getMessage() };
                    logger.logStack(Logger.WARNING,
                              BrokerResources.W_CAN_NOT_LOAD_MSG,
                              args, ex);
                    continue;
                }
                s.add(pr);
                packetlistAdd(pr.getSysMessageID(), pr.getDestinationUID());

                curcnt ++;
                if (curcnt > 0 && (curcnt % LOAD_COUNT == 0
                    || (curcnt > LOAD_COUNT && curcnt == size))) {
                    String args[] = { toString(),
                       String.valueOf(curcnt),
                       String.valueOf(maxloadcnt),
                       String.valueOf((curcnt*100)/maxloadcnt) };
                    logger.log(Logger.INFO,
                        BrokerResources.I_LOADING_DEST_IN_PROCESS,
                       args);
               }

            }

            } finally {
            store.closeEnumeration(msgs);
            }
               
            // now we're sorted, process
            Iterator itr = s.iterator();
            while (itr.hasNext()) {
    
                PacketReference pr = (PacketReference)itr.next();

                // ok .. see if we need to remove the message
                ConsumerUID[] consumers = store.
                    getConsumerUIDs(getDestinationUID(),
                          pr.getSysMessageID());

                if (consumers == null) consumers = new ConsumerUID[0];

                if (consumers.length == 0 &&
                    store.hasMessageBeenAcked(uid,pr.getSysMessageID())) {
                    if (DEBUG) {
                    logger.log(Logger.INFO,"Message " +
                    pr.getSysMessageID()+"["+this+"] has been acked, destory..");
                    }
                    decrementDestinationSize(pr);
                    removePacketList(pr.getSysMessageID(), pr.getDestinationUID());
                    pr.destroy();
                    continue;
                }

                if (consumers.length > 0) {
                    pr.setStoredWithInterest(true);
                } else {
                    pr.setStoredWithInterest(false);
                }

                // first producer side transactions

                boolean dontRoute = false;
                if (pr.getTransactionID() != null) {
                    // if unrouted and not in rollback -> remove
                    Boolean state = (Boolean) (transactionStates == null ?
                             null : transactionStates.get(
                                  pr.getTransactionID()));

                    // at this point, we should be down to 3 states
                    if (state == null ) // committed
                    {
                        if (consumers.length == 0) {
                            // route the message, it depends on the type of
                            // message 
                            try {
                                consumers = routeLoadedTransactionMessage(pr);
                            } catch (Exception ex) {
                                logger.log(Logger.INFO,"Internal Error "
                                   + "loading/routing transacted message, " 
                                   + "throwing out message " + 
                                   pr.getSysMessageID(), ex);
                            }
                            if (consumers.length > 0) {
                                int[] states = new int[consumers.length];
                                for (int i=0; i < states.length; i ++)  
                                    states[i] = Store.INTEREST_STATE_ROUTED;
                                try {
                                    Globals.getStore().storeInterestStates(
                                          getDestinationUID(),
                                          pr.getSysMessageID(),
                                          consumers, states, true, null);
                                    pr.setStoredWithInterest(true);
                                } catch (Exception ex) {
                                      // ok .. maybe weve already been routed
                                }
                            } else {
                                if (DEBUG) {
                                logger.log(Logger.INFO, "Message "+pr.getSysMessageID()+
                                " [TUID="+pr.getTransactionID()+", "+this+"] no interest" +", destroy...");
                                }
                                decrementDestinationSize(pr);
                                removePacketList(pr.getSysMessageID(), pr.getDestinationUID());
                                pr.destroy();
                                continue;
                            }
                        }
                    } else if (state == Boolean.TRUE) // prepared
                    {
                        if (preparedTrans == null)
                            preparedTrans = new LinkedHashMap();
                        preparedTrans.put(pr.getSysMessageID(), 
                              pr.getTransactionID());
                        dontRoute = true;
                    } else { // rolledback
                        if (DEBUG) {
                        logger.log(Logger.INFO, "Message "+pr.getSysMessageID()+
                        " [TUID="+pr.getTransactionID()+", "+this+"] to be rolled back" +", destroy...");
                        }
                        decrementDestinationSize(pr);
                        removePacketList(pr.getSysMessageID(), pr.getDestinationUID());
                        pr.destroy();
                        continue;
                    }
                }
                     

                // if the message has a transactionID AND there are 
                // no consumers, we never had time to route it
                //

                if (consumers.length == 0 && !dontRoute) {   
                    logger.log(Logger.DEBUG,"Unrouted packet " + pr+", "+this);
                    decrementDestinationSize(pr);
                    removePacketList(pr.getSysMessageID(), pr.getDestinationUID());
                    pr.destroy();
                    continue;
                }
    
                int states[] = new int[consumers.length];
    
                for (int i = 0; i < consumers.length; i ++) {
                    states[i] = store.getInterestState(
                        getDestinationUID(),
                        pr.getSysMessageID(), consumers[i]);
                }

                if (consumers.length > 0 ) {
                    pr.update(consumers, states);
                }
                try {
                    putMessage(pr, AddReason.LOADED);
                } catch (IllegalStateException ex) {
                    String args[] = { pr.getSysMessageID().toString(),
                        pr.getDestinationUID().toString(),
                        ex.getMessage() };
                    logger.logStack(Logger.WARNING,
                              BrokerResources.W_CAN_NOT_LOAD_MSG,
                              args, ex);
                    continue;
                } catch (OutOfLimitsException ex) {
                    String args[] = { pr.getSysMessageID().toString(),
                        pr.getDestinationUID().toString(),
                        ex.getMessage() };
                    logger.logStack(Logger.WARNING,
                              BrokerResources.W_CAN_NOT_LOAD_MSG,
                              args, ex);
                    continue;
                }
                ExpirationInfo ei = pr.getExpiration();
                if (ei != null && expireReaper != null) {
                    expireReaper.addExpiringMessage(ei);
                }


                List consumerList = Arrays.asList(consumers);

                // now, deal with consumer side transactions
                Map transCidToState = (Map)(preparedAcks == null ? null : 
                        preparedAcks.get(pr.getSysMessageID()));

                if (transCidToState != null) {
                    // ok .. this isnt code focused on performance, but
                    // its rarely called and only once

                    // new a new list that allows itr.remove()
                    consumerList = new ArrayList(consumerList);
                    
                    Iterator citr = consumerList.iterator();
                    while (citr.hasNext()) {
                        ConsumerUID cuid = (ConsumerUID)citr.next();
                        TransactionUID tid = (TransactionUID)
                                  transCidToState.get(cuid);
                        Boolean state = (Boolean) (transactionStates == null ?
                             null : transactionStates.get(
                                  tid));
                        // OK for committed transactions, acknowledge
                        if (state == null) {
                            // acknowledge
                            if (pr.acknowledged(cuid,
                                 cuid, false, true)) {
                                 if (committingTrans != null && committingTrans.get(tid) != null) {
                                 unputMessage(pr, RemoveReason.ACKNOWLEDGED);
                                 }
                                 decrementDestinationSize(pr);
                                 removePacketList(pr.getSysMessageID(), pr.getDestinationUID());
                                 pr.destroy();
                                 continue;
                             }
                             citr.remove();
                             continue;
                        } else if (state == Boolean.TRUE) {
                            // for prepared transactions, dont route
                             citr.remove();
                        } else if (state == Boolean.FALSE) {
                            // for rolled back transactions, do nothing
                            if (DEBUG) {
                                logger.log(Logger.INFO, "Redeliver message "+
                                pr.getSysMessageID()+" [TUID="+tid+", "+this+"]" +" to consumer "+cuid);
                            }
                        }
                    }
                    // done processing acks                            
                       
                }
                loaded = true; // dont recurse
                if (!dontRoute) {
                    routeLoadedMessage(pr, consumerList);
                }
            }
        } catch (Throwable ex) {
            logger.logStack(Logger.ERROR, BrokerResources.W_LOAD_DST_FAIL,
                     getName(), ex);
            unload(true);
        }
        destMessages.enforceLimits(enforceLimit);
        loaded = true;
            
        // clean up dead messages
        Iterator deaditr = deadMsgs.iterator();
        while (deaditr.hasNext()) {
            PacketReference pr = (PacketReference)deaditr.next();
            try {
                if (preparedTrans != null)
                    preparedTrans.remove(pr.getSysMessageID());
                removeMessage(pr.getSysMessageID(), RemoveReason.EXPIRED);
            } catch (Exception ex) {
                logger.logStack(Logger.INFO,
                    BrokerResources.E_INTERNAL_BROKER_ERROR,
                    "Processing " + pr + " while loading destination " + this, ex);
            }
        }
        logger.log(Logger.INFO, BrokerResources.I_LOADING_DEST_COMPLETE,
                   toString(),  String.valueOf(size));

        return preparedTrans;

    }

    protected void routeLoadedMessage(PacketReference ref,
             List consumerids)
        throws BrokerException, SelectorFormatException
    {
        if (consumerids == null || consumerids.size() == 0) {
            return;
        }
        Iterator itr = consumerids.iterator();

        while (itr.hasNext()) {
            ConsumerUID cuid = (ConsumerUID)itr.next();
            // we dont route messages to queues
            if (cuid == PacketReference.getQueueUID()) {
                Set s = this.routeNewMessage(ref);
                this.forwardMessage(s, ref);
            } else {
                Consumer c = (Consumer)consumers.get(cuid);
                if (c == null) {
                    Set s = this.routeNewMessage(ref);
                    this.forwardMessage(s, ref);
                } else {
                    c.routeMessage(ref, false);
                }
            }
        }
    }

    protected Consumer getConsumer(ConsumerUID uid)
    {
        return (Consumer)consumers.get(uid);
    }


    public void unload(boolean refs) {
        if (DEBUG) {
            logger.log(Logger.DEBUG,"Unloading " + this);
        }
        if (!loaded) {
            return;
        }
        bytes = destMessages.byteSize();
        size = destMessages.size();

        // get all the persistent messages
        Map m = destMessages.getAll(unloadfilter);
        try {

            // unload them
            if (refs) {
                // remove the refs
                Iterator i = m.values().iterator();
                while (i.hasNext()) {
                    PacketReference ref = (PacketReference)i.next();
                    destMessages.remove(ref.getSysMessageID(),RemoveReason.UNLOADED);
                    ref.clear();
                }
                destMessages = new SimpleNFLHashMap();
                remoteSize = 0;
                remoteBytes = 0;
                loaded = false;
                initialize();
            } else { // clear the ref
                Iterator itr = destMessages.values().iterator();
                while (itr.hasNext()) {
                    PacketReference ref = (PacketReference)itr.next();
                    ref.unload();
                }
            }
            // technically we are still loaded so
        } catch (Throwable thr) {
            logger.logStack(Logger.WARNING,
                BrokerResources.E_INTERNAL_BROKER_ERROR,
                "Unloading destination " + this, thr);

            destMessages = new SimpleNFLHashMap();
            remoteSize = 0;
            remoteBytes = 0;
            loaded = false;
            initialize();
        }
    }

    // Sync the message store for the specified destination
    // Note: should only be invoked when txn is committed or rollbacked
    public void sync() throws BrokerException {
        Globals.getStore().syncDestination(this);
    }

    class UnloadFilter implements Filter
    {
        public boolean matches(Object o) {
            assert o instanceof PacketReference;
            return ((PacketReference)o).isPersistent();
        }
        public boolean equals(Object o) {
             return super.equals(o);
        }
        public int hashCode() {
             return super.hashCode();
        }
    };
    transient Filter unloadfilter = new UnloadFilter();


    protected void destroy(String destroyReason)
        throws IOException, BrokerException {
        destroy(destroyReason, false);
    }

    // optional hook for destroying other data
    private void destroy(String destroyReason, boolean noerrnotfound) 
        throws IOException, BrokerException
    {
        synchronized (destinationList) {
            destvalid = false;
        }
        synchronized(this) {

            if (destReaper != null) {
                destReaper.cancel();
                destReaper = null;
            }
            if (reconnectReaper != null) {
                reconnectReaper.cancel();
                reconnectReaper = null;
            }
            if (expireReaper != null) {
                expireReaper.destroy();
                expireReaper = null;
            }
            if (!neverStore || stored) {
                purgeDestination(noerrnotfound);
                try {
                    Globals.getStore().removeDestination(this, PERSIST_SYNC);
                } catch (DestinationNotFoundException e) {
                    if (!noerrnotfound) throw e; 
                    logger.log(Logger.INFO, br.getKString(
                               br.I_RM_DST_NOTFOUND_INSTORE, 
                               getName(), e.getMessage()));
                }
                stored = false;
            }
        }
    }


    public String toString() {
        return uid.getLocalizedName();
    }

    public String getUniqueName() {
        return uid.toString();
    }

    /**
     * @deprecated
     * remove
     */
    public static String getUniqueName(boolean isQueue, String name) {
        return DestinationUID.getUniqueString(name, isQueue);
    }


    /**
     * Called when a specific event occurs
     * @param type the event that occured
     */

    public void eventOccured(EventType type,  Reason r,
                Object target, Object oldValue, Object newValue, 
                Object userdata) 
    {

    }



    protected void _messageAdded(PacketReference ref, Reason r,
                                 boolean overrideRemote) {
        if (r == AddReason.LOADED) {
            if (ref.isLocal() && overrideRemote) {
                synchronized(this) {
                    long objsize = ref.byteSize();
                    remoteSize --;
                    remoteBytes -= objsize;
                    size --;
                    bytes -= objsize;
                    synchronized(this.getClass()) {
                        totalbytes -= objsize;
                        totalcnt --;
                    }
                }
            }
            return;
        }
        incrementDestinationSize(ref);
    }

    private static void packetlistAdd(SysMessageID uid, DestinationUID duid) {
        synchronized (packetlist.getClass()) {
            Set s = (Set)packetlist.get(uid);
            if (s == null) {
                s = Collections.synchronizedSet(new LinkedHashSet());
                packetlist.put(uid, s);
            }
            s.add(duid);
        }
    }

    private static DestinationUID getPacketListFirst(SysMessageID uid) {
        Set s = null;
        synchronized (packetlist.getClass()) {
            s = (Set)packetlist.get(uid);
            if (s == null) {
                return null;
            }
        }
        synchronized (s) {
            Iterator itr = s.iterator();
            if (itr.hasNext())
                return (DestinationUID) itr.next();
        }
        return null;
    }

    private static Object removePacketList(SysMessageID uid, DestinationUID duid) {
        synchronized (packetlist.getClass()) {
            Set s = (Set)packetlist.get(uid);
            if (s == null) return null;
            if (s.contains(duid)) {
                s.remove(duid);
                if (s.isEmpty())
                    packetlist.remove(uid);
                return duid;
            }
            return null;
        }
    }

    protected void _messageRemoved(PacketReference ref, long objsize, Reason r, boolean doCount) {
        if (ref == null) return; // did nothing

        removePacketList(ref.getSysMessageID(), getDestinationUID());
        if (!doCount) return;

        boolean onRollback = false;
        synchronized (this) {
            if (r == RemoveReason.REMOVED_LOW_PRIORITY ||
                r == RemoveReason.REMOVED_OLDEST ||
                r == RemoveReason.REMOVED_OTHER )
                discardedCnt ++;
            else if (r == RemoveReason.EXPIRED || 
                     r == RemoveReason.EXPIRED_BY_CLIENT || 
                     r == RemoveReason.EXPIRED_ON_DELIVERY)
                expiredCnt ++;
            else if (r == RemoveReason.PURGED)
                purgedCnt ++;
            else if (r == RemoveReason.ROLLBACK)
            {
                rollbackCnt ++;
                onRollback=true;
            }
            else if (r == RemoveReason.ACKNOWLEDGED)
                ackedCnt ++;
            else if (r == RemoveReason.OVERFLOW)
                overflowCnt ++;
            else if (r == RemoveReason.ERROR)
                errorCnt ++;
            decrementDestinationSize(ref);
        }
        ref.remove(onRollback);

        // see if we need to pause/resume any consumers
        producerFlow.checkResumeFlow(null, true);
    }


    /**
     * this method is called to determine if a destination can
     * be removed when all interests are removed.
     * it can be removed if:
     *     - it was autocreated
     *     - no existing messages are stored on the destination
     *     - not being taken over; only in HA mode
     */
    public boolean shouldDestroy() {

        if (Globals.getHAEnabled()) {
            HAMonitorService haMonitor = Globals.getHAMonitorService();
            if (haMonitor != null && haMonitor.checkTakingoverDestination(this)) {
                logger.log(Logger.DEBUG, BrokerResources.X_DESTROY_DEST_EXCEPTION,
                    this.getUniqueName(), "destination is being taken over");
                return false;
            }
        }

        return (size() == 0 && isAutoCreated() && !isTemporary() &&
            producers.isEmpty() && consumers.isEmpty());
    }


    boolean overrideP = false;
    boolean overridePvalue = false;
    boolean overrideTTL = false;
    long overrideTTLvalue = 0;

    public void overridePersistence(boolean persist) {
       neverStore = !persist;
       overrideP =true;
       overridePvalue = persist;
    }
    public void clearOverridePersistence() {
        overrideP = false;
    }
    public void overrideTTL(long ttl) {
        overrideTTL = true;
        overrideTTLvalue = ttl;
    }
    public void clearOverrideTTL() {
        overrideTTL = false;
    }

    public boolean shouldOverridePersistence() {
        return overrideP;
    }
    public boolean getOverridePersistence() {
        return overridePvalue;
    }
    public boolean shouldOverrideTTL() {
        return overrideTTL;
    }
    public long getOverrideTTL() {
        return overrideTTLvalue;
    }

    public boolean isInternal() {
        boolean ret = DestType.isInternal(type);
        return ret;
    }

    public boolean isDMQ() {
        boolean ret = DestType.isDMQ(type);
        return ret;
    }

    public boolean isAdmin() {
        boolean ret = DestType.isAdmin(type);
        return ret;
    }

    public int getConsumerCount() {
        return consumers.size();
    }

    public Iterator getConsumers() {
        List l = new ArrayList(consumers.values());
        return l.iterator();
    }

    public List getAllActiveConsumers() {
        List l = new ArrayList();
        synchronized (consumers) {
            Iterator itr = consumers.values().iterator();
            Consumer c = null;
            while (itr.hasNext()) {
                c = (Consumer)itr.next();
                if (c instanceof Subscription) {
                    l.addAll(((Subscription)c).getChildConsumers());
                } else  {
                    l.add(c);
                }
            }
        }
        return l;
    }

    public Iterator getProducers() {
        List l = null;
        synchronized(producers) {
            l = new ArrayList(producers.values());
        }
        return l.iterator();
    }

    public int getProducerCount() {
        return producers.size();
    }

    public int getMaxPrefetch() {
        return maxPrefetch;
    }
    public void setMaxPrefetch(int prefetch) {
	Long oldVal = new Long(maxPrefetch);

        maxPrefetch = prefetch;

        notifyAttrUpdated(DestinationInfo.DEST_PREFETCH, 
			oldVal, new Long(maxPrefetch));
    }
    public void setMaxSharedConsumers(int prefetch) {
        // does nothing for destinations
    }
    public void setSharedFlowLimit(int prefetch) {
        // does nothing for destinations
    }

    public int getMaxNumSharedConsumers() {
        // does nothing for destinations (topic only)
        return -1;
    }
    public int getSharedConsumerFlowLimit() {
        // does nothing for destinations(topic only)
        return 5;
    }



    public long getMsgBytesProducerFlow() {
        // xxx - should we cache this
        if (NO_PRODUCER_FLOW)
            return -1;
        long bytes = 0;
        if (msgSizeLimit == null || msgSizeLimit.getBytes() <= 0) {
            bytes = Limitable.UNLIMITED_BYTES;
        } else {
            bytes = msgSizeLimit.getBytes();
        }
        return bytes;
    }

    public long getBytesProducerFlow() {
        if (NO_PRODUCER_FLOW)
            return -1;
        return producerMsgBatchBytes;
    }
    public int getSizeProducerFlow() {
        if (NO_PRODUCER_FLOW)
            return -1;
        return producerMsgBatchSize;
    }

    public void forceResumeFlow(Producer p)
    {
        producerFlow.pauseProducer(p);
        producerFlow.forceResumeFlow(p);
    }


    public boolean producerFlow(IMQConnection con, Producer producer) {

        producerFlow.pauseProducer(producer);
        boolean retval = producerFlow.checkResumeFlow(producer, true);
        logger.log(Logger.DEBUGHIGH,"producerFlow " + producer + " resumed: "
                   + retval);
        return retval;
    }

   

    
    private static Map destinationList = Collections.synchronizedMap(
                 new HashMap());

    public static  List findMatchingIDs(DestinationUID wildcarduid)
    {
        List l = new ArrayList();
        if (!wildcarduid.isWildcard()) {
            l.add(wildcarduid);
            return l;
        }
        synchronized (destinationList) {
            Iterator itr = destinationList.keySet().iterator();
            while (itr.hasNext()) {
                DestinationUID uid = (DestinationUID)itr.next();
                if (DestinationUID.match(uid, wildcarduid)) {
                    l.add(uid);
                }
            }
        }
        return l;
        
    }

    public static void clearDestinations() 
    {
        destsLoaded = false;
        destinationList.clear();
        packetlist.clear();
        Queue.clear();
        inited = false;

        BrokerConfig cfg = Globals.getConfig();

        // OK add listeners for the properties
        cfg.removeListener(SYSTEM_MAX_SIZE, cl);
        cfg.removeListener(SYSTEM_MAX_COUNT, cl);
        cfg.removeListener(MAX_MESSAGE_SIZE, cl);
        cfg.removeListener(AUTO_QUEUE_STR, cl);
        cfg.removeListener(AUTO_TOPIC_STR, cl);
        cfg.removeListener(DST_REAP_STR, cl);
        cfg.removeListener(MSG_REAP_STR, cl);
        cfg.removeListener(AUTO_MAX_NUM_MSGS, cl);
        cfg.removeListener(AUTO_MAX_TOTAL_BYTES, cl);
        cfg.removeListener(AUTO_MAX_BYTES_MSG, cl);
        cfg.removeListener(AUTO_MAX_NUM_PRODUCERS, cl);
        cfg.removeListener(AUTO_LOCAL_ONLY, cl);
        cfg.removeListener(AUTO_LIMIT_BEHAVIOR, cl);
        cfg.removeListener(USE_DMQ_STR, cl);
        cfg.removeListener(TRUNCATE_BODY_STR, cl);
        cfg.removeListener(LOG_MSGS_STR, cl);
        cl = null;
    }

    // called from multibroker code

    public static void addDestination(Destination d) 
    {
        addDestination(d, true);
    }

    public static void addDestination(Destination d, boolean throwRT) 
    {
        synchronized (destinationList) {
            if (destinationList.get(d.getDestinationUID()) != null) {
                 if (throwRT)
                     throw new RuntimeException("Destination " + d
                        + " is also being" + " created by another broker");
                 return;
            }

            destinationList.put(d.getDestinationUID(), d);
	    Agent agent = Globals.getAgent();
	    if (agent != null)  {
	        agent.registerDestination(d);
	        agent.notifyDestinationCreate(d);
	    }
        }
    }

    public static int destinationsSize()
    {
        return destinationList.size();
    }

    public static LinkedHashMap processTransactions(Map inprocessAcks, Map openTrans, Map committingTrans)
        throws BrokerException
    {
         loadDestinations();
         Subscription.initSubscriptions();
         LinkedHashMap prepared = new LinkedHashMap();
         Iterator itr = getAllDestinations();
         while (itr.hasNext()) {
             Destination d = (Destination)itr.next();
             boolean loaded = d.loaded;
             if (loaded)
                 d.unload(true);
             LinkedHashMap m = d.load(false, inprocessAcks, openTrans, committingTrans, null, false);
             if (m != null)
                 prepared.putAll(m);
         }
         return prepared;
    }
    
    static boolean destsLoaded = false;

    public static void loadDestinations() 
        throws BrokerException
    {
        if (destsLoaded) return;
        Logger logger = Globals.getLogger();
        destsLoaded = true;
        if (defaultIsLocal && !CAN_USE_LOCAL_DEST) {
            Globals.getLogger().log(Logger.ERROR,
               BrokerResources.E_FATAL_FEATURE_UNAVAILABLE,
               Globals.getBrokerResources().getString(
                    BrokerResources.M_LOCAL_DEST)); 
            com.sun.messaging.jmq.jmsserver.Broker.getBroker().exit(
                   1, Globals.getBrokerResources().getKString(
                  BrokerResources.E_FATAL_FEATURE_UNAVAILABLE,
               Globals.getBrokerResources().getString(
                    BrokerResources.M_LOCAL_DEST)),
                BrokerEvent.Type.FATAL_ERROR);

        }
        if (canAutoCreate(true))   {
            logger.log(Logger.INFO,
              BrokerResources.I_QUEUE_AUTOCREATE_ENABLED);
        } 
        if (!canAutoCreate(false)) {
            logger.log(Logger.INFO,
              BrokerResources.I_TOPIC_AUTOCREATE_DISABLED);
        } 
        logger.log(Logger.DEBUG,"Loading All Stored Destinations ");
       

        // before we do anything else, make sure we dont have any
        // unexpected exceptions
        LoadException load_ex = Globals.getStore().getLoadDestinationException();

        if (load_ex != null) {
            // some messages could not be loaded
            LoadException processing = load_ex;
            while (processing != null) {
                String destid = (String)processing.getKey();
                Destination d = (Destination)processing.getValue();
                if (destid == null && d == null) {
                    logger.log(Logger.WARNING,
                          BrokerResources.E_INTERNAL_ERROR,
                         "both key and value are corrupted");
                    continue;
                }
                if (destid == null) { 
                    // store with valid key
                    try {
                        Globals.getStore().storeDestination(d, PERSIST_SYNC);
                    } catch (Exception ex) {
                        logger.log(Logger.WARNING, 
                            BrokerResources.W_DST_RECREATE_FAILED,
                              d.toString(), ex);
                        try {
                            Globals.getStore().removeDestination(d, true);
                        } catch (Exception ex1) {
                            logger.logStack(Logger.DEBUG,"Unable to remove dest", ex1);
                        }
                    }
                } else {
                    DestinationUID duid = new DestinationUID(destid);
                    String name = duid.getName();
                    boolean isqueue = duid.isQueue();
                    int type = isqueue ? DestType.DEST_TYPE_QUEUE
                            : DestType.DEST_TYPE_TOPIC;
                    // XXX we may want to parse the names to determine
                    // if this is a temp destination etc
                    try {
                        d = Destination.createDestination(
                            name, type);
                        d.store();
                        logger.log(Logger.WARNING, 
                            BrokerResources.W_DST_REGENERATE,
                                    duid.getLocalizedName());
                    } catch (Exception ex) {
                        logger.log(Logger.WARNING, 
                            BrokerResources.W_DST_REGENERATE_ERROR,
                                  duid, ex);
                        try {
                            if (duid.isQueue()) {
                                d = new Queue(duid);
                            } else {
                                d = new Topic(duid);
                            }
                            Globals.getStore().removeDestination(d, true);
                        } catch (Exception ex1) {
                            logger.logStack(Logger.DEBUG,
                                "Unable to remove dest", ex1);
                        }
                    }

                } // end if
                processing = processing.getNextException();
            } // end while
        } 

       // retrieve stored destinations
        try {
            Destination dests[] = Globals.getStore().getAllDestinations();
            if (DEBUG)
                logger.log(Logger.DEBUG, "Loaded {0} stored destinations",
                    String.valueOf(dests.length));

            for (int i =0; i < dests.length; i ++) {
                if ( dests[i] == null) 
                    continue;
                if (DEBUG)
                    logger.log(Logger.INFO, "Destination: Loading destination {0}",
                        dests[i].toString());
                if (!dests[i].isAdmin() && (dests[i].getIsDMQ() || !dests[i].isInternal())) {
                    dests[i].initialize();
                }
                if (dests[i].isAutoCreated() &&
                    dests[i].size == 0 &&  
                    dests[i].bytes == 0) {
                    destinationList.remove(dests[i].getDestinationUID());
                    try {
                        Globals.getLogger().log(logger.INFO,
                              BrokerResources.I_DST_ADMIN_DESTROY, dests[i].getName());
                        dests[i].destroy(Globals.getBrokerResources().getString
                                (BrokerResources.M_AUTO_REAPED));
                    } catch (BrokerException ex) {
                        // if HA, another broker may have removed this
                        //
                        if (ex.getStatusCode() == Status.NOT_FOUND) {
                            // someone else removed it already
                            return;
                        }
                        throw ex;
                    }
                } else {
                    addDestination(dests[i], false);
                }
            }

            deadMessageQueue = createDMQ();

            // iterate through and deal with monitors
            Iterator itr = destinationList.values().iterator();
            while (itr.hasNext()) {
                Destination d = (Destination)itr.next();
                try {
                    d.initMonitor();
                } catch (IOException ex) {
                    logger.logStack(logger.INFO,
                          BrokerResources.I_CANT_LOAD_MONITOR,
                          d.toString(),
                          ex);
                    itr.remove();
               }
            }

        } catch (BrokerException ex) {
            logger.logStack(Logger.ERROR,BrokerResources.E_INTERNAL_BROKER_ERROR,  "unable to load destinations", ex);
            throw ex;
        } catch (IOException ex) {
            logger.logStack(Logger.ERROR,BrokerResources.E_INTERNAL_BROKER_ERROR,  "unable to load destinations", ex);
            throw new BrokerException(BrokerResources.X_LOAD_DESTINATIONS_FAILED,
                   ex);
        } finally {
        }

    }

    public boolean isValid() {
        return valid && destvalid;
    }

    public void incrementRefCount() 
        throws BrokerException
    {
        synchronized(destinationList) {
            if (!valid) {
                throw new IllegalStateException("Broker Shutting down");
            }
            if (!isValid()) {
                throw new BrokerException("Destination already destroyed");
            }
            refCount ++;
        }
    }

    public synchronized void decrementRefCount() {
        synchronized(destinationList) {
            refCount --;
        }
    }

    public int getRefCount() {
        synchronized(destinationList) {
            return refCount;
        }
    }

    public static Destination getLoadedDestination(DestinationUID uid)
    {
        Destination d = null;
        synchronized(destinationList) {
            d = (Destination)destinationList.get(uid);
        }
        if ((d != null) && !d.dest_inited)
            d.initialize();
        return d;
    }

    public static Destination getDestination(DestinationUID uid) {
        Destination d = null;
        synchronized(destinationList) {
            d = (Destination)destinationList.get(uid);

            if (d == null) {
                try {
                    Store pstore = Globals.getStore();
                    d = pstore.getDestination(uid);
                    if (d != null) {
                        addDestination(d, false);
                    }
                } catch (Exception ex) {
                    // ignore we want to create it
                }
            }
        }
        if ((d != null) && !d.dest_inited)
            d.initialize();
        return d;
    }

    public static Destination findDestination(DestinationUID uid) {
        Destination d = null;
        synchronized(destinationList) {
            d = (Destination)destinationList.get(uid);
        }
        return d;
    }


    // XXX : Destination class public methods should normally accept
    // DestinationUID to identify destinations. (Instead of name,
    // type).
    public static Destination getDestination(String name, boolean isQueue) 
                throws BrokerException, IOException
    {
        DestinationUID uid = new DestinationUID(name, isQueue);
        return getDestination(uid);
    }

    public static Destination findDestination(String name, boolean isQueue) 
                throws BrokerException, IOException
    {
        DestinationUID uid = new DestinationUID(name, isQueue);
        return findDestination(uid);
    }


    public static Destination getLoadedDestination(String name, boolean isQueue) 
                throws BrokerException, IOException
    {
        DestinationUID uid = new DestinationUID(name, isQueue);
        return getLoadedDestination(uid);
    }

    
    public static Destination getDestination(String name, int type, 
              boolean autocreate, boolean store) 
                throws BrokerException, IOException
    {
        DestinationUID uid = new DestinationUID(name, DestType.isQueue(type)); 
        return getDestination(uid, type, autocreate, store);
    }

    /**
     * @param uid the destination uid 
     * @param type the type of destination specified by uid
     */
    public static Destination getDestination(DestinationUID uid, int type,
                                             boolean autocreate, boolean store) 
                                             throws BrokerException, IOException {

        Destination d = (Destination)destinationList.get(uid);

        if (autocreate && d == null) {
            try {
               d = createDestination(uid.getName(), type, store,
                   true, null);
            } catch (ConflictException ex) {
                //re-get destination             
                d = (Destination)destinationList.get(uid);
            }
        }
        if (d != null && !d.dest_inited)
            d.initialize();
        return d;
    }

    public static Destination createDestination(String name, int type) 
                throws BrokerException, IOException
    {
        Destination d = createDestination(name, type, true,
                 false, null, true, false);

        if (d != null && !d.dest_inited)
                d.initialize();

        return d;
    }

    public static Destination createTempDestination(String name, 
        int type, ConnectionUID uid, boolean store, long time) 
                throws BrokerException, IOException
    {
        Destination d = null;
        try {
            d = createDestination(name, type, false,
                 false, uid);
            d.setReconnectInterval(time);
            d.overridePersistence(store);
            d.store();
        } catch (ConflictException ex) {
            d = getDestination(name, type, false, false);
        }

        return d;
    }

    static protected boolean valid = true;
    public static void shutdown() {
        valid = false;
    }

    public static boolean isShutdown() {
        return valid;
    }


    public static Destination createDestination(String name, int type,
            boolean store, boolean autocreated, Object from) 
                throws BrokerException, IOException
    {
        ConnectionUID uid = null;
        boolean remote = false;
        if (from instanceof ConnectionUID) {
            uid = (ConnectionUID) from;
        }
        if (from instanceof BrokerAddress) {
            remote = ((BrokerAddress)from).equals(Globals.getMyAddress());
        }
        return createDestination(name, type, store, autocreated,
             uid, !remote, CAN_USE_LOCAL_DEST && DestType.isLocal(type)); 
    }


    // XXX : The public createDestination methods can be renamed so
    // that it is easier to find the right variant. (e.g.
    // createTempDestination, createAutoDestination,
    // createClusterDestination etc...

    private static Destination createDestination(String name, int type,
            boolean store, boolean autocreated, ConnectionUID uid,
            boolean notify, boolean localOnly) 
                throws BrokerException, IOException
    {
        DestinationUID duid = new DestinationUID(name, DestType.isQueue(type));
        if (!valid) {
            throw new BrokerException(
                       Globals.getBrokerResources().getKString(
                           BrokerResources.X_SHUTTING_DOWN_BROKER),
                       BrokerResources.X_SHUTTING_DOWN_BROKER,
                       (Throwable) null,
                       Status.ERROR );
        }
        if (destinationList.get(duid) != null) {
            throw new ConflictException(
                   Globals.getBrokerResources().getKString(
                        BrokerResources.X_DESTINATION_EXISTS,
                        duid));
        }
        // OK, check the persistent store (required for HA)
        try {
            Store pstore = Globals.getStore();
            Destination d = pstore.getDestination(duid);
            if (d != null) {
                addDestination(d, false);
                return d;
            }
        } catch (Exception ex) {
            // ignore we want to create it
        }
       String lockname = null;

       ClusterBroadcast mbi = Globals.getClusterBroadcast();

       boolean clusterNotify = false;
       Destination d = null;
       try {
            if (DestType.isQueue(type)) {
                d = new Queue(name, type, 
                    store, uid, autocreated);
            } else {
                d = new Topic(name, type, 
                    store, uid, autocreated);
            }
            d.setClusterNotifyFlag(notify);

            try {
              synchronized (destinationList) {
                    Destination newd = (Destination)destinationList.get(duid);
                    if (newd != null) { // updating existing
                        throw new BrokerException("Destination already exists");
                    }

                    if (!autocreated)
                        d.setIsLocal(localOnly);

                    if (store) {
                        d.store();
                    }
                    destinationList.put(duid, d);

               }
            } catch (BrokerException ex) {
                    // may happen with timing of two brokers in an
                    // HA cluster
                    // if this happens, we should try to re-load the 
                    // destination
                    //
                    // so if we get a Broker Exception, throw a new
                    // Conflict message
                    throw new BrokerException(
                        Globals.getBrokerResources().getKString(
                            BrokerResources.X_DESTINATION_EXISTS,
                            d.getName()),
                        BrokerResources.X_DESTINATION_EXISTS,
                        (Throwable) ex,
                        Status.CONFLICT );

            }
            clusterNotify = !d.isAutoCreated() && d.sendClusterUpdate() && notify;

            if (mbi != null && clusterNotify ) { // only null in standalone tonga test
                // prevents two creates at the same time
                // if this is a response to a creation on another broker ..
                // dont worry about locking
                if (!mbi.lockDestination(duid, uid)) {
                     throw new ConflictException("Internal Exception:"
                       + " Destination " + duid + " is in the process"
                       + " of being created");
                }
            }
            if (clusterNotify && mbi != null ) {
                // we dont care about updating other brokers for 
                // autocreated, internal or admin destinations
                // we may or may not update local dests (depends on version
                // of cluster)
                mbi.createDestination(d);
            }
        } finally {
            if (mbi != null && clusterNotify) { // only null in tonga test
                mbi.unlockDestination(duid, uid);
            }
        }

        // NOW ATTACH ANY WILDCARD PRODUCERS OR CONSUMERS
        Iterator itr = Consumer.getWildcardConsumers();
        while (itr.hasNext()) {
            ConsumerUID cuid = (ConsumerUID) itr.next();
            Consumer c = Consumer.getConsumer(cuid);
            if (c == null){
                Globals.getLogger().log(Logger.INFO,"Consumer already destroyed");
                continue;
            }
            DestinationUID wuid = c.getDestinationUID();
            // compare the uids
            if (DestinationUID.match(d.getDestinationUID(), wuid)) {
                try {
                // attach the consumer
                    if (c.getSubscription() != null) {
                         d.addConsumer(c.getSubscription(), false /* XXX- TBD*/);
                    } else {
                        d.addConsumer(c, false);
                    }


                } catch (SelectorFormatException ex) {
                   //LKS TBD
                }
            }
         }

        itr = Producer.getWildcardProducers();
        while (itr.hasNext()) {
            ProducerUID puid = (ProducerUID) itr.next();
            Producer p = Producer.getProducer(puid);
            DestinationUID wuid = p.getDestinationUID();
            // compare the uids
            if (DestinationUID.match(d.getDestinationUID(), wuid)) {
                // attach the consumer
                d.addProducer(p);
            }
        }

	Agent agent = Globals.getAgent();
	if (agent != null)  {
	    agent.registerDestination(d);
	    agent.notifyDestinationCreate(d);
	}
        return d;
    }

    private void setClusterNotifyFlag(boolean b) {
        this.clusterNotifyFlag = b;
    }
    private boolean getClusterNotifyFlag() {
        return clusterNotifyFlag;
    }

    public static Destination removeDestination(String name, boolean isQueue, String reason)
        throws IOException, BrokerException
    {
        DestinationUID duid = new DestinationUID(name, isQueue);
        return removeDestination(duid, true, reason);
    }

    public static Destination removeDestination(DestinationUID uid, boolean notify, String reason)
        throws IOException, BrokerException
    {
        Destination d = null;
        boolean noerrnotfound = Globals.getHAEnabled() && !notify;
        if (noerrnotfound) {
            // Quick check to see if dst is in memory; doesn't load/initialize
            d = findDestination(uid);
            if (d != null && !d.isTemporary()) {
                // Because temp dst can be deleted in HA, do this check to avoid
                // getting error during load if it has already been deleted
                d = getDestination(uid);
            }
        } else {
            d = getDestination(uid);
        }

        if (d != null) {
            if (d.isDMQ) {
                throw new BrokerException(
                    Globals.getBrokerResources().getKString(
                        BrokerResources.X_DMQ_INVAID_DESTROY));
            } else if (notify && d.sendClusterUpdate() && !d.isTemporary()) {
                Globals.getClusterBroadcast().recordRemoveDestination(d);
            }

            int level = (DestType.isAdmin(d.getType()) ?
                Logger.DEBUG : Logger.INFO);
            Globals.getLogger().log(level,
                BrokerResources.I_DST_ADMIN_DESTROY, d.getName());
        }

        try {
            d =(Destination)destinationList.get(uid);
            DestinationUID.clearUID(uid); // remove from cache
            if (d != null) {
                if (d.producers.size() > 0) {
                    String args[] = { d.getName(),
                       String.valueOf(d.producers.size()),
                       reason};
                    
                    Globals.getLogger().log(Logger.WARNING,
                        BrokerResources.W_DST_ACTIVE_PRODUCERS,
                        args);
                } 
                if (d.consumers.size() > 0) {
                    int csize = d.consumers.size();
                    boolean destroyDurables = false;
                    Set cons = new HashSet(d.consumers.values());
                    Iterator itr = cons.iterator();
                    while (itr.hasNext()) {
                        Consumer c = (Consumer)itr.next();
                        if (c instanceof Subscription && 
                               ((Subscription)c).isDurable()) {
                            destroyDurables = true;
                            Subscription s = (Subscription)c;
                            if (s.isActive()) {
                                csize += s.getActiveSubscriberCnt();
                            }
                            Subscription.unsubscribeOnDestroy(
                                s.getDurableName(),
                                s.getClientID(), notify);
                            csize --;
                         }
                    }
                    if (destroyDurables) {
                        Globals.getLogger().log(Logger.INFO,
                              BrokerResources.I_DST_DURABLE_RM,
                              d.toString(), reason);
                    }
                    if (csize > 0) {
                        String args[] = { d.getName(),
                           String.valueOf(csize),
                           reason};
                        Globals.getLogger().log(Logger.WARNING,
                            BrokerResources.W_DST_ACTIVE_CONSUMERS,
                            args);
                    }
                }
                if (d.size() > 0) {
                    Globals.getLogger().log(Logger.WARNING,
                        BrokerResources.W_REMOVING_DST_WITH_MSG,
                        String.valueOf(d.size()), d.toString());
                }
                d.destroy(reason, noerrnotfound);
                if (notify && d.sendClusterUpdate())
                    Globals.getClusterBroadcast().destroyDestination(d);

	        Agent agent = Globals.getAgent();
	        if (agent != null)  {
	            agent.notifyDestinationDestroy(d);
	            agent.unregisterDestination(d);
	        }
            } 
        } finally {
            d =(Destination)destinationList.remove(uid);
        }
        return d ;

    }

    public static boolean removeDestination(Destination dest, String reason) 
        throws IOException, BrokerException
    {
        return removeDestination(dest.getDestinationUID(), true, reason) != null;
    }


    /**
     * id  is the ID (if any) to match
     * mast is the DestType to match
     *
     * @param id is either a BrokerAddress or a ConnectionUID
     */
    public static Iterator getDestinations(Object id, int mask)
    {

        List tq = new ArrayList();

        synchronized (destinationList) {
            Collection values = destinationList.values();
            Iterator itr = values.iterator();
            while (itr.hasNext()) {
                Destination dest = (Destination)itr.next();
                if (((dest.getType() & mask) == mask) && 
                    (id == null || id.equals(dest.getConnectionUID()) ||
                     ((id instanceof BrokerAddress) && id == Globals.getMyAddress()
                      && dest.getClusterNotifyFlag() && dest.sendClusterUpdate()))) 

                {
                    tq.add(dest);
                }
            }

        }
        return tq.iterator();
        
    }

    public static List getLocalTemporaryDestinations() {
        List tq = new ArrayList();

        synchronized (destinationList) {
            Set keys = destinationList.keySet();
            Iterator itr = keys.iterator();
            while (itr.hasNext()) {
               Destination dest = (Destination)itr.next();
               if (dest.isTemporary() && dest.getIsLocal()) {
                    tq.add(dest);
                }
            }

        }

        if (DEBUG) {
           Globals.getLogger().log(Logger.DEBUGHIGH, "Matching destinations are: " );
           for (int i =0; tq != null && i < tq.size(); i ++ ) {
               Globals.getLogger().log(Logger.DEBUGHIGH, "\t {0}", tq.get(i).toString());
           }
           Globals.getLogger().log(Logger.DEBUGHIGH, "----------------------" );
        }
        return tq;
        
    }


    // called after a destination has been changed because of an admin
    // action
    public void updateDestination() 
        throws BrokerException, IOException
    {
        update(true);
    }

    public static Iterator getAllDestinations() {
        return getAllDestinations(Destination.ALL_DESTINATIONS_MASK);
    }

    public static Iterator getAllDestinations(int mask) {

        return getDestinations(null, mask);
    }

    public static Iterator getTempDestinations(BrokerAddress address) {
        return getDestinations(address, Destination.TEMP_DESTINATIONS_MASK);
    }

    public static Iterator getStoredDestinations() {
        return getDestinations( null,
              (Destination.ALL_DESTINATIONS_MASK &
              (~ DestType.DEST_TEMP)));
    }


    //-------------------------------------------------------
    //-------------------------------------------------------
    // overall system methods for messages
    //-------------------------------------------------------
    //-------------------------------------------------------

     /**
     * maximum size of any message (in bytes) 
     */
    private static SizeString individual_max_size = null;

    /**
     * memory max
     */
    private static SizeString max_size = null;

    /**
     * message max 
     */
    private static long message_max_count = 0;

    /**
     * current size in bytes of total data
     */
    private static long totalbytes = 0;

    /**
     * current size of total data
     */
    private static int totalcnt = 0;
    private static int totalcntNonPersist = 0;

    
    /**
     * list of messages to destination
     */
    private static Map packetlist = Collections.synchronizedMap(new HashMap());


    public static final String SYSTEM_MAX_SIZE 
                 = Globals.IMQ + ".system.max_size";
    public static final  String SYSTEM_MAX_COUNT 
                 = Globals.IMQ + ".system.max_count";
    public static final  String MAX_MESSAGE_SIZE 
                 = Globals.IMQ + ".message.max_size";

    private static ConfigListener cl = new ConfigListener() {
                public void validate(String name, String value)
                    throws PropertyUpdateException {
            
                }
            
                public boolean update(String name, String value) {
                    BrokerConfig cfg = Globals.getConfig();
                    if (name.equals(SYSTEM_MAX_SIZE)) {
                        setMaxSize(cfg.getSizeProperty(SYSTEM_MAX_SIZE));
                    } else if (name.equals(SYSTEM_MAX_COUNT)) {
                        setMaxMessages(cfg.getIntProperty(SYSTEM_MAX_COUNT));
                    } else if (name.equals(MAX_MESSAGE_SIZE)) {
                        setIndividualMessageMax(
                          cfg.getSizeProperty(MAX_MESSAGE_SIZE));
                    } else if (name.equals(AUTO_QUEUE_STR)) {
                          ALLOW_QUEUE_AUTOCREATE = cfg.getBooleanProperty(
                               AUTO_QUEUE_STR);
                    } else if (name.equals(AUTO_TOPIC_STR)) {
                          ALLOW_TOPIC_AUTOCREATE = cfg.getBooleanProperty(
                               AUTO_TOPIC_STR);
                    } else if (name.equals(DST_REAP_STR)) {
                          AUTOCREATE_EXPIRE = cfg.getLongProperty(
                               DST_REAP_STR) * 1000L;
                    } else if (name.equals(MSG_REAP_STR)) {
                          MESSAGE_EXPIRE = cfg.getLongProperty(
                               MSG_REAP_STR) * 1000L;
                    } else if (name.equals(AUTO_MAX_NUM_MSGS)) {
                          defaultMaxMsgCnt = cfg.getIntProperty(
                               AUTO_MAX_NUM_MSGS);
                    } else if (name.equals(AUTO_MAX_TOTAL_BYTES)) {
                          defaultMaxMsgBytes = cfg.getSizeProperty(
                               AUTO_MAX_TOTAL_BYTES);
                    } else if (name.equals(AUTO_MAX_BYTES_MSG)) {
                          defaultMaxBytesPerMsg = cfg.getSizeProperty(
                               AUTO_MAX_BYTES_MSG);
                    } else if (name.equals(AUTO_MAX_NUM_PRODUCERS)) {
                          defaultProducerCnt = cfg.getIntProperty(
                               AUTO_MAX_NUM_PRODUCERS);
                    } else if (name.equals(AUTO_LOCAL_ONLY)) {
                          defaultIsLocal = cfg.getBooleanProperty(
                               AUTO_LOCAL_ONLY);
                    } else if (name.equals(AUTO_LIMIT_BEHAVIOR)) {
                          defaultLimitBehavior =
                              DestLimitBehavior.getStateFromString(
                                  Globals.getConfig().
                                  getProperty(AUTO_LIMIT_BEHAVIOR));
                    } else if (name.equals(USE_DMQ_STR)) {
                          autocreateUseDMQ = cfg.getBooleanProperty(
                               USE_DMQ_STR);
                    } else if (name.equals(TRUNCATE_BODY_STR)) {
                          storeBodyWithDMQ = !cfg.getBooleanProperty(
                               TRUNCATE_BODY_STR);
                    } else if (name.equals(LOG_MSGS_STR)) {
                          verbose = cfg.getBooleanProperty(
                               LOG_MSGS_STR);
                    } else if (name.equals(DEBUG_LISTS_PROP)) {
                          DEBUG_LISTS = Boolean.valueOf(value);
                    } else if (name.equals(CHECK_MSGS_RATE_AT_DEST_CAPACITY_RATIO_PROP)) {
                          CHECK_MSGS_RATE_AT_DEST_CAPACITY_RATIO = cfg.getIntProperty(
                              CHECK_MSGS_RATE_AT_DEST_CAPACITY_RATIO_PROP);
                    } else if (name.equals(CHECK_MSGS_RATE_FOR_ALL_PROP)) {
                          CHECK_MSGS_RATE_FOR_ALL = cfg.getBooleanProperty(
                              CHECK_MSGS_RATE_FOR_ALL_PROP);
                    } else if (name.equals(MAX_DELAY_NEXT_FETCH_MILLISECS_PROP)) {
                          MAX_DELAY_NEXT_FETCH_MILLISECS = cfg.getIntProperty(
                              MAX_DELAY_NEXT_FETCH_MILLISECS_PROP);
                    }
                    return true;
                }

            };


    public void debug() {
        logger.log(Logger.INFO,"Dumping state for destination " + this);
        logger.log(Logger.INFO,"Consumer Count " + consumers.size());
        logger.log(Logger.INFO,"Producer Count " + producers.size());
        logger.log(Logger.INFO,"Message count " + destMessages.size());
        logger.log(Logger.INFO," --------- consumers");
        Iterator itr = consumers.values().iterator();
        while (itr.hasNext()) {
            Consumer c = (Consumer)itr.next();
            c.debug("\t");
        }
    }
   

    private static boolean inited = false;

    public static void init() 
        throws BrokerException
    {

        if (inited) {
            if (!valid) {
                throw new BrokerException(
                Globals.getBrokerResources().getKString(
                    BrokerResources.X_SHUTTING_DOWN_BROKER));
            }
            return;
        }
        valid = true;
        inited = true;

        BrokerConfig cfg = Globals.getConfig();

        // OK add listeners for the properties
        cfg.addListener(SYSTEM_MAX_SIZE, cl);
        cfg.addListener(SYSTEM_MAX_COUNT, cl);
        cfg.addListener(MAX_MESSAGE_SIZE, cl);
        cfg.addListener(AUTO_QUEUE_STR, cl);
        cfg.addListener(AUTO_TOPIC_STR, cl);
        cfg.addListener(DST_REAP_STR, cl);
        cfg.addListener(MSG_REAP_STR, cl);
        cfg.addListener(AUTO_MAX_NUM_MSGS, cl);
        cfg.addListener(AUTO_MAX_TOTAL_BYTES, cl);
        cfg.addListener(AUTO_MAX_BYTES_MSG, cl);
        cfg.addListener(AUTO_MAX_NUM_PRODUCERS, cl);
        cfg.addListener(AUTO_LOCAL_ONLY, cl);
        cfg.addListener(AUTO_LIMIT_BEHAVIOR, cl);
        cfg.addListener(USE_DMQ_STR, cl);
        cfg.addListener(TRUNCATE_BODY_STR, cl);
        cfg.addListener(LOG_MSGS_STR, cl);
        cfg.addListener(DEBUG_LISTS_PROP, cl);
        cfg.addListener(CHECK_MSGS_RATE_AT_DEST_CAPACITY_RATIO_PROP, cl);
        cfg.addListener(CHECK_MSGS_RATE_FOR_ALL_PROP, cl);
        cfg.addListener(MAX_DELAY_NEXT_FETCH_MILLISECS_PROP, cl);

        // now configure the system based on the properties
        setMaxSize(cfg.getSizeProperty(SYSTEM_MAX_SIZE));
        setMaxMessages(cfg.getIntProperty(SYSTEM_MAX_COUNT));
        setIndividualMessageMax(cfg.getSizeProperty(MAX_MESSAGE_SIZE));
        Queue.init();
        loadDestinations();
    }

    /**
     * sets the maximum size of an individual message
     *
     * @param size the size limit for a message (0 is no message
     *           maximum)
     */
    public static void setIndividualMessageMax(SizeString size)
    {
        if (size == null)
            size = new SizeString();

        individual_max_size = size;
        long bytesize = size.getBytes();
	//
	// Bug id = 6366551
	// Esc id = 1-13890503
	// 
	// 28/02/2006 - Tom Ross
	// forward port by Isa Hashim July 18 2006
	//
	if (bytesize <= 0){
		bytesize = Long.MAX_VALUE;
	}
	// end of bug change


        // Inform packet code of the largest packet size allowed
        Packet.setMaxPacketSize(bytesize);

        // update memory
        if (Globals.getMemManager() != null)
            Globals.getMemManager().updateMaxMessageSize(bytesize);
        
    }

    /**
     * sets the maximum # of messages (total = swap & memory)
     *
     * @param messages the maximum number of messages (0 is no message
     *           maximum)
     */
    public static  void setMaxMessages(long messages)
    {
        message_max_count = messages;
    }


    /**
     * sets the maximum size of all messages (total = swap & memory)
     *
     * @param size the maximum size of messages (0 is no message
     *           size maximum)
     */
    public  static void setMaxSize(SizeString size)
    {

        if (size == null)
            size = new SizeString();
        max_size = size;
    }

    public static PacketReference get(SysMessageID id)
    { 
        return get(id, true);
    }
    public static PacketReference get(SysMessageID id, boolean wait)
    { 
        DestinationUID uid = (DestinationUID)getPacketListFirst(id);
        if (uid == null) return null;
        Destination d = (Destination)destinationList.get(uid);
        if (d == null) return null;
        PacketReference ref = (PacketReference)d.destMessages.get(id);
        if (ref == null) return null;
        return ref.checkLock(wait);
    }
    
    public  PacketReference getMessage(SysMessageID id)
    {        
    	return (PacketReference)destMessages.get(id);       
    }

    public static boolean isLocked(SysMessageID id)
    { 
        DestinationUID uid = (DestinationUID)getPacketListFirst(id);
        if (uid == null) return false;
        Destination d = (Destination)destinationList.get(uid);
        if (d == null) return false;
        PacketReference ref = (PacketReference)d.destMessages.get(id);
        if (ref == null) return false;
        return (ref.checkLock(false) == null);
    }
    
    /**
     * adds information on the new message to the globals tables.
     * It may or may not change the size (since the size may
     * already have been taken into account
     * @param checkLimits true if message is new, false if it is
     *       just being loaded into memory
     * @param ref the reference to use
     * @return true if the message was added, false it if wasnet
     *         because it had expired
     * @throws BrokerException if a system limit has been exceeded
     */
    private static  boolean addNewMessage(
               boolean checkLimits, PacketReference ref)
        throws BrokerException {

        if (checkLimits) {
            // existing msgs can not fail to add if limits are
            // exceeded, so we only need to do limit checks on 
            // non admin stored messages
            checkSystemLimit(ref); 
        }

        // Add to list
        packetlistAdd(ref.getSysMessageID(), ref.getDestinationUID());

        if (ref.isExpired()) {
            return false;
        }

        // check on expiration
        return true;
    }

    public static long checkSystemLimit(PacketReference ref)
        throws BrokerException {
    
        long room = -1;

        // check message size
        long indsize = individual_max_size.getBytes();
        if (indsize > 0 && ref.byteSize() > indsize) {
            String limitstr = (indsize <= 0 ?
                               Globals.getBrokerResources().getString(
                                    BrokerResources.M_UNLIMITED):
                                individual_max_size.toString());

            String msgs[] = { String.valueOf(ref.byteSize()),
                              ref.getSysMessageID().toString(), 
                              limitstr};
            throw new BrokerException(
                Globals.getBrokerResources().getKString(
                BrokerResources.X_IND_MESSAGE_SIZE_EXCEEDED, msgs),
                BrokerResources.X_IND_MESSAGE_SIZE_EXCEEDED,
                (Throwable) null, Status.ENTITY_TOO_LARGE);
        }

        long newsize = 0;
        int newcnt = 0;
        synchronized(Destination.class) {
            newcnt = totalcnt + 1;
            newsize = totalbytes + ref.byteSize();
        }

        // first check if we have exceeded our maximum message count
        if (message_max_count > 0 && newcnt > message_max_count) {
            String limitstr = (message_max_count <= 0 ?
                               Globals.getBrokerResources().getString(
                                    BrokerResources.M_UNLIMITED):
                                String.valueOf(message_max_count));
            throw new BrokerException(
                Globals.getBrokerResources().getKString(
                BrokerResources.X_MAX_MESSAGE_COUNT_EXCEEDED,
                limitstr, ref.getSysMessageID()),
                BrokerResources.X_MAX_MESSAGE_COUNT_EXCEEDED,
                (Throwable) null, Status.RESOURCE_FULL);
        }
        if (message_max_count > 0) {
            room = message_max_count - totalcnt;
            if (room < 0) {
                room  = 0;
            }
        }
        // now check if we have exceeded our maximum message size
        if (max_size.getBytes() > 0 && newsize > max_size.getBytes()) {
            String limitstr = (max_size.getBytes() <= 0 ?
                               Globals.getBrokerResources().getString(
                                   BrokerResources.M_UNLIMITED):
                                   max_size.toString());
            throw new BrokerException(
                Globals.getBrokerResources().getKString(
                    BrokerResources.X_MAX_MESSAGE_SIZE_EXCEEDED,
                        limitstr, ref.getSysMessageID()),
                        BrokerResources.X_MAX_MESSAGE_SIZE_EXCEEDED,
                        (Throwable) null, Status.RESOURCE_FULL);
        }    
        if (max_size.getBytes() > 0) {
            long cnt = (max_size.getBytes() - totalbytes)/ref.byteSize();
            if (cnt < 0) {
                cnt = 0;
            }
            if (cnt < room) {
                room = cnt;
            }
        }
        return room;
    }

    public static synchronized int totalCount() {
        assert totalcnt >= 0;
        return totalcnt;
    }
    public static synchronized long totalBytes() {
        assert totalbytes >= 0;
        return totalbytes;
    }

    public static synchronized int totalCountNonPersist() {
        assert totalcntNonPersist >= 0;
        return totalcntNonPersist;
    }

    public static float totalCountPercent() {
        if (message_max_count <= 0) {
            return (float)0.0;
         }
         synchronized(Destination.class) {
             return ((float)totalcnt/(float)message_max_count)*100;
        }
     }

     private static RemoveMessageReturnInfo 
     removeExpiredMessage(DestinationUID duid, SysMessageID id)
     throws BrokerException {

          RemoveMessageReturnInfo ret = null;

          if (duid == null) {
              throw new RuntimeException("expired messages");
          }
          Destination d = (Destination)destinationList.get(duid);
          if (d != null) {
              ret = d._removeMessage(id, RemoveReason.EXPIRED, null, null, true);
              if (ret.removed) {
                 removePacketList(id, d.getDestinationUID());
              }
          }
          if (ret == null) {
              ret = new RemoveMessageReturnInfo();
              ret.removed = false;
              ret.indelivery = false;
          }
          return ret;
     }

    public static boolean canAutoCreate(boolean queue) {
        return (queue ? ALLOW_QUEUE_AUTOCREATE :
            ALLOW_TOPIC_AUTOCREATE);
    }

    public static boolean canAutoCreate(boolean queue,int type) {

	if (DestType.isTemporary(type)){
		return false;
	}

        return (queue ? ALLOW_QUEUE_AUTOCREATE :
            ALLOW_TOPIC_AUTOCREATE);
    }



    static final int DEST_PAUSE = 0;
    static final int DEST_RESUME = 1;
    static final int DEST_UPDATE = 2;
    static final int DEST_BEHAVIOR_CHANGE = 3;


    class ProducerFlow
    {

        transient Map pausedProducerMap = null;
        transient Map activeProducerMap = null;

        public ProducerFlow() {
            pausedProducerMap = new LinkedHashMap();
            activeProducerMap = new LinkedHashMap();
        }


        public synchronized int pausedProducerCnt() {
            return pausedProducerMap.size();
        }

        public synchronized int activeProducerCnt() {
            return activeProducerMap.size();
        }
        public synchronized Vector getDebugPausedProducers()
        {
            Vector v = new Vector();
            Iterator itr = pausedProducerMap.values().iterator();
            while (itr.hasNext()) {
                try {
                    Object o = itr.next();
                    ProducerUID pid = 
                        ((Producer)itr.next()).getProducerUID();
                    v.add(String.valueOf(pid.longValue()));
                } catch (Exception ex) {
                    v.add(ex.toString());
                }
            }
            return v;
        }

        public synchronized Vector getDebugActiveProducers()
        {
            Vector v = new Vector();
            Iterator itr = activeProducerMap.values().iterator();
            while (itr.hasNext()) {
                try {
                    Object o = itr.next();
                    ProducerUID pid = 
                        ((Producer)itr.next()).getProducerUID();
                    v.add(String.valueOf(pid.longValue()));
                } catch (Exception ex) {
                    v.add(ex.toString());
                }
            }
            return v;
        }
    
        private void sendResumeFlow(Producer p, boolean pause,
                String reason)
        {
            int size = 0;
            long bytes = 0;
            long mbytes = 0;
            if (!pause) {
                size = producerMsgBatchSize;
                bytes = producerMsgBatchBytes;
                mbytes = getMsgBytesProducerFlow();
            }

            p.sendResumeFlow(getDestinationUID(), size, bytes, mbytes, reason, false);
        }

        public  void updateAllProducers(int why,
                String info)
        {
            if (why == DEST_PAUSE) {
               synchronized (this) {
                   // iterate through the active Producers
                   Iterator itr = activeProducerMap.values().iterator();
                   while (itr.hasNext()) {
                         Producer p = (Producer)itr.next();
                         pausedProducerMap.put(p.getProducerUID(), p);
                         itr.remove();
                         p.pause();
                         sendResumeFlow(p, true,info);
                   }
               }
            } else if (why == DEST_RESUME) {
                checkResumeFlow(null, true, info);
            } else if (why == DEST_UPDATE || why == DEST_BEHAVIOR_CHANGE ) {

               synchronized (this) {
                   // iterate through the active Producers
                   Iterator itr = activeProducerMap.values().iterator();
                   while (itr.hasNext()) {
                       Producer p = (Producer)itr.next();
                       sendResumeFlow(p, false,info);
                   }
                }
            }             
        }

        public synchronized boolean pauseProducer(Producer producer)
        {
            boolean wasActive = false;
            if (activeProducerMap.remove(producer.getProducerUID()) != null) {
                pausedProducerMap.put(producer.getProducerUID(),
                           producer);
                wasActive = true;
            }
            producer.pause();
            return wasActive;
        }

        public boolean isProducerActive(ProducerUID uid) {
             return pausedProducerMap.get(uid) == null;
        }

        private synchronized void resumeProducer(Producer producer)
        {
            if (pausedProducerMap.remove(producer.getProducerUID()) != null) {
                activeProducerMap.put(producer.getProducerUID(), producer);
            }
            producer.resume();
        }


        public  boolean checkResumeFlow(Producer p, 
                    boolean notify)
        {

             return checkResumeFlow(p, notify, null);
        }

        private  boolean checkResumeFlow(Producer producer, 
                    boolean notify, String info)
        {

            // note: lock order destMessages->this->producer
            // so any calls into destMessages must hold the destMessages
            // lock before this
            //
            synchronized (this) {
                // 1. handle pausing/resuming any producers
                // 2. then handle if the producer passed in (if any)
                // 3. handle pause/resume
                // 4. then notify
                // 
                // first deal with Paused destinations
                // 
                if (state == DestState.PRODUCERS_PAUSED || 
                    state == DestState.PAUSED) {
                    if (activeProducerMap != null) {
                        // pause all
                       Iterator itr = activeProducerMap.values().iterator();
                       while (itr.hasNext()) {
                             Producer p = (Producer)itr.next();
                             pausedProducerMap.put(p.getProducerUID(), p);
                             itr.remove();
                             p.pause();

                             // notify if notify=true or pids dont match
                             boolean shouldNotify = notify || 
                                   (producer != null && 
                                    !p.getProducerUID().equals(
                                      producer.getProducerUID()));
                             if (shouldNotify)
                                 sendResumeFlow(p, true,info);
                       }
                    }
                    return false; // paused
                }
                // now handle the case where we arent using FLOW_CONTROL
                if (limit != DestLimitBehavior.FLOW_CONTROL) {
                    if (pausedProducerMap != null) {
                        // pause all
                       Iterator itr = pausedProducerMap.values().iterator();
                       while (itr.hasNext()) {
                             Producer p = (Producer)itr.next();
                             activeProducerMap.put(p.getProducerUID(), p);
                             itr.remove();
                             p.resume();
                             // notify if notify=true or pids dont match
                             boolean shouldNotify = notify || 
                                   (producer != null && 
                                    !p.getProducerUID().equals(
                                      producer.getProducerUID()));
                             if (shouldNotify)
                                 sendResumeFlow(p, false,info);
                       }
                    }
                    return true; // not paused
                }
            }

            boolean resumedProducer = false;

            // ok, now we are picking the producer to flow control
            synchronized (destMessages) {
                int fs = destMessages.freeSpace();
                long fsb = destMessages.freeBytes();

                synchronized (this) {
     
                    Iterator itr = pausedProducerMap.values().iterator();

                    while (itr.hasNext() &&

                         (fs == Limitable.UNLIMITED_CAPACITY ||
                            (fs > 0  &&
                             fs > (activeProducerMap.size()*
                                 producerMsgBatchSize)))
                       && (fsb == Limitable.UNLIMITED_BYTES ||
                            (fsb > 0 &&
                              fsb > (activeProducerMap.size()*
                              producerMsgBatchBytes))))
                    {
                         // remove the first producer
                         Producer p = (Producer)itr.next();

                         if (!p.isValid()) {
                             continue;
                         }
                         if (DEBUG)
                             logger.log(logger.DEBUGHIGH,"Resuming producer " 
                                 + p + " The destination has " + fs 
                                 + " more space and " 
                                 + activeProducerMap.size() 
                                 + " active producers ["
                                 + " batch size " 
                                 + producerMsgBatchSize + "  msg " 
                                 + destMessages.size());

                         activeProducerMap.put(p.getProducerUID(), p);
                         itr.remove();
                         p.resume();
                         // notify if notify=true or pids dont match
                         boolean shouldNotify = notify || 
                                   (producer != null && 
                                    !p.getProducerUID().equals(
                                      producer.getProducerUID()));


                         if ( shouldNotify) {
                             if (info == null) {
                                 info = "Producer " 
                                   + p.getProducerUID()
                                   + " has become active";
                             }
 
                             sendResumeFlow(p, false, info);
                        }
                    }
                    if (producer != null) {
                        resumedProducer = activeProducerMap.containsKey(
                              producer.getProducerUID());
                    }
                } // end synchronize
                
            }
            return resumedProducer;
            
        }

        public void forceResumeFlow(Producer p)
        {
           synchronized (destMessages) {
                int fs = destMessages.freeSpace();
                long fsb = destMessages.freeBytes();

                synchronized (this) {
                    if ((fs == Limitable.UNLIMITED_CAPACITY ||
                             fs >= (activeProducerMap.size()*
                                 producerMsgBatchSize))
                       && (fsb == Limitable.UNLIMITED_BYTES ||
                          fsb >= (activeProducerMap.size()*
                          producerMsgBatchBytes)))
                    {
                         activeProducerMap.put(p.getProducerUID(), p);
                         pausedProducerMap.remove(p.getProducerUID());
                         p.resume();
                         sendResumeFlow(p, false, "Producer " 
                                   + p.getProducerUID()
                                   + " has become active");
                    }
                } // end synchronize
                
            }
        }

        public synchronized boolean removeProducer(Producer producer)
        {
             producer.destroy();
             Object oldobj =
                     activeProducerMap.remove(producer.getProducerUID());
             pausedProducerMap.remove(producer.getProducerUID());
             return oldobj != null;
        }

        public synchronized boolean addProducer(Producer producer)
        {
            // always add as paused, then check
            Object o = 
              pausedProducerMap.put(producer.getProducerUID(), producer);
            return o == null;
        }

    }
    public boolean isProducerActive(ProducerUID uid) {
        return producerFlow.isProducerActive(uid);
    }

    /**
     * Method for trigerring JMX notifications whenever a destination
     * attribute is updated.
     *
     * @param attr	Attribute that was modified. Constant from DestinationInfo
     *			class.
     * @param oldVal	Old value
     * @param newVal	New value
     */
    public void notifyAttrUpdated(int attr, Object oldVal, Object newVal)  {
	Agent agent = Globals.getAgent();
	if (agent != null)  {
	    agent.notifyDestinationAttrUpdated(this, attr, oldVal, newVal);
	}
    }
    
    public void setValidateXMLSchemaEnabled(boolean b)  {
	this.validateXMLSchemaEnabled = b;
    }

    public boolean validateXMLSchemaEnabled()  {
	return (this.validateXMLSchemaEnabled);
    }

    public void setXMLSchemaUriList(String s)  {
	this.XMLSchemaUriList = s;
    }

    public String getXMLSchemaUriList()  {
	return (this.XMLSchemaUriList);
    }

    public void setReloadXMLSchemaOnFailure(boolean b)  {
	this.reloadXMLSchemaOnFailure = b;
    }

    public boolean reloadXMLSchemaOnFailure()  {
	return (this.reloadXMLSchemaOnFailure);
    }
}


class LoadComparator implements Comparator
{
    public int compare(Object o1, Object o2) {
        if (o1 instanceof PacketReference && o2 instanceof PacketReference) {
                PacketReference ref1 = (PacketReference) o1;
                PacketReference ref2 = (PacketReference) o2;
                // compare priority
                long dif = ref2.getPriority() - ref1.getPriority();

                if (dif == 0)
                    dif = ref1.getTimestamp() - ref2.getTimestamp();

                // then sequence
                if (dif == 0)
                    dif = ref1.getSequence() - ref2.getSequence();
                if (dif < 0) return -1;
                if (dif > 0) return 1;
                return 0;
        } else {
            assert false;
            return o1.hashCode() - o2.hashCode();
        }
    }
    public int hashCode() {
        return super.hashCode();
    }

    public boolean equals(Object o) {
        return super.equals(o);
    }
    
}

