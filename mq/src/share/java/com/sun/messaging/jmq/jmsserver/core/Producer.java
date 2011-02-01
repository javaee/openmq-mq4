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

import java.util.*;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.jmsserver.service.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.io.PacketType;
import com.sun.messaging.jmq.util.CacheHashMap;
import com.sun.messaging.jmq.util.lists.Limitable;
import com.sun.messaging.jmq.util.log.*;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.jmsserver.management.agent.Agent;

/**
 *
 */

//XXX - it would be nice to add metrics info
// unfortunately we dont know what producer a message
// comes from at this time
public class Producer {
    
    private static boolean DEBUG=false;

    private transient Logger logger = Globals.getLogger();

    // record information about the last 20 removed
    // producers
    private static CacheHashMap cache = new CacheHashMap(20);

    private boolean valid = true;
    
    private static Map allProducers = Collections.synchronizedMap(
                    new HashMap());

    private static Set wildcardProducers = Collections.synchronizedSet(
                    new HashSet());

    private ConnectionUID connection_uid;
    
    private DestinationUID destination_uid;
    private ProducerUID uid;
    
    private long creationTime;

    private int pauseCnt = 0;
    private int resumeCnt = 0;
    private int msgCnt = 0;
    transient String creator = null;

    transient Set destinations = null;

    transient Map lastResumeFlowSizes = Collections.synchronizedMap(new HashMap());

    public String toString() {
        return "Producer["+ uid + "," + destination_uid + "," +
               connection_uid + "]";
    }
    
    public static Hashtable getAllDebugState() {
        Hashtable ht = new Hashtable();
        ht.put("TABLE", "AllProducers");
        Vector v = new Vector();
        synchronized (cache) {
            Iterator itr = cache.keySet().iterator();
            while (itr.hasNext()) {
                v.add(String.valueOf(((ProducerUID)itr.next()).longValue()));
            }
            
        }
        ht.put("cache", v);
        HashMap tmp = null;
        synchronized(allProducers) {
            tmp = new HashMap(allProducers);
        }
        Hashtable producers = new Hashtable();
        Iterator itr = tmp.keySet().iterator();
        while(itr.hasNext()) {
            ProducerUID p = (ProducerUID)itr.next();
            Producer producer = (Producer)tmp.get(p);
            producers.put(String.valueOf(p.longValue()),
                  producer.getDebugState());
        }
        ht.put("producersCnt", new Integer(allProducers.size()));
        ht.put("producers", producers);
        return ht;
            
    }

    public synchronized void pause() {
        pauseCnt ++;
    }

    public synchronized void addMsg()
    {
        msgCnt ++;
    }

    public synchronized int getMsgCnt()
    {
        return msgCnt;
    }
    public synchronized boolean isPaused()
    {
        return pauseCnt > resumeCnt;
    }

    public synchronized void resume() { 
        resumeCnt ++;
    }

    public Hashtable getDebugState() {
        Hashtable ht = new Hashtable();
        ht.put("TABLE", "Producer["+uid.longValue()+"]");
        ht.put("uid", String.valueOf(uid.longValue()));
        ht.put("valid", String.valueOf(valid));
        ht.put("pauseCnt", String.valueOf(pauseCnt));
        ht.put("resumeCnt", String.valueOf(resumeCnt));
        if (connection_uid != null)
            ht.put("connectionUID", String.valueOf(connection_uid.longValue()));
        if (destination_uid != null)
            ht.put("destination", destination_uid.toString());
        return ht;
    }

    /** Creates a new instance of Producer */
    private Producer(ConnectionUID cuid, DestinationUID duid) {
        uid = new ProducerUID();
        this.connection_uid = cuid;
        this.destination_uid = duid;
        logger.log(Logger.DEBUG,"Creating new Producer " + uid + " on "
             + duid + " for connection " + cuid);
    }
   
    public ProducerUID getProducerUID() {
        return uid;
    } 

    public ConnectionUID getConnectionUID() {
        return connection_uid;
    }
    public DestinationUID getDestinationUID() {
        return destination_uid;
    }

    public static void clearProducers() {
        cache.clear();
        allProducers.clear();
        wildcardProducers.clear();
    }

    public static String checkProducer(ProducerUID uid)
    {
        String str = null;

        synchronized (cache) {
            str = (String)cache.get(uid);
        }
        if (str == null) {
             return " pid " + uid + " not of of last 20 removed";
        }
        return "Producer[" +uid + "]:" + str;
    }

    public static void updateProducerInfo(ProducerUID uid, String str)
    {
        synchronized (cache) {
            cache.put(uid, System.currentTimeMillis() + ":" + str);
        }
    }

    public static Iterator getAllProducers() {
        return (new ArrayList(allProducers.values())).iterator();
    }

    public static Iterator getWildcardProducers() {
        return (new ArrayList(wildcardProducers)).iterator();
    }

    public static int getNumProducers() {
        return (allProducers.size());
    }

    public static int getNumWildcardProducers() {
        return (wildcardProducers.size());
    }

    public static Producer getProducer(ProducerUID uid) {
        return (Producer)allProducers.get(uid);
    }

    public static Producer destroyProducer(ProducerUID uid, String info) {
        Producer p = (Producer)allProducers.remove(uid);
        updateProducerInfo(uid, info);
        if (p == null) {
            return p;
        }
        if (p.getDestinationUID().isWildcard()) {
            wildcardProducers.remove(p.getProducerUID());
            // remove from each destination
            List duids = Destination.findMatchingIDs(p.getDestinationUID());
            Iterator itr = duids.iterator();
            while (itr.hasNext()) {
                DestinationUID duid = (DestinationUID)itr.next();
                Destination d = Destination.getDestination(duid);
                if (d != null) {
                   d.removeProducer(uid);
                }
            }


        } else {
            Destination d = Destination.getDestination(p.getDestinationUID());
            if (d != null) {
               d.removeProducer(uid);
            }
        }
        p.destroy();
        return p;
    }

    public static Producer createProducer(DestinationUID duid,
              ConnectionUID cuid, String id) 
    {
        Producer producer = new Producer(cuid, duid);
        Object old = allProducers.put(producer.getProducerUID(), producer);
        if (duid.isWildcard())
            wildcardProducers.add(producer.getProducerUID());
        producer.creator = id;
        assert old == null : old;

        return producer;
    }

    public synchronized void destroy() {
        valid = false;
        lastResumeFlowSizes.clear();
    }

    public synchronized boolean isValid() {
        return valid;
    }

    public static Producer getProducer(String creator)
    {
        if (creator == null) return null;

        synchronized(allProducers) {
            Iterator itr = allProducers.values().iterator();
            while (itr.hasNext()) {
                Producer c = (Producer)itr.next();
                if (creator.equals(c.creator))
                    return c;
            }
        }
        return null;
    }

    public boolean isWildcard() {
        return destination_uid.isWildcard();
    }

    public Set getDestinations() {
        if (this.destinations == null) {
            destinations = new HashSet();
            if (!destination_uid.isWildcard()) {
                destinations.add(Destination.getDestination(destination_uid));
            } else {
                List l = Destination.findMatchingIDs(destination_uid);
                Iterator itr = l.iterator();
                while (itr.hasNext()) {
                    DestinationUID duid = (DestinationUID)itr.next();
                    destinations.add(Destination.getDestination(duid));
                }
                    
            }
        }
        return destinations;
    }

    class ResumeFlowSizes {
         int size = 0;
         long bytes = 0;
         long mbytes = 0;

         public ResumeFlowSizes(int s, long b, long mb) {
             size = s;
             bytes = b;
             mbytes = mb;
         }
    }

    public void sendResumeFlow(DestinationUID duid) {
        resume();
        sendResumeFlow(duid, 0, 0, 0, ("Resuming " + this), true);
        logger.log(Logger.DEBUGHIGH,"Producer.sendResumeFlow("+duid+") resumed: "+this);
    }

    public void sendResumeFlow(DestinationUID duid, 
                               int size, long bytes, long mbytes,
                               String reason, boolean uselast) {

        ResumeFlowSizes rfs = null;
        if (uselast) {
            rfs = (ResumeFlowSizes)lastResumeFlowSizes.get(duid);
            if (rfs == null) {
                rfs = new ResumeFlowSizes(Destination.MAX_PRODUCER_BATCH,
                                            -1, Limitable.UNLIMITED_BYTES);
                lastResumeFlowSizes.put(duid, rfs);
            }
        } else {
            rfs = new ResumeFlowSizes(size, bytes, mbytes);
            lastResumeFlowSizes.put(duid, rfs);
        }

        ConnectionUID cuid = getConnectionUID();
        if (cuid == null) {
            logger.log(Logger.DEBUG,"cant resume flow[no con_uid] " + this);
            return;
        }

        IMQConnection con =(IMQConnection)Globals.getConnectionManager()
                                                 .getConnection(cuid);

        if (reason == null) reason = "Resuming " + this;

        Hashtable hm = new Hashtable();
        hm.put("JMQSize", rfs.size);
        hm.put("JMQBytes", new Long(rfs.bytes));
        hm.put("JMQMaxMsgBytes", new Long(rfs.mbytes));
        if (con != null) {
            Packet pkt = new Packet(con.useDirectBuffers());
            pkt.setPacketType(PacketType.RESUME_FLOW);
            hm.put("JMQProducerID", new Long(getProducerUID().longValue()));
            hm.put("JMQDestinationID", duid.toString());
            hm.put("Reason", reason);
            pkt.setProperties(hm);
            con.sendControlMessage(pkt);
        }
    }

}
