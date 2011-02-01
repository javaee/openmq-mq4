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
 * @(#)FalconProtocol.java	1.43 07/23/07
 */ 

package com.sun.messaging.jmq.jmsserver.multibroker.falcon;

import java.util.*;
import java.io.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.Broker;
import com.sun.messaging.jmq.jmsservice.BrokerEvent;
import com.sun.messaging.jmq.util.log.*;
import com.sun.messaging.jmq.io.*;
import com.sun.messaging.jmq.util.*;
import com.sun.messaging.jmq.util.selector.*;
import com.sun.messaging.jmq.jmsserver.core.*;
import com.sun.messaging.jmq.jmsserver.config.*;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.service.ConnectionUID;
import com.sun.messaging.jmq.jmsserver.multibroker.MessageBusCallback;
import com.sun.messaging.jmq.jmsserver.multibroker.ClusterGlobals;
import com.sun.messaging.jmq.jmsserver.multibroker.Protocol;
import com.sun.messaging.jmq.jmsserver.multibroker.Cluster;
import com.sun.messaging.jmq.jmsserver.multibroker.CallbackDispatcher;
import com.sun.messaging.jmq.jmsserver.multibroker.BrokerInfo;
import com.sun.messaging.jmq.jmsserver.multibroker.raptor.RaptorProtocol;

public class FalconProtocol
{
    protected static final long ConsumerVersionUID = 99353142765567461L;

    protected Logger logger = Globals.getLogger();

    protected MessageBusCallback cb = null;
    protected CallbackDispatcher cbDispatcher = null;
    
    protected int version = ClusterGlobals.VERSION_300;

    public FalconProtocol(MessageBusCallback cb, Cluster c, 
            BrokerAddress myaddress)
       throws BrokerException
    {
        this.cb = cb;
        this.cbDispatcher = new CallbackDispatcher(cb);
    }


    public static Consumer readConsumer(DataInputStream dis,
        CallbackDispatcher cbDispatcher)
          throws IOException
    {
        Logger logger = Globals.getLogger();
        ConsumerUID id = null;
        String destName = null;
        String clientID = null;
        String durableName = null;
        String selstr = null;
        boolean isQueue;
        boolean noLocalDelivery;
        boolean consumerReady;

        long ver = dis.readLong(); // version
        if (ver != ConsumerVersionUID) {
            throw new IOException("Wrong Consumer Version " + ver + " expected " + ConsumerVersionUID);
        }
        destName = dis.readUTF();
        boolean hasId = dis.readBoolean();
        if (hasId) {
            id = readConsumerUID(dis);
        }
        boolean hasClientID = dis.readBoolean();
        if (hasClientID) {
            clientID = dis.readUTF();
        }
        boolean hasDurableName = dis.readBoolean();
        if (hasDurableName) {
            durableName = dis.readUTF();
        }

        boolean hasSelector = dis.readBoolean();
        if (hasSelector) {
            selstr = dis.readUTF();
        }

        isQueue = dis.readBoolean();
        noLocalDelivery = dis.readBoolean();
        consumerReady = dis.readBoolean();


        try {
            DestinationUID dest = DestinationUID.getUID(destName, isQueue);
            Consumer c = new Consumer(dest, selstr, noLocalDelivery,
                    id);
            if (durableName != null) {
                Subscription sub = Subscription.findCreateDurableSubscription
                       (clientID,durableName, dest, selstr, noLocalDelivery);

                int type = (dest.isQueue() ? DestType.DEST_TYPE_QUEUE :
                     DestType.DEST_TYPE_TOPIC);

                // Make sure that the destination exists...
                Destination tmpdest = Destination.getDestination(
                    dest.getName(),
                    type, true, true);

                if (cbDispatcher != null)
                    cbDispatcher.interestCreated(sub);

                sub.attachConsumer(c);
             }
             return c;
         } catch (SelectorFormatException ex) {
             logger.log(Logger.INFO,"L10N-XXX Got bad selector["+selstr + "] " , ex);
             throw new IOException("bad selector " + selstr);
         } catch (BrokerException ex) {
             logger.log(Logger.INFO,"L10N-XXX error creating consumer ", ex);
             throw new IOException("error creating consumer ");
         }

    }


    protected static ConsumerUID readConsumerUID(DataInputStream dis)
          throws IOException
    {
        long id = dis.readLong(); // UID write
        ConnectionUID conuid = new ConnectionUID(dis.readLong());
        BrokerAddress tempaddr = Globals.getMyAddress();
        if (tempaddr == null) {
            // XXX Revisit and cleanup : This method may be called
            // before cluster initialization only during persistent
            // store upgrade. i.e. from -
            // FalconProtocol.upgradeConfigChangeRecord()
            // At that time, Globals.getMyAddress() returns null.
            // Hence this kludge...
            try {
                tempaddr =
                    new com.sun.messaging.jmq.jmsserver.multibroker.fullyconnected.BrokerAddressImpl();
            }
            catch (Exception e) {}
        }


        BrokerAddress brokeraddr = (BrokerAddress)tempaddr.clone();
        brokeraddr.readBrokerAddress(dis); // UID write
        ConsumerUID cuid = new ConsumerUID(id);
        cuid.setConnectionUID(conuid);
        cuid.setBrokerAddress(brokeraddr);
        return cuid;
    }

    public static byte[] upgradeConfigChangeRecord(byte[] olddata)
        throws IOException {

        ChangeRecord cr = ChangeRecord.makeChangeRecord(olddata);

        if (cr.eventDestId == ClusterGlobals.MB_RESET_PERSISTENCE) {
            return RaptorProtocol.prepareResetPersistenceRecord();
        }
        else if (cr.eventDestId == ClusterGlobals.MB_INTEREST_UPDATE) {
            ByteArrayInputStream bis = new ByteArrayInputStream(cr.rec);
            DataInputStream dis = new DataInputStream(bis);

            int pktVersion = dis.readInt();
            int type = dis.readInt();
            int count = dis.readInt();

            if (type == ClusterGlobals.MB_NEW_INTEREST) {
                dis.readBoolean(); // is durable?
                dis.readUTF(); // durable name
                dis.readUTF(); // client id
                Consumer c = readConsumer(dis, null);
                Subscription sub = c.getSubscription();
                return RaptorProtocol.generateAddDurableRecord(sub);
            }

            if (type == ClusterGlobals.MB_REM_DURABLE_INTEREST) {
                String dname = dis.readUTF();
                String cname = dis.readUTF();
                Subscription sub = Subscription.findDurableSubscription(
                    cname, dname);
                return RaptorProtocol.generateRemDurableRecord(sub);
            }
        }
        else if (cr.eventDestId == ClusterGlobals.MB_DESTINATION_UPDATE) {
            ByteArrayInputStream bis = new ByteArrayInputStream(cr.rec);
            DataInputStream dis = new DataInputStream(bis);

            int pktVersion = dis.readInt();
            int operation = dis.readInt();
            String name = dis.readUTF();
            int type = dis.readInt();

            if (operation == ClusterGlobals.MB_NEW_DESTINATION) {
                DestinationUID duid = null;
                try {
                    duid = DestinationUID.getUID(name, type);
                } catch (BrokerException ex) {
                    // should never happen, destination should already be verified
                    return olddata;
                }
                Destination d = Destination.getDestination(duid);
                return RaptorProtocol.generateAddDestinationRecord(d);
            }

            if (operation == ClusterGlobals.MB_REM_DESTINATION) {
                DestinationUID duid = null;
                try {
                    duid = DestinationUID.getUID(name, type);
                } catch (BrokerException ex) {
                    // should never happen, destination should already be verified
                    return olddata;
                }
                Destination d = Destination.getDestination(duid);
                return RaptorProtocol.generateRemDestinationRecord(d);
            }
        }

        Globals.getLogger().log(Logger.INFO,
            "Internal error : upgradeConfigChangeRecord conversion failed.");

        return olddata;
    }

}

/**
 * Encapsulates a config server backup record.
 *
 * TBD : Note - there is some duplicate code here. The normal event
 * processing methods (receiveDestinationUpdate and
 * receiveInterestUpdate) also do the same thing - i.e. read and parse
 * the event data. They should be merged so that the event data gets
 * parsed by common methods.
 */
class ChangeRecord {
    protected byte[] rec;
    protected int eventDestId;
    protected boolean discard = false;

    protected static ChangeRecord makeChangeRecord(byte[] rec)
        throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(rec);
        DataInputStream dis = new DataInputStream(bis);

        int destid = dis.readInt();
        ChangeRecord cr = null;

        dis.mark(rec.length);

        if (destid == ClusterGlobals.MB_INTEREST_UPDATE)
            cr = new InterestUpdateChangeRecord(dis);
        else if (destid == ClusterGlobals.MB_DESTINATION_UPDATE)
            cr = new DestinationUpdateChangeRecord(dis);
        else if (destid == ClusterGlobals.MB_RESET_PERSISTENCE)
            cr = new ChangeRecord();

        dis.reset(); // jump back in the input stream.

        cr.rec = new byte[rec.length - 4];
        dis.readFully(cr.rec, 0, cr.rec.length);

        cr.eventDestId = destid;
        cr.discard = false;
        return cr;
    }

    protected String getUniqueKey() {
        return "???";
    }

    protected boolean isAddOp() {
        return false;
    }

    public String toString() {
        return getUniqueKey() + ", isAddOp() = " + isAddOp();
    }
}

class DestinationUpdateChangeRecord extends ChangeRecord {
    protected String name;
    protected int type;
    protected int operation;

    protected DestinationUpdateChangeRecord(DataInputStream dis)
        throws IOException {
        int version = dis.readInt();

        operation = dis.readInt();
        name = dis.readUTF();
        type = dis.readInt();
    }

    protected String getUniqueKey() {
        return name + ":" + type + ":" + eventDestId;
    }

    protected boolean isAddOp() {
        return (operation == ClusterGlobals.MB_NEW_DESTINATION);
    }
}

class InterestUpdateChangeRecord extends ChangeRecord {
    protected String dname;
    protected String cid;
    protected int operation;

    protected InterestUpdateChangeRecord(DataInputStream dis)
        throws IOException {
        int version = dis.readInt();
        operation = dis.readInt();

        int count = dis.readInt(); // must be 1

        if (operation == ClusterGlobals.MB_NEW_INTEREST) {
            if (dis.readBoolean()) { // is durable?
                dname = dis.readUTF(); // durable name
                cid = dis.readUTF(); // client id
            }
        }
        else if (operation == ClusterGlobals.MB_REM_DURABLE_INTEREST) {
            dname = dis.readUTF(); // durable name
            cid = dis.readUTF(); // client id
        }
    }

    protected String getUniqueKey() {
        return dname + ":" + cid + ":" + eventDestId;
    }

    protected boolean isAddOp() {
        return (operation == ClusterGlobals.MB_NEW_INTEREST);
    }
}

/*
 * EOF
 */
