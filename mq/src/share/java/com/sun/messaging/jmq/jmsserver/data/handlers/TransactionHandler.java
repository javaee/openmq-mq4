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
 * @(#)TransactionHandler.java	1.148 11/16/07
 */ 

package com.sun.messaging.jmq.jmsserver.data.handlers;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;

import javax.transaction.xa.XAResource;

import com.sun.messaging.jmq.io.JMQByteBufferInputStream;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.PacketType;
import com.sun.messaging.jmq.io.Status;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.jmsserver.BrokerStateHandler;
import com.sun.messaging.jmq.jmsserver.FaultInjection;
import com.sun.messaging.jmq.jmsserver.GlobalProperties;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;
import com.sun.messaging.jmq.jmsserver.core.ClusterBroadcast;
import com.sun.messaging.jmq.jmsserver.core.Consumer;
import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;
import com.sun.messaging.jmq.jmsserver.core.Session;
import com.sun.messaging.jmq.jmsserver.core.SessionUID;
import com.sun.messaging.jmq.jmsserver.data.AutoRollbackType;
import com.sun.messaging.jmq.jmsserver.data.BaseTransaction;
import com.sun.messaging.jmq.jmsserver.data.ClusterTransaction;
import com.sun.messaging.jmq.jmsserver.data.LocalTransaction;
import com.sun.messaging.jmq.jmsserver.data.PacketHandler;
import com.sun.messaging.jmq.jmsserver.data.RollbackReason;
import com.sun.messaging.jmq.jmsserver.data.TransactionBroker;
import com.sun.messaging.jmq.jmsserver.data.TransactionList;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.data.TransactionWork;
import com.sun.messaging.jmq.jmsserver.data.TransactionWorkMessage;
import com.sun.messaging.jmq.jmsserver.data.TransactionWorkMessageAck;
import com.sun.messaging.jmq.jmsserver.management.agent.Agent;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQBasicConnection;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.jmsserver.util.AckEntryNotFoundException;
import com.sun.messaging.jmq.jmsserver.util.BrokerDownException;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.util.lists.RemoveReason;
import com.sun.messaging.jmq.util.CacheHashMap;
import com.sun.messaging.jmq.util.GoodbyeReason;
import com.sun.messaging.jmq.util.JMQXid;
import com.sun.messaging.jmq.util.UID;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.util.selector.SelectorFormatException;
import com.sun.messaging.jmq.util.txnlog.TransactionLogType;


/**
 * Handler class for transaction packets.
 */
public class TransactionHandler extends PacketHandler 
{
    private static boolean DEBUG = false;

    private static boolean DEBUG_CLUSTER_TXN = 
        Globals.getConfig().getBooleanProperty(
            Globals.IMQ + ".cluster.debug.txn") || DEBUG;

    private TransactionList translist = null;

    FaultInjection fi = null;

    public TransactionHandler(TransactionList list)
    {
        fi = FaultInjection.getInjection();
        this.translist = list;
    }

    public TransactionList getTransactionList() {
        return translist;
    }

    public void sendReply(IMQConnection con, Packet msg, 
                          int type, int status, long tid, String reason) {
        sendReply(con, msg, type, status, tid, reason, null);
    }

    public void sendReply(IMQConnection con, Packet msg, int type, 
            int status, long tid, String reason, BrokerException bex)
    {
    	sendReply(con, msg, type, status, tid, reason, bex, 0);
    }

    public void sendReply(IMQConnection con, Packet msg, int type, 
                int status, long tid, String reason, BrokerException bex, long nextTid)
    {
         if (fi.FAULT_INJECTION)
            checkFIAfterProcess(msg.getPacketType());
          Packet pkt = new Packet(con.useDirectBuffers());
          pkt.setPacketType(type);
          pkt.setConsumerID(msg.getConsumerID());
          Hashtable hash = new Hashtable();
          hash.put("JMQStatus", new Integer(status));
          if (reason != null)
              hash.put("JMQReason", reason);
          if (tid != 0) {
            hash.put("JMQTransactionID", new Long(tid));
          }
          if (bex != null && bex.isRemote()) {
            hash.put("JMQRemote", Boolean.valueOf(true));
            if (bex.getRemoteConsumerUIDs() != null) {
            hash.put("JMQRemoteConsumerIDs", bex.getRemoteConsumerUIDs());
            }
          }
          if(nextTid!=0)
        	  hash.put("JMQNextTransactionID", new Long(nextTid));

         
          pkt.setProperties ( hash );
	      con.sendControlMessage(pkt);
          if (fi.FAULT_INJECTION)
            checkFIAfterReply(msg.getPacketType());
    }

    /**
     * Send a reply packet with a body and properties
     */
    public void sendReplyBody(IMQConnection con, Packet msg, int type, int status, Hashtable hash, byte[] body)
    {
          Packet pkt = new Packet(con.useDirectBuffers());
          pkt.setPacketType(type);
          pkt.setConsumerID(msg.getConsumerID());

          if (hash == null) {
            hash = new Hashtable();
          }
          hash.put("JMQStatus", new Integer(status));
          if (((IMQBasicConnection)con).getDumpPacket() ||
                ((IMQBasicConnection)con).getDumpOutPacket()) 
              hash.put("JMQReqID", msg.getSysMessageID().toString());

          pkt.setProperties ( hash );

          if (body != null) {
            pkt.setMessageBody(body);
          }
	  con.sendControlMessage(pkt);
    }

    /**
     * Get the value of the JMQTransactionID property. For 2.0 clients
     * the property is an Integer. For Falcon it is a Long
     */
    public long getJMQTransactionID(Hashtable props) {
        if (props != null) {
            Object obj = props.get("JMQTransactionID");
            if (obj != null && obj instanceof Integer) {
                // old protocol
                return ((Integer)obj).intValue();
            } else if (obj != null) {
                // new protocol
                return ((Long)obj).longValue();
            }
        }
        return 0;
    }

    /**
     * Convert the transaction ID on an iMQ 2.0 packet to the new style.
     */
    public static void convertPacketTid(IMQConnection con, Packet p) {
        long messagetid = p.getTransactionID();
        HashMap tidMap =
            (HashMap)con.getClientData(IMQConnection.TRANSACTION_IDMAP);

        if (tidMap == null) {
            // No transactions have been started yet. Can't convert ID
            return;
        }

        // Lookup old style ID in table
        TransactionUID id = (TransactionUID)tidMap.get(new Long(messagetid));
        if (id == null) {
            return;
        }

        // Convert the old ID to the corresponding new ID
        p.setTransactionID(id.longValue());
    }


    /**
     * Method to handle Transaction Messages
     */
    public boolean handle(IMQConnection con, Packet msg) 
        throws BrokerException
    {

        long    messagetid = 0;
        TransactionUID id = null;
        TransactionState ts = null;
        JMQXid xid = null;
        Integer xaFlags = null;
         
        boolean redeliverMsgs = false;
        
        boolean startNextTransaction = false;
        
        boolean setRedeliverFlag = true;
        boolean isIndemp = msg.getIndempotent();
        boolean replay = false;
        boolean jmqonephase = false;
        Boolean jmqonephaseFlag = null;

        Hashtable props = null;
        String reason = null;

        try {
            props = msg.getProperties();
            if (props == null)
                props = new Hashtable();
        } catch (Exception ex) {
            logger.log(Logger.INFO,"Internal Error: unable to retrieve "+
                " properties from transaction message " + msg, ex);
            props = new Hashtable();

        }
        
        // performance optimisation:
        // start a new transaction immediately after commit or rollback and return 
        // transactionID in message ack.
        // The client then does not need to make a separate call to startTransaction.
        
        Boolean startNextTransactionBool = (Boolean)props.get("JMQStartNextTransaction");
        startNextTransaction = (startNextTransactionBool == null ? false : 
        	startNextTransactionBool.booleanValue());        
        
        Boolean redeliverMsgBool = (Boolean)props.get("JMQRedeliver");
        redeliverMsgs = (redeliverMsgBool == null ? false : 
                      redeliverMsgBool.booleanValue());
   
        Boolean redeliverFlag = (Boolean)props.get("JMQSetRedelivered");
        setRedeliverFlag = (redeliverFlag == null ? true :
                         redeliverFlag.booleanValue());
        jmqonephaseFlag = (Boolean)props.get("JMQXAOnePhase");
        jmqonephase = (jmqonephaseFlag == null ? false:jmqonephaseFlag.booleanValue());

        if (DEBUG) {
            logger.log(Logger.DEBUG, PacketType.getString(msg.getPacketType())+": "+
                       "TUID=" + id + ", JMQRedeliver=" + redeliverMsgBool+
                       (jmqonephaseFlag == null ? "":", JMQXAOnePhase="+jmqonephase));
        }

        List conlist = (List)con.getClientData(IMQConnection.TRANSACTION_LIST);
        if (conlist == null) {
            conlist = new ArrayList();
             con.addClientData(IMQConnection.TRANSACTION_LIST, conlist);
        }

        // If there is a message body, then it should contain an Xid.
        ByteBuffer body = msg.getMessageBodyByteBuffer();
        if (body != null) {
            JMQByteBufferInputStream  bbis = new JMQByteBufferInputStream(body);
            try {
                xid = JMQXid.read(new DataInputStream(bbis));
                startNextTransaction=false;
            } catch (IOException e) {
                logger.log(Logger.ERROR,
                        BrokerResources.E_INTERNAL_BROKER_ERROR,
                        "Could not decode xid from packet: " +  e +
                        " Ignoring " +
                        PacketType.getString(msg.getPacketType()));
                reason = e.getMessage();
                sendReply(con, msg, msg.getPacketType() + 1,
                          Status.BAD_REQUEST, 0, reason);
                return true;
            }
        }

        // Get XAFlags. Note, not all packets will have this -- that's OK.
        if (props != null) {
            xaFlags = (Integer)props.get("JMQXAFlags");
        }

        // tidMap maps an old style transaction identifier to a TransactionUID.
        // In iMQ2.0 the transaction identifier in the packet was an int
        // generated by the client and only unique on the connection.
        // In Falcon it is a long that is unique accross the cluster.
        // So for 2.0 clients we allocate a TransactionUID and map the old
        // style identifier to it.
        //
        // Get tidMap
        HashMap tidMap = null;
        synchronized(con) {
            tidMap = (HashMap)con.getClientData(IMQConnection.TRANSACTION_IDMAP);
            if (tidMap == null) {
                tidMap = new HashMap();
                con.addClientData(IMQConnection.TRANSACTION_IDMAP, tidMap);
            }
        }

        // Go ahead and get the value of "JMQTransactionID" from the packet.
        // may not be used in all cases.
        messagetid = getJMQTransactionID(props);


        // handle initial fault injections

        if (fi.FAULT_INJECTION)
            checkFIBeforeProcess(msg.getPacketType());

        // If it is a new transaction then create a new transaction id.
        // else wrap the one specified in the packet
        if (msg.getPacketType() == PacketType.START_TRANSACTION && 
            (xaFlags == null ||
            TransactionState.isFlagSet(XAResource.TMNOFLAGS, xaFlags) )) {

            // It's a START_TRANSACTION that is not a Join, or Resume.
            // Allocate a new TransactionUID


            if (isIndemp) { // deal with indemp flag
                id = translist.getTransaction(msg.getSysMessageID().toString());
                if (id != null) {
                    replay = true;
                } else {
                    id = new TransactionUID();
                }
            } else {
                id = new TransactionUID();
            }
        } else if (msg.getPacketType() == PacketType.RECOVER_TRANSACTION) {
            if (messagetid != 0) {
                // Recovering a specific transaction.
                id = new TransactionUID(messagetid);
            }
            xid = null;
        } else {
            // Operation on an existing transaction
            // Get TransactionUID out of packet
            // If only Xid was specified need to lookup TransactionUID
            if (messagetid == 0 && xid != null) {
                id = translist.xidToUID(xid);

                if (id != null) {
                    messagetid = id.longValue();
                } else {
                    // Hmmm...haven't seen this Xid before.
                    // XXX I18N
                    logger.log(Logger.WARNING,
                        PacketType.getString(msg.getPacketType()) +
                        ": Ignoring unknown XID=" + xid +
                         " broker will " 
                         + (msg.getSendAcknowledge() ? 
                               "notify the client" :
                               " not notify the client" ));
                    if (msg.getSendAcknowledge()) {
                        reason = "Uknown XID " + xid;
                        sendReply(con, msg, msg.getPacketType() + 1,
                            Status.NOT_FOUND, 0, reason);
                    }
                    return true;
                }
            } else if (messagetid != 0) {
                if (con.getClientProtocolVersion() == PacketType.VERSION1) {
                    // Map old style to new
                    synchronized(tidMap) {
                        id = (TransactionUID)tidMap.get(new Long(messagetid));
                    }
                } else {
                    // Wrap new style
                    id = new TransactionUID(messagetid);
                }
            }

            // Get the state of the transaction
            if (id == null) {
                logger.log(Logger.INFO,"InternalError: "
                         + "Transaction ID was not passed by "
                         +"the jms api on a method that reqires an "
                         + "existing transaction ");
                sendReply(con, msg, msg.getPacketType() + 1,
                          Status.ERROR, 0, "Internal Error: bad MQ protocol,"
                               + " missing TransactionID");
                return true;

                
            } else {
                ts = translist.retrieveState(id);
            }

            if (ts == null) {
                // Check if we've recently operated on this transaction

                if (isIndemp && ( msg.getPacketType() == 
                          PacketType.ROLLBACK_TRANSACTION ||
                       msg.getPacketType() == 
                          PacketType.COMMIT_TRANSACTION  )) {
                    if (msg.getSendAcknowledge()) {
                        sendReply(con, msg, msg.getPacketType() + 1,
                            Status.OK, id.longValue(), reason);
                        return true;
                    } else {
                        if (fi.FAULT_INJECTION) {
                            checkFIAfterProcess(msg.getPacketType());
                            checkFIAfterReply(msg.getPacketType());
                        }
                    }

                } else {
                    ts = cacheGetState(id, con);
                    if (ts != null) {
                        // XXX l10n
                        logger.log(Logger.ERROR,
                            "Transaction ID " + id +
                            " has already been resolved. Ignoring request: " +
                            PacketType.getString(msg.getPacketType()) + 
                            ". Last state of this transaction: " + 
                            ts.toString() +
                             " broker will " 
                             + (msg.getSendAcknowledge() ? 
                                   "notify the client" :
                                  " not notify the client" ));
                    } else {
                    logger.log((BrokerStateHandler.shuttingDown? Logger.DEBUG : Logger.WARNING),
                               Globals.getBrokerResources().getKString(
                               (msg.getSendAcknowledge() ? BrokerResources.W_UNKNOWN_TRANSACTIONID_NOTIFY_CLIENT :
                                                           BrokerResources.W_UNKNOWN_TRANSACTIONID_NONOTIFY_CLIENT),
                               ""+id+ "(" + messagetid + ")"+ (xid == null ? "":"XID="+xid),
                               PacketType.getString(msg.getPacketType())) + "\n" +
                               com.sun.messaging.jmq.jmsserver.util.PacketUtil.dumpPacket(msg));
                    }

                    // Only send reply if A bit is set
                    if (msg.getSendAcknowledge()) {
                        reason = "Unknown transaction " + id;
                        sendReply(con, msg, msg.getPacketType() + 1,
                        Status.NOT_FOUND, id.longValue(), reason);
                    }
                    return true;
                }
            }
        }


        if (DEBUG) {
            logger.log(Logger.INFO, this.getClass().getName() + ": " +
                PacketType.getString(msg.getPacketType()) + ": " +
                "TUID=" + id +
                " XAFLAGS=" + TransactionState.xaFlagToString(xaFlags) +
                (jmqonephaseFlag == null ? "":" JMQXAOnePhase="+jmqonephase)+
                " State=" + ts + " Xid=" + xid);
        }

        // If a packet came with an Xid, make sure it matches what
        // we have in the transaction table.
        if (xid != null && ts != null) {
            if (ts.getXid() == null || !xid.equals(ts.getXid())) {
                // This should never happen
                logger.log(Logger.ERROR,
                        BrokerResources.E_INTERNAL_BROKER_ERROR,
                        "Transaction Xid mismatch. " +
                        PacketType.getString(msg.getPacketType()) +
                        " Packet has tuid=" +
                        id + " xid=" + xid + ", transaction table has tuid=" +
                        id + " xid=" + ts.getXid() +
                        ". Using values from table.");
                xid = ts.getXid();
            }
        }

        if (xid == null && ts != null && ts.getXid() != null &&
            msg.getPacketType() != PacketType.RECOVER_TRANSACTION) {
            // Client forgot to put Xid in packet.
            xid = ts.getXid();
            logger.log(Logger.WARNING,
                        BrokerResources.E_INTERNAL_BROKER_ERROR,
                        "Transaction Xid "+xid+" not found in " +
                        PacketType.getString(msg.getPacketType()) +
                        " packet for tuid " +
                        id + ". Will use " + xid);
        }


        int status = Status.OK; 

        // retrieve new 4.0 properties
        AutoRollbackType type = null;
        long lifetime = 0;
        boolean sessionLess = false;
        Integer typeValue = (Integer)props.get("JMQAutoRollback");
        Long lifetimeValue = (Long)props.get("JMQLifetime");
        Boolean sessionLessValue = (Boolean)props.get("JMQSessionLess");

        if (typeValue != null) {
            type = AutoRollbackType.getType(typeValue.intValue());
        }

        if (lifetimeValue != null) {
            lifetime = lifetimeValue.longValue();
        }

        if (sessionLessValue != null) {
            sessionLess = sessionLessValue.booleanValue();
        } else {
            sessionLess = xid != null;
        }


        switch (msg.getPacketType())  {
            case PacketType.START_TRANSACTION:
            {
                try {
                    SessionUID suid = null;
                    Long sessionID = (Long)props.get("JMQSessionID");
                    if (sessionID != null) {
                        suid = new SessionUID(sessionID.longValue());
                    }
                    doStart(id, conlist, con, 
                        type, xid, sessionLess, lifetime, messagetid,
                        xaFlags, msg.getPacketType(), replay, msg.getSysMessageID().toString());
                } catch (Exception ex) {
                    status = Status.ERROR;
                    logger.logStack(Logger.ERROR,
                           BrokerResources.E_INTERNAL_BROKER_ERROR,
                           ex.toString() + ": TUID=" + id + " Xid=" + xid, ex);
                    reason = ex.getMessage();
                    if (ex instanceof BrokerException) {
                        status = ((BrokerException)ex).getStatusCode();
                    }
                }
                sendReply(con, msg, PacketType.START_TRANSACTION_REPLY,
                            status, id.longValue(), reason);
                break;
            }

            case PacketType.END_TRANSACTION:
                try {
                    doEnd(msg.getPacketType(), xid, xaFlags, ts, id);
                } catch (Exception ex) {
                    status = Status.ERROR;
                    reason = ex.getMessage();
                    if (ex instanceof BrokerException) status = ((BrokerException)ex).getStatusCode();
                }
                sendReply(con, msg,  msg.getPacketType() + 1, status, id.longValue(), reason);
                break;
            case PacketType.PREPARE_TRANSACTION:
                BrokerException bex = null;
                try {
                    doPrepare(id, xaFlags, ts, msg.getPacketType(), jmqonephase, null);
                } catch (Exception ex) {
                    status = Status.ERROR;
                    if ((!(ex instanceof BrokerDownException) && 
                         !(ex instanceof AckEntryNotFoundException)) || DEBUG_CLUSTER_TXN) {
                    logger.logStack(Logger.ERROR,
                            ex.toString() + ": TUID=" + id + " Xid=" + xid, ex);
                    } else {
                    logger.log(((ex instanceof AckEntryNotFoundException) ? Logger.WARNING:Logger.ERROR),
                               ex.toString() + ": TUID=" + id + " Xid=" + xid);
                    }
                    reason = ex.getMessage();
                    if (ex instanceof BrokerException) {
                        status = ((BrokerException)ex).getStatusCode();
                        bex = (BrokerException)ex;
                    }
                }
                sendReply(con, msg,  msg.getPacketType() + 1, status, 
                          id.longValue(), reason, bex);
                break;
            case PacketType.RECOVER_TRANSACTION:

                Vector v = null;

                if (id != null) {
                    // Check if specified transaction is in PREPARED state
                    v = new Vector();
                    ts = translist.retrieveState(id);
                    if (ts.getState() == TransactionState.PREPARED) {
                        v.add(id);
                    }
                } else {
                    // XA Transaction only. We return all Xids on a STARTRSCAN
                    // and nothing on ENDRSCAN or NOFLAGS
                    if (xaFlags == null ||
                        !TransactionState.isFlagSet(XAResource.TMSTARTRSCAN,
                                                                    xaFlags)) {
                        Hashtable hash = new Hashtable();
                        hash.put("JMQQuantity", new Integer(0));
    
                        sendReplyBody(con, msg,
                            PacketType.RECOVER_TRANSACTION_REPLY,
                            Status.OK, hash, null);
                        break;
                    }
                    // Get list of transactions in PENDING state and marshal
                    // the Xid's to a byte array.
                    v = translist.getTransactions(TransactionState.PREPARED);
                }

                int nIDs = v.size();
                int nWritten = 0;
                ByteArrayOutputStream bos =
                            new ByteArrayOutputStream(nIDs * JMQXid.size());
                DataOutputStream dos = new DataOutputStream(bos);
                for (int n = 0; n < nIDs; n++) {
                    TransactionUID tuid = (TransactionUID)v.get(n);
                    TransactionState _ts = translist.retrieveState(tuid);
                    if (_ts == null) {
                        // Should never happen
                        logger.log(Logger.ERROR,
                                BrokerResources.E_INTERNAL_BROKER_ERROR,
                                "Could not find state for TUID " + tuid);
                        continue;
                    }
                    JMQXid _xid = _ts.getXid();
                    if (_xid != null) {
                        try {
                            _xid.write(dos);
                            nWritten++;
                        } catch (Exception e) {
                            logger.log(Logger.ERROR,
                                BrokerResources.E_INTERNAL_BROKER_ERROR,
                                "Could not write Xid " +
                                _xid + " to message body: " +
                                e.toString());
                        }
                    }
                }

                Hashtable hash = new Hashtable();
                hash.put("JMQQuantity", new Integer(nWritten));

                if (id != null) {
                    hash.put("JMQTransactionID", new Long(id.longValue()));
                }

                // Send reply with serialized Xids as the body
                sendReplyBody(con, msg, PacketType.RECOVER_TRANSACTION_REPLY,
                    Status.OK, hash, bos.toByteArray());
                break;


            case PacketType.COMMIT_TRANSACTION:

                //XXX-LKS clean up any destination locks ???
                  

                try {
                    // doCommit will send reply if successful
                    if (xaFlags != null && jmqonephase) {
                        Integer newxaFlags = new Integer(xaFlags.intValue() & ~XAResource.TMONEPHASE);
                        doCommit(id, xid, newxaFlags, ts, conlist, true, con, msg);
                    } else {
                        doCommit(id, xid, xaFlags, ts, conlist, true, con, msg, startNextTransaction);
                    }
                } catch (BrokerException ex) {
                    // doCommit has already logged error
                    status = ex.getStatusCode();
                    reason = ex.getMessage();
                    if (msg.getSendAcknowledge()) {
                        sendReply(con, msg,  msg.getPacketType() + 1, status, 
                                  id.longValue(), reason, ex);
                    } else {
                        if (fi.FAULT_INJECTION) {
                            checkFIAfterProcess(msg.getPacketType());
                            checkFIAfterReply(msg.getPacketType());
                        }
                    }

                }
                break;
                
            case PacketType.ROLLBACK_TRANSACTION:
            {
                try {
                    preRollback(id, xid, xaFlags, ts);

                try {
                    // if redeliverMsgs is true, we want to redeliver
                    // to both active and inactive consumers 
                    boolean processActiveConsumers = redeliverMsgs;
                    redeliverUnacked(id, processActiveConsumers, setRedeliverFlag);
                } catch (BrokerException ex) {
                    logger.logStack(Logger.ERROR,
                        BrokerResources.E_INTERNAL_BROKER_ERROR,
                        "REDELIVER: " + 
                        ex.toString() + ": TUID=" + id + " Xid=" + xid, ex);
                    reason = ex.getMessage();
                    status = ex.getStatusCode();
                }
                try {
                    doRollback(id, xid, xaFlags, ts, conlist,
                               con, RollbackReason.APPLICATION);
                } catch (BrokerException ex) {
                    // doRollback has already logged error
                    reason = ex.getMessage();
                    status = ex.getStatusCode();
                }

                } catch (BrokerException ex) {
                    // preRollback has already logged error
                    reason = ex.getMessage();
                    status = ex.getStatusCode();
                }
                
                // performance optimisation
                // start next transaction and return transaction id
                long nextTxnID = 0;
                if(startNextTransaction)
                {
                	try {                        
                        TransactionUID nextid = new TransactionUID();
                        doStart(nextid, conlist, con, 
                            type, xid, sessionLess, lifetime, 0,
                            xaFlags, PacketType.START_TRANSACTION, replay, msg.getSysMessageID().toString());
                        nextTxnID = nextid.longValue();
                    } catch (Exception ex) {
                        status = Status.ERROR;
                        logger.logStack(Logger.ERROR,
                               BrokerResources.E_INTERNAL_BROKER_ERROR,
                               ex.toString() + ": TUID=" + id + " Xid=" + xid, ex);
                        reason = ex.getMessage();
                        if (ex instanceof BrokerException) {
                            status = ((BrokerException)ex).getStatusCode();
                        }
                    }	
                }
                
                if (msg.getSendAcknowledge()) {
                    sendReply(con, msg,  msg.getPacketType() + 1, status,
                        id.longValue(), reason, null, nextTxnID);
                } else {
                    if (fi.FAULT_INJECTION) {
                        checkFIAfterProcess(msg.getPacketType());
                        checkFIAfterReply(msg.getPacketType());
                    }
                }
                break;
            }
        }

        return true;
    }

    public void checkFIBeforeProcess(int type)
    {
        switch (type) {
            case PacketType.START_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_START_1,
                      null, 2, false);
                break;
            case PacketType.END_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_END_1,
                      null, 2, false);
                break;
            case PacketType.PREPARE_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_PREPARE_1,
                      null, 2, false);
                break;
            case PacketType.ROLLBACK_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_ROLLBACK_1,
                      null, 2, false);
                break;
            case PacketType.COMMIT_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_COMMIT_1,
                      null, 2, false);
                break;
        }
    }

    public void checkFIAfterProcess(int type)
    {
        switch (type) {
            case PacketType.START_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_START_2,
                      null, 2, false);
                break;
            case PacketType.END_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_END_2,
                      null, 2, false);
                break;
            case PacketType.PREPARE_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_PREPARE_2,
                      null, 2, false);
                break;
            case PacketType.ROLLBACK_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_ROLLBACK_2,
                      null, 2, false);
                break;
            case PacketType.COMMIT_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_COMMIT_2,
                      null, 2, false);
                break;
        }
    }

    public void checkFIAfterDB(int type)
    {
        switch (type) {
            case PacketType.ROLLBACK_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_ROLLBACK_4,
                      null, 2, false);
                break;
            case PacketType.COMMIT_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_COMMIT_4,
                      null, 2, false);
                break;
        }
    }

    public void checkFIAfterReply(int type)
    {
        switch (type) {
            case PacketType.START_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_START_3,
                      null, 2, false);
                break;
            case PacketType.END_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_END_3,
                      null, 2, false);
                break;
            case PacketType.PREPARE_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_PREPARE_3,
                      null, 2, false);
                break;
            case PacketType.ROLLBACK_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_ROLLBACK_3,
                      null, 2, false);
                break;
            case PacketType.COMMIT_TRANSACTION:
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_COMMIT_3,
                      null, 2, false);
                break;
        }
    }

    /**
     * We cache the last N transactions to be committed or rolledback.
     * This is simply to let us more easily debug problems when the
     * client sends a bogus transaction ID.
     *
     * To improve performance, this feature should be disabled by setting
     * the "imq.transaction.debug" property to false. For now, it will be
     * disabled by default to improve direct mode benchmark performance.
     */
    private void cacheSetState(TransactionUID id,
                            TransactionState ts,
                            IMQConnection con) {

        // Do this only if transaction debug is enabled!
        if (GlobalProperties.getGlobalProperties().TRANSACTION_DEBUG) {
            if (con == null) return;

            CacheHashMap cache =(CacheHashMap)con.getClientData(
                IMQConnection.TRANSACTION_CACHE);
            if (cache == null) {
                cache = new CacheHashMap(4);
                con.addClientData(IMQConnection.TRANSACTION_CACHE, cache);
            }
            cache.put(id, ts);
        }
    }

    private TransactionState cacheGetState(TransactionUID id,
                                           IMQConnection con) {

        TransactionState ts = null;

        // Do this only if transaction debug is enabled!
        if (GlobalProperties.getGlobalProperties().TRANSACTION_DEBUG) {
            CacheHashMap cache = (CacheHashMap)con.getClientData(
                IMQConnection.TRANSACTION_CACHE);
            if (cache != null) {
               ts = (TransactionState)cache.get(id);
            }
        }

        return ts;
    }

    public void doCommit(TransactionUID id, JMQXid xid,
            Integer xaFlags, TransactionState ts, List conlist,
            boolean sendReply, IMQConnection con, Packet msg)
            throws BrokerException {
    	
    	doCommit(id,xid,xaFlags,ts,conlist,sendReply,con,msg,false);
    }
    

    /**
     * Commit a transaction. This method is invoked from two places:
     * 1) From TransactionHandler.handle() when handling a client
     *    COMMIT packet. This is the common case.
     * 2) From the admin handler when an admin commit request has been
     *    issued on a PREPARED XA transaction.
     *
     * @param id  The TransactionUID to commit
     * @param xid The Xid of the transaction to commit. Required if transaction
     *            is an XA transaction. Must be null if it is not an XA
     *            transaction.
     * @param xaFlags  xaFlags passed on COMMIT operation. Used only if
     *                 an XA transaction.
     * @param ts       Current TransactionState of this transaction.
     * @param conlist  List of transactions on this connection. Will be null
     *                 if commit is trigger by an admin request.
     * @param sendReply True to have method send a Status.OK reply while
     *                  processing transaction. This should be "true" for
     *                  client initiated commits, "false" for admin
     *                  initiated commits.
     * @param con       Connection client commit packet came in on or, for
     *                  admin, the connection the admin request came in on.
     * @param msg       Client commit packet. Should be "null" for admin
     *                  initiated commits.
     *
     * @throws BrokerException on an error. The method will have logged a
     *         message to the broker log.
     */
    public void doCommit(TransactionUID id, JMQXid xid,
                          Integer xaFlags, TransactionState ts, List conlist,
                          boolean sendReply, IMQConnection con, Packet msg,
                          boolean startNextTransaction)
                          throws BrokerException {

        int status = Status.OK;
        HashMap cmap = null;
        HashMap sToCmap = null;
        BrokerException bex = null;
        List plist = null;
        
        int transactionType=BaseTransaction.UNDEFINED_TRANSACTION_TYPE; //local, or cluster

        // let acks get handled at a lower level since the
        // lower level methods assumes only 1 ack per message
        plist = translist.retrieveSentMessages(id);
        cmap = translist.retrieveConsumedMessages(id);
        sToCmap = translist.retrieveStoredConsumerUIDs(id);
        cacheSetState(id, ts, con);

        // remove from our active connection list
        if (conlist != null) {
            conlist.remove(id);
        }

        
     // new performance enhancement 
     // log transactions before we store transaction work in message store
     // and before remote commit        
             
             

             try{
             Globals.getStore().txnLogSharedLock.lock();
             TransactionWork txnWork=null;
             if(Globals.isNewTxnLogEnabled())
             {                    	
                txnWork = getTransactionWork2(plist, cmap, sToCmap);          
             }
                     
        
        // Update transaction state
        try {
            int s;
            if (xid == null) {
                // Plain JMS transaction.
                s = TransactionState.COMMITTED;
            } else {
                // XA Transaction. 
                s = ts.nextState(PacketType.COMMIT_TRANSACTION, xaFlags);
            }

        //  After this call, returned base transaction will either be:
            // a) null (for single phase LOCAL transaction)
            // b) a prepared XA LOCAL transaction 
            // c) a prepared (XA or not) CLUSTER transaction
            
            // currently, all cluster transactions are 2 phase
            BaseTransaction baseTransaction = doRemoteCommit(id, xaFlags, ts, s, msg, txnWork);
            if (Globals.isNewTxnLogEnabled()) {
					if (ts.getState() == TransactionState.PREPARED) {
						// commit called (from client) on 2-phase transaction
						// 
			
						transactionType = BaseTransaction.LOCAL_TRANSACTION_TYPE;
						if (translist.isClusterTransaction(id)) {
							transactionType = BaseTransaction.CLUSTER_TRANSACTION_TYPE;
						}
						logTxnCompletion(id, TransactionState.COMMITTED,
								transactionType);
					} else if ((baseTransaction != null && baseTransaction
							.getState() == TransactionState.PREPARED)) {
						transactionType = baseTransaction.getType();
						logTxnCompletion(id, TransactionState.COMMITTED,
								transactionType);
					} else {

						// one phase commit 
						// log all work here 
						transactionType = BaseTransaction.LOCAL_TRANSACTION_TYPE;
						LocalTransaction localTxn = new LocalTransaction(id,
								TransactionState.COMMITTED, xid, txnWork);
						logTxn(localTxn);
					}
				}
            else {
            	//System.out.println("isFastLogTransactions=false ");
            	}

            if (fi.FAULT_INJECTION) {
                fi.checkFaultAndThrowBrokerException(
                   FaultInjection.FAULT_TXN_COMMIT_1_1, null);
            }
            ts = translist.updateState(id, s, true);
            if (fi.FAULT_INJECTION) {
                checkFIAfterDB(PacketType.COMMIT_TRANSACTION);
                fi.checkFaultAndExit(FaultInjection.FAULT_TXN_COMMIT_1_5, 
                                     null, 2, false);
            }
            
            startTxnAndSendReply(con, msg, status, startNextTransaction,
					conlist, xid, id, xaFlags, sendReply);                                 


        } catch (BrokerException ex) {
            logger.logStack(((ex instanceof AckEntryNotFoundException) ? Logger.WARNING:Logger.ERROR),
                            ex.toString() + ": TUID=" + id + " Xid=" + xid, ex);
            throw ex;
        }
        try {
            /*
	         * Can't really call the JMX notification code at the end of doCommit()
	         * because the call to translist.removeTransactionID(id) removes the MBean.
	         */
	        Agent agent = Globals.getAgent();
	        if (agent != null)  {
		        agent.notifyTransactionCommit(id);
	        }
        } catch (Exception e) {
            logger.log(Logger.WARNING, "JMX agent notify transaction committed failed:"+e.getMessage());
        }

        // OK .. handle producer transaction
        int pLogRecordByteCount = 0;
        ArrayList pLogMsgList = null;
        for (int i =0; plist != null && i < plist.size(); i ++ ) {
            SysMessageID sysid = (SysMessageID)plist.get(i);
            PacketReference ref = Destination.get(sysid);
            if (ref == null) {
                logger.log(Logger.ERROR,BrokerResources.E_INTERNAL_BROKER_ERROR,  "transacted message removed too early "+sysid);
                continue;
            }
                     
            // handle forwarding the message
            try {
                if (Globals.txnLogEnabled()) {
                    if (pLogMsgList == null) {
                        pLogMsgList = new ArrayList();
                    }

                    // keep track for producer txn log
                    pLogRecordByteCount += ref.getSize();
                    pLogMsgList.add(ref.getPacket().getBytes());
                }

                Destination d = Destination.getDestination(ref.getDestinationUID());
                if (fi.FAULT_INJECTION) {
                    fi.checkFaultAndExit(FaultInjection.FAULT_TXN_COMMIT_1_6, null, 2, false);
                }
                Set s = d.routeNewMessage(ref);
                d.forwardMessage(s,ref);

             } catch (Exception ex) {
                 logger.logStack((BrokerStateHandler.shuttingDown? Logger.DEBUG : Logger.ERROR),BrokerResources.E_INTERNAL_BROKER_ERROR, "unable to route/send transaction message " + sysid , ex);
             }

        }

        boolean processDone = true;

        // handle consumer transaction
        int cLogRecordCount = 0;
        ArrayList cLogDstList = null;
        ArrayList cLogMsgList = null;
        ArrayList cLogIntList = null;
        HashMap remoteNotified = new HashMap();
        if (cmap != null && cmap.size() > 0) {
            Iterator itr = cmap.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry entry = (Map.Entry)itr.next();
                SysMessageID sysid = (SysMessageID)entry.getKey();
                // CANT just pull from connection
                if (sysid == null) continue;
                PacketReference ref = Destination.get(sysid);
                if (ref == null || ref.isDestroyed() || ref.isInvalid()) {
                    // already been deleted .. ignore
                    continue;
                }

                Destination dst =
                    Destination.getDestination(ref.getDestinationUID());

                List interests = (List) entry.getValue();
                for (int i = 0; i < interests.size(); i ++) {
                    ConsumerUID intid = (ConsumerUID) interests.get(i);
                    ConsumerUID sid = (ConsumerUID)sToCmap.get(intid);
                    if (sid == null) sid = intid;

                    try {
                        Session s = Session.getSession(intid);
                        if (s != null) {
                            PacketReference r1 = null;
                            if (fi.FAULT_INJECTION && fi.checkFault(
                                FaultInjection.FAULT_TXN_COMMIT_1_7_1, null)) {
                                Globals.getConnectionManager().getConnection(
                                        s.getConnectionUID()).destroyConnection(
                                        true, GoodbyeReason.OTHER,
                                        "Fault injection of closing connection");
                            }
                            r1 =s.ackMessage(intid, sysid, id, remoteNotified, true);
                            if (r1 != null) {
                                if (fi.FAULT_INJECTION) {
                                    fi.checkFaultAndExit(FaultInjection.FAULT_TXN_COMMIT_1_7, null, 2, false);
                                }
                                dst.removeMessage(ref.getSysMessageID(),
                                    RemoveReason.ACKNOWLEDGED);
                            } else {
                                s = Session.getSession(intid);
                            }
                        }
                        if (s == null) {
                            //OK the session has been closed, we need
                            // to retrieve the message and acknowledge it
                            // with the stored UID
                            try {
                                if (ref.acknowledged(intid, sid, 
                                    true, true, id, remoteNotified, true)) {
                                    dst.removeMessage(ref.getSysMessageID(),
                                        RemoveReason.ACKNOWLEDGED);
                                }
                            } catch (BrokerException ex) {
                                // XXX improve internal error
                                logger.log(Logger.WARNING,"Internal error", ex);
                            }
                        }

                        if (Globals.txnLogEnabled()) {
                            if (cLogDstList == null) {
                                cLogDstList = new ArrayList();
                                cLogMsgList = new ArrayList();
                                cLogIntList = new ArrayList();
                            }

                            // keep track for consumer txn log;
                            // ignore non-durable subscriber
                            if (!dst.isQueue() && !sid.shouldStore()) {
                                continue;
                            }

                            cLogRecordCount++;
                            cLogDstList.add(dst.getUniqueName());
                            cLogMsgList.add(sysid);
                            cLogIntList.add(sid);
                        }
                    } catch (Exception ex) {
                        processDone = false;
                        logger.logStack(Logger.ERROR,BrokerResources.E_INTERNAL_BROKER_ERROR,
                            "-------------------------------------------" +
                            "Processing Acknowledgement during committ [" +
                            sysid + ":" + intid + ":" + con.getConnectionUID()+
                            "]\nReference is " + (ref == null ? null : ref.getSysMessageID())
                            + "\n" + com.sun.messaging.jmq.jmsserver.util.PacketUtil.dumpPacket(msg)
                            + "--------------------------------------------",
                            ex);
                    }
                }
            }
        }

        
        if(Globals.isNewTxnLogEnabled())
        {
        	// notify that transaction work has been written to message store
           loggedCommitWrittenToMessageStore(id, transactionType);
        }
               

        // OK .. now remove the acks .. and free up the id for ues
        // XXX Fixed 6383878, memory leaks because txn ack can never be removed
        // from the store if the txn is removed before the ack; this is due
        // to the fack that in 4.0 when removing the ack, the method check
        // to see if the txn still exits in the cache. This temporary fix
        // will probably break some HA functionality and need to be revisited.
        translist.removeTransaction(id, (!processDone || 
                    (cmap.size()>0 && BrokerStateHandler.shuttingDown)));
        if (conlist == null) { //from admin
            logger.log(logger.WARNING, 
            BrokerResources.W_ADMIN_COMMITTED_TXN, id, ((xid==null)? "null":xid.toString()));
        }

        // log to txn log if enabled
        try {
            if (pLogRecordByteCount > 0 && cLogRecordCount > 0) {
                // Log all msgs and acks for producing and consuming txn
                ByteArrayOutputStream bos = new ByteArrayOutputStream(
                    (pLogRecordByteCount) +
                    (cLogRecordCount * (32 + SysMessageID.ID_SIZE + 8)) + 16);
                DataOutputStream dos = new DataOutputStream(bos);

                dos.writeLong(id.longValue()); // Transaction ID (8 bytes)

                // Msgs produce section
                dos.writeInt(pLogMsgList.size()); // Number of msgs (4 bytes)
                Iterator itr = pLogMsgList.iterator();
                while (itr.hasNext()) {
                    dos.write((byte[])itr.next()); // Message
                }

                // Msgs consume section
                dos.writeInt(cLogRecordCount); // Number of acks (4 bytes)
                for (int i = 0; i < cLogRecordCount; i++) {
                    String dst = (String)cLogDstList.get(i);
                    dos.writeUTF(dst); // Destination
                    SysMessageID sysid = (SysMessageID)cLogMsgList.get(i);
                    sysid.writeID(dos); // SysMessageID
                    ConsumerUID intid = (ConsumerUID)cLogIntList.get(i);
                    dos.writeLong(intid.longValue()); // ConsumerUID
                }

                dos.close();
                bos.close();
                Globals.getStore().logTxn(
                    TransactionLogType.PRODUCE_AND_CONSUME_TRANSACTION,
                    bos.toByteArray());
            } else if (pLogRecordByteCount > 0) {
                // Log all msgs for producing txn
                ByteBuffer bbuf = ByteBuffer.allocate(pLogRecordByteCount + 12);
                bbuf.putLong(id.longValue()); // Transaction ID (8 bytes)
                bbuf.putInt(pLogMsgList.size()); // Number of msgs (4 bytes)
                Iterator itr = pLogMsgList.iterator();
                while (itr.hasNext()) {
                    bbuf.put((byte[])itr.next()); // Message
                }
                Globals.getStore().logTxn(
                    TransactionLogType.PRODUCE_TRANSACTION, bbuf.array());
            } else if (cLogRecordCount > 0) {
                // Log all acks for consuming txn
                ByteArrayOutputStream bos = new ByteArrayOutputStream(
                    (cLogRecordCount * (32 + SysMessageID.ID_SIZE + 8)) + 12);
                DataOutputStream dos = new DataOutputStream(bos);
                dos.writeLong(id.longValue()); // Transaction ID (8 bytes)
                dos.writeInt(cLogRecordCount); // Number of acks (4 bytes)
                for (int i = 0; i < cLogRecordCount; i++) {
                    String dst = (String)cLogDstList.get(i);
                    dos.writeUTF(dst); // Destination
                    SysMessageID sysid = (SysMessageID)cLogMsgList.get(i);
                    sysid.writeID(dos); // SysMessageID
                    ConsumerUID intid = (ConsumerUID)cLogIntList.get(i);
                    dos.writeLong(intid.longValue()); // ConsumerUID
                }
                dos.close();
                bos.close();
                Globals.getStore().logTxn(
                    TransactionLogType.CONSUME_TRANSACTION, bos.toByteArray());
            }
        } catch (IOException ex) {
            logger.logStack(Logger.ERROR,
                BrokerResources.E_INTERNAL_BROKER_ERROR,
                "Got exception while writing to transaction log", ex);
            throw new BrokerException(
                "Internal Error: Got exception while writing to transaction log", ex);
        }
       
        
             }
             finally
             {
             	 // release lock
             	 Globals.getStore().txnLogSharedLock.unlock();
             }
    }

             TransactionWork getTransactionWork2(List plist, HashMap cmap,
			HashMap sToCmap) {
		TransactionWork txnWork = new TransactionWork();

		// iterate over messages produced in this transaction
		// NB should we be checking for persistent messages?
		for (int i = 0; plist != null && i < plist.size(); i++) {
			SysMessageID sysid = (SysMessageID) plist.get(i);
			PacketReference ref = Destination.get(sysid);
			if (ref == null) {
				logger.log(Logger.ERROR,
						BrokerResources.E_INTERNAL_BROKER_ERROR,
						"transacted message removed too early " + sysid);
				continue;
			}
			try {
				if (ref.isPersistent()) {
					TransactionWorkMessage txnWorkMessage = new TransactionWorkMessage();
					Destination dest = ref.getDestination();
					txnWorkMessage.setDestUID(dest.getDestinationUID());
					txnWorkMessage.setPacketReference(ref);
					
					txnWork.addMesage(txnWorkMessage);
				}

			} catch (Exception ex) {
				logger.logStack((BrokerStateHandler.shuttingDown ? Logger.DEBUG
						: Logger.ERROR),
						BrokerResources.E_INTERNAL_BROKER_ERROR,
						"unable to log transaction message " + sysid, ex);
			}
		}

		// iterate over messages consumed in this transaction
		if (cmap != null && cmap.size() > 0) {

			Iterator itr = cmap.entrySet().iterator();
			while (itr.hasNext()) {
				Map.Entry entry = (Map.Entry) itr.next();

				SysMessageID sysid = (SysMessageID) entry.getKey();
				List interests = (List) entry.getValue();

				if (sysid == null)
					continue;

				PacketReference ref = Destination.get(sysid);
				if (ref == null || ref.isDestroyed() || ref.isInvalid()) {
					// already been deleted .. ignore
					continue;
				}
				
				// Do we want to store acknowledgements for remote brokers?
				// If this is a local transaction, then all acks should be local.
				// If this is a cluster transaction, then acks for remote messages 
				// will be forwarded to remote brokers as part of the protocol. 
				// The cluster txn should only need op store the addresses of the brokers involved.
				if(!ref.isLocal())
				{
					continue;
				}

				Destination dst = Destination.getDestination(ref
						.getDestinationUID());

				// A bit unlikely, but a message may have been consumed multiple
				// times in the same transaction by different consumers
				// - hence the list.
				for (int i = 0; i < interests.size(); i++) {
					ConsumerUID intid = (ConsumerUID) interests.get(i);
					ConsumerUID sid = (ConsumerUID) sToCmap.get(intid);
					if (sid == null)
						sid = intid;

					try {
						// ignore non-durable subscriber
						if (!dst.isQueue() && !sid.shouldStore()) {
							continue;
						}
						if (ref.isPersistent()) {
							TransactionWorkMessageAck ack = new TransactionWorkMessageAck();
							ack.setConsumerID(sid);
							ack.setDest(dst.getDestinationUID());
							ack.setSysMessageID(sysid);
							txnWork.addMessageAcknowledgement(ack);

						}

					} catch (Exception ex) {
						logger.logStack(Logger.ERROR,
								BrokerResources.E_INTERNAL_BROKER_ERROR,
								" unable to log transaction message acknowledgement "
										+ sysid + ":" + intid, ex);
					}
				}
			}
		}
		return txnWork;
	}

	void startTxnAndSendReply(IMQConnection con, Packet msg, int status,
			boolean startNextTransaction, List conlist, JMQXid xid,
			TransactionUID id, Integer xaFlags, boolean sendReply) {
		// performance optimisation
		long nextTxnID = 0;
		String reason = null;
		if (startNextTransaction) {
			try {
				TransactionUID nextid = new TransactionUID();
				doStart(nextid, conlist, con, AutoRollbackType.NOT_PREPARED,
						xid, false, 0, 0, xaFlags,
						PacketType.START_TRANSACTION, false, msg
								.getSysMessageID().toString());
				nextTxnID = nextid.longValue();
			} catch (Exception ex) {
				status = Status.ERROR;
				logger.logStack(Logger.ERROR,
						BrokerResources.E_INTERNAL_BROKER_ERROR, ex.toString()
								+ ": TUID=" + id + " Xid=" + xid, ex);
				reason = ex.getMessage();
				if (ex instanceof BrokerException) {
					status = ((BrokerException) ex).getStatusCode();
				}
			}
		}

		if (sendReply) { // chiaming
			sendReply(con, msg, PacketType.COMMIT_TRANSACTION_REPLY, status, id
					.longValue(), reason, null, nextTxnID);
		}
	}

	
	
	
	
	private void logTxn(BaseTransaction baseTxn) throws BrokerException {
		
		//boolean requiresLogging  = true;
			boolean requiresLogging  =calculateStoredRouting(baseTxn);
		// TODO,
		//If txn does not require logging, should stilkl manage it otherwise, we will get unknowntxn error on completion
		if (requiresLogging || baseTxn.getState()==TransactionState.PREPARED)
		{
		Globals.getStore().logTxn(baseTxn);
		}
	}
	
	private boolean calculateStoredRouting(BaseTransaction baseTxn)throws BrokerException
	{
		boolean sentMessagesNeedLogging = false;
		// only need to log if message is persistent
		TransactionWork txnWork = baseTxn.getTransactionWork();
		List<TransactionWorkMessage> sentMessages = txnWork.getSentMessages();
		if (sentMessages != null) {
			Iterator<TransactionWorkMessage> iter = sentMessages.iterator();
			while (iter.hasNext()) {
				TransactionWorkMessage twm = iter.next();
				sentMessagesNeedLogging |= calculateStoredRouting(twm);
			}
		}
		boolean ackedMessagesNeedLogging = false;
		List<TransactionWorkMessageAck> ackedMessages = txnWork
				.getMessageAcknowledgments();
		if (ackedMessages != null) {
			Iterator<TransactionWorkMessageAck> ackIter = ackedMessages
					.iterator();
			while (ackIter.hasNext()) {
				TransactionWorkMessageAck twma = ackIter.next();
				// TODO, check if ack needs logging
				ackedMessagesNeedLogging |= true;
			}
		}
		
		
		
		return ackedMessagesNeedLogging ||sentMessagesNeedLogging;
	}
	
	private boolean calculateStoredRouting(TransactionWorkMessage twm)
			throws BrokerException {
		boolean requiresLogging = false;
		PacketReference ref = twm.getPacketReference();
		
		Destination dest = Destination.getDestination(twm.getDestUID());
		ConsumerUID[] storedInterests = null;
		if(dest==null)
		{
			String msg = "Could not find destination for "+twm.getDestUID() + " refDest= "+ref.getDestinationName();
			logger.log(Logger.ERROR,msg);
			throw new BrokerException(msg);
			
		}
		try {
			storedInterests = dest.calculateStoredInterests(ref);
			twm.setStoredInterests(storedInterests);

		} catch (SelectorFormatException sfe) {
			throw new BrokerException(
					"Could not route transacted message on commit", sfe);
		}
		if (storedInterests != null)
		{
			if (DEBUG_CLUSTER_TXN)
	    	{
	    		logger.log(Logger.DEBUG, Thread.currentThread().getName() +" stored routing = null "+twm +" persist="+ref.isPersistent());
	    	}
			return true;
		}
		else{
			if (DEBUG_CLUSTER_TXN)
	    	{
				for(int i=0;i<storedInterests.length;i++)
	    	  	  logger.log(Logger.DEBUG, Thread.currentThread().getName() +" stored routing "+storedInterests[i]+" "+twm);
	    	}
		}
		return requiresLogging;
	}

	private void logTxnCompletion(TransactionUID tid, int state, int type)
			throws BrokerException {
		Globals.getStore().logTxnCompletion(tid, state, type);
	}

	private void loggedCommitWrittenToMessageStore(TransactionUID tid, int type)
			throws BrokerException {

		Globals.getStore().loggedCommitWrittenToMessageStore(tid, type);

	}
                          
    /**
     * A Tuxedo TM instance that is different from the TM instance 
     * that has the current thread control of the transaction may 
     * call xa_rollback on STARTED state when timeout the transaction
     *
	 * @param id The TransactionUID 
	 * @param xid required if XA transaction and must be null if no XA
	 * @param xaFlags used only if XA transaction.
	 * @param ts current TransactionState of this transaction
     *
     */
    public void preRollback(TransactionUID id, JMQXid xid,
                          Integer xaFlags, TransactionState ts)
                          throws BrokerException {

        if (xid != null && ts.getState() == TransactionState.STARTED) {
            int oldstate = TransactionState.STARTED;
            BrokerException ex;
            try {
                ts.nextState(PacketType.ROLLBACK_TRANSACTION, xaFlags);
                return;

            } catch (BrokerException e) {
                ex = e;
                logger.log(logger.ERROR, e.getMessage());
            }
            ts = translist.updateState(id, TransactionState.FAILED,
                                       TransactionState.STARTED, true);
            String[] args = { "ROLLBACK",
                              id.toString()+"["+TransactionState.toString(oldstate)+"]",
                              (xid==null? "null":xid.toString()) };
            logger.log(logger.WARNING, Globals.getBrokerResources().getKString(
                                       BrokerResources.W_FORCE_ENDED_TXN, args));

            throw ex;
        }
    }

             
    /**
	 * Rollback a transaction. This method is invoked from two places: 1) From
	 * TransactionHandler.handle() when handling a client ROLLBACK packet. This
	 * is the common case. 2) From the admin handler when an admin rollback
	 * request has been issued on a PREPARED XA transaction.
	 * 
	 * @param id
	 *            The TransactionUID to commit
	 * @param xid
	 *            The Xid of the transaction to commit. Required if transaction
	 *            is an XA transaction. Must be null if it is not an XA
	 *            transaction.
	 * @param xaFlags
	 *            xaFlags passed on COMMIT operation. Used only if an XA
	 *            transaction.
	 * @param ts
	 *            Current TransactionState of this transaction.
	 * @param conlist
	 *            List of transactions on this connection. Will be null if
	 *            commit is trigger by an admin request.
	 * @param con
	 *            Connection client commit packet came in on or, for admin, the
	 *            connection the admin request came in on.
	 * 
	 * @throws BrokerException
	 *             on an error. The method will have logged a message to the
	 *             broker log.
	 */
    public void doRollback(TransactionUID id, JMQXid xid,
                          Integer xaFlags, TransactionState ts, List conlist,
                          IMQConnection con,  RollbackReason rbreason)
                          throws BrokerException {
        int s;
        int oldstate = ts.getState();

        // Update transaction state
        try {
            if (xid == null) {
                // Plain JMS transaction.
                s = TransactionState.ROLLEDBACK;
            } else {
                // XA Transaction. 
                if (rbreason == RollbackReason.ADMIN ||
                    rbreason == RollbackReason.CONNECTION_CLEANUP) {
                    if (ts.getState() == TransactionState.STARTED) { 
                        ts = translist.updateState(id, TransactionState.FAILED,
                                       TransactionState.STARTED, true);
                        String[] args = { rbreason.toString(), 
                                 id.toString()+"["+TransactionState.toString(oldstate)+"]",
                                          (xid==null? "null":xid.toString()) };
                        if (rbreason != RollbackReason.ADMIN && 
                            (DEBUG || DEBUG_CLUSTER_TXN || logger.getLevel() <= Logger.DEBUG)) {
                        logger.log(logger.WARNING, Globals.getBrokerResources().getKString(
                                                   BrokerResources.W_FORCE_ENDED_TXN, args));
                        }
                    }
                }
                s = ts.nextState(PacketType.ROLLBACK_TRANSACTION, xaFlags);
            }
        } catch (BrokerException ex) {
            if (ex.getStatusCode() == Status.CONFLICT) {
            logger.log(Logger.ERROR, ex.toString());
            } else {
            logger.log(Logger.ERROR,
                    BrokerResources.E_INTERNAL_BROKER_ERROR,
                    ex.toString() + ": TUID=" + id + " Xid=" + xid);
	    }
            throw ex;
        }
        ts = translist.updateState(id, s, true);
        
		if (Globals.isNewTxnLogEnabled()
				&& oldstate == TransactionState.PREPARED) {
			int transactionType = BaseTransaction.LOCAL_TRANSACTION_TYPE;
			if (translist.isClusterTransaction(id)) {
				transactionType = BaseTransaction.CLUSTER_TRANSACTION_TYPE;
			}
			logTxnCompletion(id, TransactionState.ROLLEDBACK, transactionType);
		}
        if (fi.FAULT_INJECTION)
            checkFIAfterDB(PacketType.ROLLBACK_TRANSACTION);

        List list = new ArrayList(translist.retrieveSentMessages(id));
        for (int i =0; list != null && i < list.size(); i ++ ) {
            SysMessageID sysid = (SysMessageID)list.get(i);
            if (DEBUG) {
                logger.log(Logger.DEBUG,"Removing " +
                      sysid + " because of rollback");
            }
            PacketReference ref = Destination.get(sysid);
            if (ref == null) continue;
            DestinationUID duid = ref.getDestinationUID();
            Destination d = Destination.getDestination(duid);
            d.removeMessage(sysid, RemoveReason.ROLLBACK);
        }

        // remove from our active connection list
        if (conlist != null) {
            conlist.remove(id);
        }

        // re-queue any orphan messages

        // how we handle the orphan messages depends on a couple
        // of things:
        //   - has the session closed ?
        //     if the session has closed the messages are "orphan"
        //   - otherwise, the messages are still "in play" and
        //     we dont do anything with them
        //
        Map m = translist.getOrphanAck(id);
        if (m != null) {
            Iterator itr = m.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry me = (Map.Entry)itr.next();
                SysMessageID sysid = (SysMessageID)me.getKey();
                PacketReference ref = Destination.get(sysid, false);
                if (ref == null) {
                    logger.log(Logger.DEBUG,id + ":Unknown orphan " + sysid);
                    continue;
                }
                Map sids = (Map)me.getValue();
                if (sids == null) continue; 

                Iterator siditr = sids.entrySet().iterator();
                while (siditr.hasNext()) {
                    Map.Entry se = (Map.Entry)siditr.next();
                    ConsumerUID sid = (ConsumerUID)se.getKey();
                    if (ref.isLocal()) { 
                        ref.getDestination().forwardOrphanMessage(ref, sid);
                        continue;
                    }
                    List cids = (List)se.getValue();
                    if (cids == null) continue;

                    Iterator ciditr = cids.iterator();
                    while (ciditr.hasNext()) {
                        ConsumerUID cid = (ConsumerUID)ciditr.next();
                        try {
                            if (ref.acknowledged(cid, sid, 
                                                 !(cid.isNoAck()||cid.isDupsOK()),
                                                 false, id, null, false)) {
                                Destination d = ref.getDestination();
                                d.removeRemoteMessage(sysid, RemoveReason.ACKNOWLEDGED, ref);
                            }
                        } catch(Exception ex) {
                            logger.logStack((DEBUG_CLUSTER_TXN ? Logger.WARNING:Logger.DEBUG),
                                            "Unable to cleanup orphaned remote message "+
                                            "[" + cid + "," + sid + "," + sysid+"]"+
                                            " on rollback transaction " +id, ex);
                        }
                        BrokerAddress addr = translist.getAckBrokerAddress(id, sysid, cid);
                        try {
                            HashMap prop = new HashMap();
                            prop.put(ClusterBroadcast.RB_RELEASE_MSG_ORPHAN, id.toString());
                            Globals.getClusterBroadcast().acknowledgeMessage(
                                                          addr, sysid, cid,
                                                          ClusterBroadcast.MSG_IGNORED,
                                                          prop, false /*no wait ack*/);
                        } catch (BrokerException e) {
                            Globals.getLogger().log(Logger.WARNING, 
                            "Unable to notify "+addr+" for orphaned remote message "+
                            "["+cid+", "+sid+", "+", "+sysid+"]"+
                            " in rollback transaction "+ id);
                        }
                    }
                }
            }
        }

        // OK .. now remove the acks
        translist.removeTransactionAck(id, true);

	    /*
	     * Can't really call the JMX notification code at the end of doRollback()
	     * because the call to translist.removeTransactionID(id) removes the MBean.
	     */
	    Agent agent = Globals.getAgent();
	    if (agent != null)  {
	        agent.notifyTransactionRollback(id);
	    }
       
        try {
            ts.setState(s);
            cacheSetState(id, ts, con);
            doRemoteRollback(id, s);
            translist.removeTransactionID(id);
            if (rbreason == RollbackReason.ADMIN ||
                rbreason == RollbackReason.CONNECTION_CLEANUP) {
                String[] args = { rbreason.toString(), 
                         id.toString()+"["+TransactionState.toString(oldstate)+"]",
                                  (xid==null? "null":xid.toString()) };
                if (rbreason == RollbackReason.CONNECTION_CLEANUP) {
                    if (DEBUG || DEBUG_CLUSTER_TXN || logger.getLevel() <= Logger.DEBUG) {
                    logger.log(logger.INFO, Globals.getBrokerResources().getKString(
                                            BrokerResources.W_FORCE_ROLLEDBACK_TXN, args));
                    }
                } else {
                    logger.log(logger.WARNING, Globals.getBrokerResources().getKString(
                                               BrokerResources.W_FORCE_ROLLEDBACK_TXN, args));
                }
            }
        } catch (BrokerException ex) {
            logger.logStack(Logger.ERROR, BrokerResources.E_INTERNAL_BROKER_ERROR,
                            "exception removing transaction", ex);
            throw ex;
        }
    }


    /**
     * this method triggers a redeliver of all unacked
     * messages in the transaction.
     * Since there may be multiple connections for the various
     * consumers .. we guarentee that the messages for this
     * transaction are resent in order but make no guarentee
     * about other messages arriving before these messages.
     */
    public void redeliverUnacked(TransactionUID tid, boolean processActiveConsumers,
             boolean redeliver) 
        throws BrokerException
    {
        List plist = null;
        HashMap cmap = null;
        HashMap sToCmap = null;
        BrokerException bex = null;
        cmap = translist.retrieveConsumedMessages(tid, true);
        sToCmap = translist.retrieveStoredConsumerUIDs(tid);
        /* OK .. we've retrieved the messages .. now go through
           them and get References and order them
         */

        if (DEBUG) {
            logger.log(logger.INFO, "redeliverUnacked:tid="+tid+", consumed#="+cmap.size());
        }

        HashMap sendMap = new HashMap();

        /*
         * transaction ack data is stored as message -> list of IDs
         */
        // loop through the transaction ack list
        if (cmap != null && cmap.size() > 0) {
            Iterator itr = cmap.entrySet().iterator();
            while (itr.hasNext()) {
                // for each message, get the ref
                Map.Entry entry = (Map.Entry)itr.next();
                SysMessageID sysid = (SysMessageID)entry.getKey();
                // CANT just pull from connection
                if (sysid == null) continue;
                PacketReference ref = Destination.get(sysid, false);

                if (ref == null || ref.isDestroyed() ||
                      ref.isInvalid())  {
                      // already been deleted .. ignore
                      if (DEBUG) {
					  logger.log(logger.INFO, "redeliverUnacked:tid="+tid+": ref="+ref+" already deleted");
                      }
                      continue;
                }
                if (ref.isOverrided()) continue; 

                // now get the interests and stick each message
                // in a list specific to the consumer
                // the list is automatically sorted
                List interests = (List) entry.getValue();
                for (int i = 0; i < interests.size(); i ++) {
                    ConsumerUID intid = (ConsumerUID) interests.get(i);
                    ConsumerUID stored = (ConsumerUID)sToCmap.get(intid);
                    if (stored == null) {
                        stored = intid;
                    }

                    SortedSet ss = (SortedSet)sendMap.get(intid);
                    if (ss == null) {
                        ss = new TreeSet(new RefCompare());
                        sendMap.put(intid, ss);
                    }
                    ref.removeInDelivery(stored);
                    ss.add(ref);
                }
            }
        }

        if (DEBUG) {
           logger.log(logger.INFO, "redeliverUnacked:tid="+tid+", sendMap#="+sendMap.size());
        }

        // OK -> now we have a NEW hashmap id -> messages (sorted)
        // lets resend the ones we can to the consumer
        ArrayList updatedRefs = new ArrayList();
        Iterator sitr = sendMap.entrySet().iterator();
        while (sitr.hasNext()) {
            Map.Entry entry = (Map.Entry)sitr.next();
            ConsumerUID intid = (ConsumerUID)entry.getKey();
            ConsumerUID stored = (ConsumerUID)sToCmap.get(intid);
            if (stored == null) {
                stored = intid;
            }
            SortedSet ss = (SortedSet)entry.getValue();

            Consumer cs = Consumer.getConsumer(intid);
            if (cs == null || processActiveConsumers) {
                updateRefsState(stored, ss, redeliver, updatedRefs, tid);
            }
            if (cs == null) {
                if (DEBUG) {
                    logger.log(Logger.INFO,tid + ":Can not redeliver messages to "
                       + intid + " consumer is gone");
                }
                continue;
            }
            if (DEBUG) {
                logger.log(Logger.INFO,tid + ":Redelivering " + 
                       ss.size() + " msgs to " + intid );
            }
            if (!processActiveConsumers) {
               // we dont want to process open consumers
               // if the boolen is false ->
               // remove them so that we only process inactive
               // consumers later
                sitr.remove();
            } else if (cs.routeMessages(ss, true /* toFront*/)) {
                if (DEBUG) {
                    logger.log(Logger.INFO,"Sucessfully routed msgs to " + cs);
                }
                sitr.remove();
            } else { // couldnt route messages ..invalid consumer
                if (DEBUG) {
                    logger.log(Logger.INFO,"Could not route messages to " + cs);
                }
            }
        }

        // finally deal w/ any remaining messages (those are the
        // ones that dont have consumers)
        if (DEBUG) {
            logger.log(Logger.INFO,tid + ":after redeliver, " +
                sendMap.size() + " inactive consumers remaining");
        }
        // loop through remaining messages

        redeliverUnackedNoConsumer(sendMap, sToCmap, redeliver, tid, translist);

    }    

    //called from redeliverUnacked for rollback
    private void updateRefsState(ConsumerUID storedCuid, SortedSet refs,
                                 boolean redeliver, List updatedRefs, TransactionUID tid) { 
        PacketReference ref = null;
        Iterator itr = refs.iterator(); 
        while (itr.hasNext()) {
            try {
                ref = (PacketReference)itr.next();
                if (updatedRefs.contains(ref)) {
                    continue;
                }
                if (redeliver) {
                    ref.consumed(storedCuid, false, false);
                } else {
                    ref.removeDelivered(storedCuid, true);
                }
                updatedRefs.add(ref);
            } catch (Exception ex) {
                Object[] args = { "["+ref+", "+storedCuid+"]", tid, ex.getMessage() };
                logger.log(Logger.WARNING, Globals.getBrokerResources().getKString(
                    BrokerResources.W_UNABLE_UPDATE_REF_STATE_ON_ROLLBACK, args), ex);
            }
        }
    }

    /**
     * @param sendMap consumerUID to PacketReferences map
     * @param sToCmap consumerUID to stored ConsumerUID map
     * @param redeliver whether set redeliver flag
     * @param tid null if not for transaction rollback
     * @param translist null if tid is null
     */
    public static void redeliverUnackedNoConsumer(HashMap sendMap, 
                                                  HashMap sToCmap, 
                                                  boolean redeliver,
                                                  TransactionUID tid,
                                                  TransactionList translist)
                                                  throws  BrokerException {

        Logger logger = Globals.getLogger();

        Iterator sitr = sendMap.entrySet().iterator();
        while (sitr.hasNext()) {
            Map.Entry entry = (Map.Entry)sitr.next();
            ConsumerUID intid = (ConsumerUID)entry.getKey();
            ConsumerUID storedID =(ConsumerUID) sToCmap.get(intid); 

            SortedSet ss = (SortedSet)entry.getValue();
            Iterator itr = ss.iterator(); 
            while (itr.hasNext()) {
                PacketReference ref = (PacketReference)itr.next();
                SysMessageID sysid = ref.getSysMessageID();
                if (!ref.isLocal()) {
                    if (tid != null && translist != null) {
                        translist.removeOrphanAck(tid, sysid, storedID, intid);
                    }
                    
                    try {
                        if (ref.acknowledged(intid, storedID, 
                                             !(intid.isNoAck()||intid.isDupsOK()),
                                             false, tid, null, false)) {
                            Destination d = ref.getDestination();
                            d.removeRemoteMessage(sysid, RemoveReason.ACKNOWLEDGED, ref);
                        }
                    } catch(Exception ex) {
                        logger.logStack((DEBUG_CLUSTER_TXN ? Logger.WARNING:Logger.DEBUG),
                                        "Unable to cleanup remote message "+
                                        "[" + intid + "," + storedID + "," + sysid+"]"+
                                        " on rollback transaction " +tid+" for inactive consumer.", ex);
                    }
                    BrokerAddress addr = null;
                    if (tid != null && translist != null) {
                        addr = translist.getAckBrokerAddress(tid, sysid, intid);
                    } else {
                        addr = ref.getAddress();
                    }
                    try {
                        HashMap prop = new HashMap();
                        if (tid != null) {
                            prop.put(ClusterBroadcast.RB_RELEASE_MSG_INACTIVE, tid.toString());
                        } else {
                            prop.put(ClusterBroadcast.RC_RELEASE_MSG_INACTIVE, "");
                        }
                        Globals.getClusterBroadcast().acknowledgeMessage(
                                                      addr, sysid, intid,
                                                      ClusterBroadcast.MSG_IGNORED,
                                                      prop, false /*no wait ack*/);
                    } catch (BrokerException e) {
                        Globals.getLogger().log(Logger.WARNING, 
                        "Unable to notify "+addr+" for remote message "+
                        "["+intid+", "+storedID+", "+", "+sysid+"]"+
                        " in "+(tid != null ? ("rollback transaction "+ tid):("recover"))+" for inactive consumer.");
                    }
                    itr.remove();
                    continue;
                }
            }

            if (storedID == null || intid == storedID) { 
                    // non-durable subscriber
                    // ignore
                    sitr.remove();
                   continue;
            }

            if (ss.isEmpty()) {
                if (DEBUG) {
                    logger.log(Logger.INFO, "redeliverUnackedNoConsuemr: "+
                    "empty local message set for consumer "+intid+"[storedID="+storedID+"]");
                }
                continue;
            }

            // see if we are a queue
            if (storedID == PacketReference.getQueueUID()) {
                // queue message on 
                // queues are complex ->
                PacketReference ref = (PacketReference)ss.first();

                if (ref == null) {
                    if (DEBUG) {
                        logger.log(Logger.INFO,"Internal Error: "
                           + " null reterence");
                    }
                    continue;
                }
                try {
                    if (redeliver) {
                        ref.consumed(storedID, false, false);
                    } else {
                        ref.removeDelivered(storedID, false);
                    }
                } catch (IOException ex) {
                    logger.log(Logger.WARNING,"Internal error",
                           ex);
                }

                Destination d= ref.getDestination();
                if (d == null) {
                    if (DEBUG) {
                        logger.log(Logger.INFO,"Internal Error: "
                           + " unknown destination for reference: "
                           + ref);
                    }
                    continue;
                }

                // route each message to the destination
                // this puts it on the pending list
                try {
                    d.forwardOrphanMessages(ss, storedID);
                    sitr.remove();
                } catch (Exception ex) {
                    logger.log(Logger.INFO,"Internal Error: "
                               + "Unable to re-queue message "
                               + " to queue " + d, ex);
                }
            } else { // durable 
                // ok - requeue the message on the subscription
                Consumer cs = Consumer.getConsumer(storedID);
                if (cs == null) {
                    if (DEBUG) {
                        logger.log(Logger.INFO,"Internal Error: "
                           + " unknown consumer " + storedID);
                    }
                    continue;
                }
                if (ss != null && !ss.isEmpty() &&
                     cs.routeMessages(ss, true /* toFront*/)) {
                    // successful
                    sitr.remove();
                }
            }
        }
        if (DEBUG && sendMap.size() > 0) {
            logger.log(Logger.INFO,tid + ":after all processing, " +
                sendMap.size() + " inactive consumers remaining");
        }

    }


    /**
     * Prepare an XATransaction.
     * @param id  The TransactionUID to commit
     * @param xaFlags  xaFlags passed on COMMIT operation.
     * @param ts       Current TransactionState of this transaction.
     * @param msg Client prepare packet.
     */
    public void doPrepare(TransactionUID id, Integer xaFlags, 
                   TransactionState ts, int pktType) throws BrokerException {
           doPrepare(id, xaFlags, ts, pktType, false,null);
    }

    public BaseTransaction doPrepare(TransactionUID id, Integer xaFlags, 
                          TransactionState ts, int pktType, 
                          boolean onephasePrepare, TransactionWork txnWork) throws BrokerException {

    	BaseTransaction baseTransaction = null;
    	boolean persist = true;
    	if (Globals.isNewTxnLogEnabled()) {
    		
    		// don't persist transaction state as we are logging it instead
    		persist=false;
			if (txnWork == null) {
				// prepare has been called directly from an end client
				// We need to construct the transactional work
				
				List plist = translist.retrieveSentMessages(id);
				HashMap cmap = translist.retrieveConsumedMessages(id);
				HashMap sToCmap = translist.retrieveStoredConsumerUIDs(id);
				txnWork = getTransactionWork2(plist, cmap, sToCmap);
				
			}
		}
    	
        boolean prepared = false;
        int s = ts.nextState(pktType, xaFlags);
        try {
            TransactionState nextState = new TransactionState(ts);
            nextState.setState(s);
            baseTransaction = doRemotePrepare(id, ts, nextState, txnWork);
            if(baseTransaction==null)
            {
            	// work not logged yet, so this must be a local broker transaction 
            	// (there are no remote acknowledgements)  
            	// The end client must have called prepare
            	if(Globals.isNewTxnLogEnabled())
                {           
            	    baseTransaction= new LocalTransaction();
            		baseTransaction.setTransactionWork(txnWork);
            		baseTransaction.setTransactionState(nextState);
            		baseTransaction.getTransactionDetails().setTid(id);
            		baseTransaction.getTransactionDetails().setXid(ts.getXid());
            		baseTransaction.getTransactionDetails().setState(TransactionState.PREPARED);
            	
            		logTxn(baseTransaction);                	
                }
            }
            translist.updateState(id, s, onephasePrepare, persist);
            prepared = true;
            try {
                Agent agent = Globals.getAgent();
                if (agent != null) agent.notifyTransactionPrepare(id);
            } catch (Throwable t ) {
                logger.log(Logger.WARNING, 
                "XXXI18N - JMX agent notify transaction prepared failed: "+t.getMessage()); 
            }
        } finally {
            if (!prepared) {
                translist.updateState(id, TransactionState.FAILED, 
                          onephasePrepare, TransactionState.PREPARED, persist);
            }
        }
        return baseTransaction;
    }

    private ClusterTransaction doRemotePrepare(TransactionUID id, TransactionState ts, 
                     TransactionState nextState, 
                     TransactionWork txnWork) throws BrokerException {
        if (nextState.getState() != TransactionState.PREPARED) {
            throw new BrokerException("Unexpected state "+nextState+" for transactionID:"+id);
        }
        HashMap bmmap =  retrieveConsumedRemoteMessages(id, false);
        if (bmmap == null) return null;
        if (Globals.getClusterBroadcast().getClusterVersion() < 
            ClusterBroadcast.VERSION_410) {
            return null;
        }

        TransactionBroker[] tbas = (TransactionBroker[])bmmap.keySet().toArray(
                                                       new TransactionBroker[0]);
        boolean persist = true;
        ClusterTransaction clusterTransaction =null;
        if(Globals.isNewTxnLogEnabled())
        {
        	// create a cluster transaction which will be logged  
        	clusterTransaction = new ClusterTransaction(id,nextState,txnWork,tbas);        	
        	translist.logClusterTransaction(id, nextState, tbas, true, persist, clusterTransaction);
        }
        else{
          translist.logClusterTransaction(id, nextState, tbas, true, persist);
        }
        if (DEBUG_CLUSTER_TXN) {
            StringBuffer buf = new StringBuffer();
            buf.append("Preparing transaction "+id+", brokers");
            for (int i = 0; i < tbas.length; i++) {
                 buf.append("\n\t"+tbas[i]);
            }
            logger.log(logger.INFO, buf.toString());
        }
        ArrayList[] mcll = null;
        TransactionBroker ba = null;
        for (int i = 0; i < tbas.length; i++) {
            ba = tbas[i];
            if (ba.getBrokerAddress() == Globals.getMyAddress()) continue;
            mcll = (ArrayList[]) bmmap.get(ba);
            try {

            Globals.getClusterBroadcast().acknowledgeMessage2P(ba.getBrokerAddress(), 
                                (SysMessageID[])mcll[0].toArray(new SysMessageID[0]),
                                (ConsumerUID[])mcll[1].toArray(new ConsumerUID[0]),
                                ClusterBroadcast.MSG_PREPARE, null, new Long(id.longValue()), true, false);
            } catch (BrokerException e) {
            if (!(e instanceof BrokerDownException) && 
                !(e instanceof AckEntryNotFoundException)) throw e;

            HashMap sToCmap = translist.retrieveStoredConsumerUIDs(id);

            ArrayList remoteConsumerUIDa = new ArrayList();
            StringBuffer remoteConsumerUIDs = new StringBuffer();
            String uidstr = null;
            StringBuffer debugbuf =  new StringBuffer();
            for (int j = 0; j < mcll[0].size(); j++) {
                SysMessageID sysid = (SysMessageID)mcll[0].get(j);
                ConsumerUID  uid = (ConsumerUID)mcll[1].get(j);
                ConsumerUID suid = (ConsumerUID)sToCmap.get(uid);
                if (suid == null || suid.equals(uid)) {
                    continue;
                }
                if (e.isRemote()) {
                    uidstr = String.valueOf(uid.longValue()); 
                    if (!remoteConsumerUIDa.contains(uidstr)) {
                        remoteConsumerUIDa.add(uidstr);
                        remoteConsumerUIDs.append(uidstr);
                        remoteConsumerUIDs.append(" ");
                        Consumer c = Consumer.getConsumer(uid);
                        if (c != null) { 
                            c.recreationRequested();
                        } else {
                            logger.log(logger.WARNING, "Consumer "+uid +
                            " not found in processing remote exception on preparing transaction "+id);
                        }
                    }
                }
                debugbuf.append("\n\t["+sysid+":"+uid+"]");
            }
            if (e.isRemote()) {
                e.setRemoteConsumerUIDs(remoteConsumerUIDs.toString());
                if (DEBUG_CLUSTER_TXN) {
                logger.log(logger.INFO, 
                "doRemotePrepare: JMQRemote Exception:remoteConsumerUIDs="+remoteConsumerUIDs+", remote broker "+ba);
                }
            }

            try {
            translist.updateState(id, TransactionState.FAILED, false, TransactionState.PREPARED, true);
            } catch (Exception ex) {
            logger.logStack(logger.WARNING, 
            "Unable to update transaction "+id+ " state to FAILED on PREPARE failure from "
            +ba+": "+ex.getMessage()+debugbuf.toString(), ex);
            throw e;
            }

            if (e instanceof AckEntryNotFoundException) {
                mcll = ((AckEntryNotFoundException)e).getAckEntries();
            }

            for (int j = 0; j < mcll[0].size(); j++) {
                SysMessageID sysid = (SysMessageID)mcll[0].get(j);
                ConsumerUID  uid = (ConsumerUID)mcll[1].get(j);
                boolean remove = true; 
                if (e instanceof BrokerDownException) { 
                    ConsumerUID suid = (ConsumerUID)sToCmap.get(uid);
                    if (suid == null || suid.equals(uid)) {
                        if (DEBUG_CLUSTER_TXN) {
                        logger.log(logger.INFO, 
                        "doRemotePrepare: no remove txnack "+sysid+", "+uid+ " for BrokerDownException from "+ba);
                        }
                        remove = false;
                    }
                }
                if (remove) {
                try {

                translist.removeAcknowledgement(id, sysid, uid);
                if (DEBUG_CLUSTER_TXN) {
                    logger.log(logger.INFO, 
                    "doRemotePrepare: removed txnack "+sysid+", "+uid+ " for BrokerDownException from "+ba);
                }

                } catch (Exception ex) {
                logger.logStack(logger.WARNING, 
                "Unable to remove transaction "+id+ " ack ["+sysid+":"+uid+"] on PREPARE failure from "
                 +ba+": "+ex.getMessage(), ex);
                }
                }
            }
            logger.log(logger.INFO, 
            "Preparing transaction + "+id+ " failed from " +ba+": " +e.getMessage()+debugbuf.toString());

            throw e;
            }
            
        }
        return clusterTransaction;
    }
    
    private void doRemoteRollback(TransactionUID id, int nextState) throws BrokerException { 
        if (nextState != TransactionState.ROLLEDBACK) {
            throw new BrokerException("Unexpected state "+nextState+" for transactionUID:"+id);
        }
        if (!translist.hasRemoteBroker(id)) return;

        if (Globals.getClusterBroadcast().getClusterVersion() < 
            ClusterBroadcast.VERSION_410) {
            return;
        }
        try {
            TransactionBroker[] bas = translist.getClusterTransactionBrokers(id); 
            if (DEBUG_CLUSTER_TXN) {
                StringBuffer buf = new StringBuffer();
                buf.append("Rollback transaction "+id+", remote brokers");
                for (int i = 0; i < bas.length; i++) {
                    buf.append("\n\t"+bas[i]);
                }
                logger.log(logger.INFO, buf.toString());
            }
            BrokerAddress addr =  null;
            for (int i = 0; i < bas.length; i++) {
                if (bas[i].getBrokerAddress() == Globals.getMyAddress()) continue; 
                addr = bas[i].getCurrentBrokerAddress();
                if (addr == Globals.getMyAddress()) {
                    if (DEBUG_CLUSTER_TXN) {
                    logger.log(logger.INFO, 
                    "Transaction remote broker current address "+bas[i].toString()+
                    " is my address, TUID="+id);
                    }
                } else {
                    if (DEBUG_CLUSTER_TXN) {
                    logger.log(logger.INFO, 
                    "Transaction remote broker current address "+addr+", TUID="+id);
                    }
                }
                if (addr != null) {
                Globals.getClusterBroadcast().acknowledgeMessage2P(addr,
                                 (SysMessageID[])null, (ConsumerUID[])null,
                                 ClusterBroadcast.MSG_ROLLEDBACK, null,
                                 new Long(id.longValue()), false, false);
                } else {
                throw new BrokerDownException("Failed to rollback transaction "+
                id+" to broker "+bas[i].toString(), Status.GONE);
                }
            }
        } catch (Exception e) {
            logger.log(logger.WARNING,"notify rollback transaction"+id +" failed:"+e.getMessage()); 
            if (DEBUG_CLUSTER_TXN) {
            logger.logStack(logger.WARNING,
            "Notifying rollback transaction "+id +" failed:"+e.getMessage(), e); 
            }
        }
    }
   
    private BaseTransaction doRemoteCommit(TransactionUID id, Integer xaFlags, 
                                TransactionState ts, int nextState, Packet msg,
                                TransactionWork txnWork)
                                throws BrokerException {
        if (nextState != TransactionState.COMMITTED) {
            throw new BrokerException("Unexpected next state: "+nextState+" for transaction "+id); 
        }
        BaseTransaction baseTransaction = null;
        if (ts.getState() != TransactionState.PREPARED) {
            if (retrieveConsumedRemoteMessages(id, true) != null) {
            	// In this transaction messages that belong to a remote broker
            	// have been acknowledged. These messages must be
            	// acknowledged in the remote broker as part of the same transaction.
            	// We therefore need to make this a 2-phase commit.
              
                if (Globals.getClusterBroadcast().getClusterVersion() < 
                                            ClusterBroadcast.VERSION_410) {
                    return null;
                }
                Packet p = new Packet();
                try {
                p.fill(msg);
                } catch (IOException e) {
                logger.logStack(Logger.INFO,"Internal Exception processing packet ", e);
                throw new BrokerException(e.getMessage(), e);
                }
                p.setPacketType(PacketType.PREPARE_TRANSACTION);
                if (ts.getState() == TransactionState.STARTED && ts.getXid() == null) {
                    ts.setState(TransactionState.COMPLETE);
                    xaFlags = new Integer(XAResource.TMNOFLAGS);
                }
                baseTransaction = doPrepare(id, xaFlags, ts, p.getPacketType(), true, txnWork);
            }
        }
        return baseTransaction;
    }

    private HashMap retrieveConsumedRemoteMessages(TransactionUID id,
                    boolean checkOnly) throws BrokerException {
        HashMap mcmap = translist.retrieveConsumedMessages(id);
        if (mcmap == null || mcmap.size() == 0) return null;

        Iterator itr = mcmap.keySet().iterator();
        HashMap bmmap = new HashMap();
        ArrayList[] mcll = null;
        boolean hasRemote =  false;
        while (itr.hasNext()) {
            SysMessageID sysid = (SysMessageID)itr.next();
            if (sysid == null) continue;
            PacketReference ref = Destination.get(sysid, false);
            if (checkRefRequeued(id, ref, sysid)) {
                BrokerException bex = new BrokerException(
                   Globals.getBrokerResources().getKString(
                   BrokerResources.X_MESSAGE_MAYBE_REROUTED, sysid)+
                   ", TUID="+id, Status.GONE);
                bex.setRemote(true);
                StringBuffer buf = new StringBuffer();
                List interests = (List)mcmap.get(sysid);
                for (int i = 0 ; i < interests.size(); i++) {
                    buf.append(String.valueOf(((ConsumerUID)interests.get(i)).longValue()));
                    buf.append(" ");
                }
		        bex.setRemoteConsumerUIDs(buf.toString());
                throw bex;
            }
			if (ref == null) {
                throw new BrokerException(
                Globals.getBrokerResources().getKString(
                BrokerResources.X_MESSAGE_REF_GONE, sysid)+", TUID="+id, Status.CONFLICT);
            }
            BrokerAddress ba = ref.getAddress();
            if (ba == null) ba = Globals.getMyAddress();
            if (ba != Globals.getMyAddress() && !hasRemote) {
                hasRemote = true;
                if (checkOnly) return new HashMap();
            }
            TransactionBroker tba = new TransactionBroker(ba);
            mcll = (ArrayList[])bmmap.get(tba);
            if (mcll == null) {
                mcll = new ArrayList[2];
                mcll[0] =  new ArrayList();
                mcll[1] =  new ArrayList();
                bmmap.put(tba, mcll);
            }
            List interests = (List)mcmap.get(sysid);
            for (int i = 0 ; i < interests.size(); i++) {
                mcll[0].add(sysid);
                mcll[1].add(interests.get(i));
            }
         }
         if (!hasRemote) return null;
         return bmmap;
    }

    private boolean checkRefRequeued(TransactionUID id, 
                    PacketReference ref, SysMessageID sysid)
                    throws BrokerException {
        HashMap addrmap = translist.retrieveAckBrokerAddresses(id);
        BrokerAddress oldAddr = (BrokerAddress)addrmap.get(sysid);
        if (ref == null && Destination.isLocked(sysid)) {
        logger.log(logger.WARNING, "Message "+sysid+
        ((oldAddr==null)? "":" ("+oldAddr+")")+" is in takeover, TUID="+id);
        return true;
        }
        if (ref == null) {
        logger.log(logger.WARNING, 
        Globals.getBrokerResources().getKString(
                BrokerResources.X_MESSAGE_REF_GONE, sysid)+
                " "+((oldAddr==null)? "":" ("+oldAddr+")")+", TUID="+id);
        return false; 
        }
        if (ref.isOverrided()) return true;

        BrokerAddress currAddr = ref.getAddress();
        if (oldAddr == null && currAddr == null) return false;
        if ((oldAddr == null && currAddr != null) ||
            (oldAddr != null && currAddr == null)) return true; 
        if (!oldAddr.equals(currAddr)) return true;
        UID oldUID = oldAddr.getBrokerSessionUID();
        UID currUID = currAddr.getBrokerSessionUID();
        if (oldUID == null || currUID == null) return false;
        if (!oldUID.equals(currUID)) return true;
        return false;
    }


    public void doStart(TransactionUID id, List conlist, IMQConnection con, 
                        AutoRollbackType type, JMQXid xid, boolean sessionLess, long lifetime, long messagetid,
                        Integer xaFlags, int pktType, boolean replay, String creatorID)
        throws BrokerException
    {

        HashMap tidMap =
            (HashMap)con.getClientData(IMQConnection.TRANSACTION_IDMAP);

        int status  =0;
        String reason = null;
        TransactionState ts = null;
        ts = translist.retrieveState(id);

        if (type == AutoRollbackType.NEVER || lifetime > 0) {
            // not supported
            status = Status.NOT_IMPLEMENTED;
            reason = "AutoRollbackType of NEVER not supported";
            throw new BrokerException(reason, status);
        } else if (xid != null && !sessionLess) {
            // not supported yet
            status = Status.NOT_IMPLEMENTED;
            reason = "XA transactions only supported on sessionless "
                      + "connections";
            throw new BrokerException(reason, status);
        } else if (xid == null && sessionLess) {
            // not supported yet
            status = Status.ERROR;
            reason = "non-XA transactions only supported on "
                              + " non-sessionless connections";
            throw new BrokerException(reason, status);

        } else {
     
            if (replay) {
                // do nothing it already happened
            } else if (xaFlags != null &&
                !TransactionState.isFlagSet(XAResource.TMNOFLAGS, xaFlags))
            {
                // This is either a TMJOIN or TMRESUME. We just need to
                // update the transaction state
                int s = ts.nextState(pktType, xaFlags);
                translist.updateState(id, s, true);
            } else {
              // Brand new transaction
                try {
                    if (con.getClientProtocolVersion() ==
                                                PacketType.VERSION1) {
                        // If 2.0 client Map old style ID to new
                        tidMap.put(new Long(messagetid), id);
                    }

                    if (xid != null && type == null) {
                        type = translist.AUTO_ROLLBACK ? AutoRollbackType.ALL:AutoRollbackType.NOT_PREPARED; 
                    }
                    ts = new TransactionState(type, lifetime, sessionLess);
                    ts.setState(TransactionState.STARTED);
                    ts.setUser(con.getUserName());
                    ts.setCreator(creatorID);
                    ts.setClientID(
                        (String)con.getClientData(IMQConnection.CLIENT_ID));
                    ts.setXid(xid);

                    if (con instanceof IMQConnection) {
                        ts.setConnectionString(
                            ((IMQConnection)con).userReadableString());
                        ts.setConnectionUID(
                            ((IMQConnection)con).getConnectionUID());
                    }
                    translist.addTransactionID(id, ts);

                    conlist.add(id);
                } catch (BrokerException ex) {
                    // XXX I18N
                    logger.log(Logger.ERROR,
                        "Exception starting new transaction: " +
                        ex.toString(), ex);
                    throw ex;
                }
             }
          }
      }


      public void doEnd(int pktType, JMQXid xid, Integer xaFlags,
                      TransactionState ts, TransactionUID id)
          throws BrokerException
      {
          String reason = null;
          boolean elogged = false;
          try {
              int s;
              try {
                  s = ts.nextState(pktType, xaFlags);
              } catch (BrokerException e) {
                  if (e.getStatusCode() == Status.NOT_MODIFIED) {
                      elogged = true;
                      logger.log(Logger.WARNING,
                             e.getMessage() + ": TUID=" + id + " Xid=" + xid);
                  }
                  throw e;
              }
              
              if(Globals.isMinimizeWrites()|| Globals.isNewTxnLogEnabled())
              {
            	  // don't persist end state change            	 
            	 translist.updateState(id, s, false); 
              }
              else
              {
                 translist.updateState(id, s, true);
              }
          } catch (Exception ex) {
              int status = Status.ERROR;
              if (!elogged) {
                  logger.logStack(Logger.ERROR,
                         ex.toString() + ": TUID=" + id + " Xid=" + xid, ex);
              }
              reason = ex.getMessage();
              if (ex instanceof BrokerException) {
                 status = ((BrokerException)ex).getStatusCode();
              }
              throw new BrokerException(reason, status);
          }
      }
}
