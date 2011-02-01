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
 * @(#)AckHandler.java	1.89 10/24/07
 */ 

package com.sun.messaging.jmq.jmsserver.data.handlers;

import java.io.*;
import java.util.*;

import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.data.PacketHandler;
import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.ClusterBroadcast;
import com.sun.messaging.jmq.jmsserver.core.Consumer;
import com.sun.messaging.jmq.jmsserver.data.TransactionList;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;
import com.sun.messaging.jmq.jmsserver.core.Session;
import com.sun.messaging.jmq.jmsserver.core.SessionUID;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;
import com.sun.messaging.jmq.jmsserver.util.PacketUtil;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.util.TransactionAckExistException;
import com.sun.messaging.jmq.jmsserver.util.UnknownTransactionException;
import com.sun.messaging.jmq.jmsserver.util.lists.RemoveReason;
import com.sun.messaging.jmq.io.*;
import com.sun.messaging.jmq.jmsserver.service.Connection;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQBasicConnection;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.util.JMQXid;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.BrokerStateHandler;
import com.sun.messaging.jmq.jmsserver.FaultInjection;



/**
 * Handler class which deals with recording message acknowldegements
 */
public class AckHandler extends PacketHandler 
{
    // An Ack block is a Long ConsumerUID and a SysMessageID
    static final int ACK_BLOCK_SIZE =  8 + SysMessageID.ID_SIZE;

    private int ackProcessCnt = 0; // used for fault injection
    private FaultInjection fi = null;

    private TransactionList translist = null;
    private final Logger logger = Globals.getLogger();
    private static boolean DEBUG = false;

    public static final int ACKNOWLEDGE_REQUEST=0;
    public static final int UNDELIVERABLE_REQUEST=1;
    public static final int DEAD_REQUEST=2;

    public static final int DEAD_REASON_UNDELIVERABLE = 0;
    public static final int DEAD_REASON_EXPIRED = 1;

    public static void checkRequestType(int ackType)
        throws BrokerException
    {
        if (ackType > DEAD_REQUEST || ackType < ACKNOWLEDGE_REQUEST) {
            // L10N .. localize internal error
            throw new BrokerException("Internal Error: unknown ackType "
                   + ackType);
        }
    }

    public AckHandler( TransactionList translist) 
    {
        this.translist = translist;
        fi = FaultInjection.getInjection();
    }

    /**
     * Method to handle Acknowledgement messages
     */
    public boolean handle(IMQConnection con, Packet msg) 
        throws BrokerException
    {
        int size = msg.getMessageBodySize();
        int ackcount = size/ACK_BLOCK_SIZE;
        int mod = size%ACK_BLOCK_SIZE;
        int status = Status.OK;
        String reason = null;

        if (DEBUG) {
            logger.log(Logger.INFO, "AckHandler: processing packet "+ 
                msg.toString()+", on connection "+con);
        }
        if (!con.isAdminConnection() && fi.FAULT_INJECTION) {
           ackProcessCnt ++; // for fault injection
        } else {
           ackProcessCnt = 0;
        }

        if (ackcount == 0 ) {
            logger.log(Logger.ERROR,BrokerResources.E_INTERNAL_BROKER_ERROR,
                     "Internal Error: Empty Ack Message "+
                     msg.getSysMessageID().toString());
            reason = "Empty ack message";
            status = Status.ERROR;
        }
        if (mod != 0) {
            logger.log(Logger.ERROR, BrokerResources.E_INTERNAL_BROKER_ERROR,
                     "Internal Error: Invalid Ack Message Size "
                     + String.valueOf(size) +  " for message " +
                     msg.getSysMessageID().toString());
            reason = "corrupted ack message";
            status = Status.ERROR;
        }

        TransactionUID tid = null;
        if (msg.getTransactionID() != 0) { // HANDLE TRANSACTION
            try {
                tid = new TransactionUID(msg.getTransactionID());
            } catch (Exception ex) {
               logger.log(Logger.ERROR, BrokerResources.E_INTERNAL_BROKER_ERROR,
                     "Internal Error: can not create transactionID for "
                     + msg, ex);
               status = Status.ERROR;
            }
        }
        boolean markDead = false;
        Hashtable props = null;
        Throwable deadthr = null;
        String deadcmt = null;
        int deadrs = DEAD_REASON_UNDELIVERABLE;
        int deliverCnt = 0;
        int ackType = ACKNOWLEDGE_REQUEST;
        boolean JMQValidate = false;
        try {
            props = msg.getProperties();
            Integer iackType = (props == null ? null 
                  :(Integer)props.get("JMQAckType"));
            ackType = (iackType == null ? ACKNOWLEDGE_REQUEST
                 : iackType.intValue());

            Boolean validateFlag = (props == null ? null 
                  :(Boolean)props.get("JMQValidate"));

            JMQValidate = (validateFlag == null ? false
                 : validateFlag.booleanValue());

            checkRequestType(ackType);
            if (ackType == DEAD_REQUEST ) {
                deadthr = (Throwable)props.get("JMQException");
                deadcmt = (String)props.get("JMQComment");
                Integer val = (Integer)props.get("JMQDeadReason");
                if (val != null) {
                    deadrs = val.intValue();
                }
                val = (Integer)props.get("JMSXDeliveryCount");
                deliverCnt = (val == null ? -1 : val.intValue());
            }   
        } catch (Exception ex) {
            // assume not dead
            logger.logStack(Logger.INFO, "Internal Error: bad protocol", ex);
            ackType = ACKNOWLEDGE_REQUEST;
        }

       
        // OK .. handle Fault Injection
        if (!con.isAdminConnection() && fi.FAULT_INJECTION) {
            Map m = new HashMap();
            if (props != null)
                m.putAll(props);
            m.put("mqAckCount", new Integer(ackProcessCnt));
            m.put("mqIsTransacted", Boolean.valueOf(tid != null));
            fi.checkFaultAndExit(FaultInjection.FAULT_ACK_MSG_1,
                 m, 2, false);
        }
        boolean remoteStatus = false;
        StringBuffer remoteConsumerUIDs = null; 
        List cleanList = null;
        SysMessageID ids[] = null;
        ConsumerUID cids[] = null;
        try {
            if (status == Status.OK) {
                DataInputStream is = new DataInputStream(
		            msg.getMessageBodyStream());

                // pull out the messages into two lists
                ids = new SysMessageID[ackcount];
                cids = new ConsumerUID[ackcount];
                for (int i = 0; i < ackcount; i ++) {
                    long newid = is.readLong();
                    cids[i] = new ConsumerUID(newid);
                    cids[i].setConnectionUID(con.getConnectionUID());
                    ids[i] =  new SysMessageID();
                    ids[i].readID(is);
                }
                if (JMQValidate) {
                    if (ackType == DEAD_REQUEST || 
                           ackType == UNDELIVERABLE_REQUEST) {
                        status = Status.BAD_REQUEST;
                        reason = "Can not use JMQValidate with ackType of "
                                  + ackType;
                    } else if (tid == null) {
                        status = Status.BAD_REQUEST;
                        reason = "Can not use JMQValidate with no tid ";
                    } else if (!validateMessages(tid, ids, cids)) {
                        status = Status.NOT_FOUND;
                        reason = "Acknowledgement not processed";
                    }
                } else if (ackType == DEAD_REQUEST) {
                     cleanList = handleDeadMsgs(con, ids, cids, deadrs,
                                                deadthr, deadcmt, deliverCnt);
                } else if (ackType == UNDELIVERABLE_REQUEST) {
                     cleanList = handleUndeliverableMsgs(con, ids, cids);
                } else {
                    if (tid != null) {
                        cleanList= handleTransaction(con, tid, ids, cids);
                     } else  {
                        cleanList = handleAcks(con, ids, cids, msg.getSendAcknowledge());
                     }
                }
           } 

        } catch (Throwable thr) {
            status = Status.ERROR;
            if (thr instanceof BrokerException) {
                status = ((BrokerException)thr).getStatusCode();
                remoteStatus = ((BrokerException)thr).isRemote();
                if (remoteStatus && ids != null && cids != null) {
                    remoteConsumerUIDs = new StringBuffer();
                    remoteConsumerUIDs.append(((BrokerException)thr).getRemoteConsumerUIDs());
                    remoteConsumerUIDs.append(" ");
                    String cidstr = null;
                    ArrayList remoteConsumerUIDa = new ArrayList();
                    for (int i = 0; i < ids.length; i++) {
                        PacketReference ref = Destination.get(ids[i]);
                        Consumer c = Consumer.getConsumer(cids[i]); 
                        if (c == null) continue;
                        ConsumerUID sid = c.getStoredConsumerUID();
                        if (sid == null || sid.equals(cids[i])) {
                            continue;
                        }
                        BrokerAddress ba = (ref == null ? null: ref.getAddress());
                        BrokerAddress rba = ((BrokerException)thr).getRemoteBrokerAddress();
                        if (c != null && ref != null && 
                            ba != null && rba != null && ba.equals(rba)) {
                            cidstr = String.valueOf(c.getConsumerUID().longValue());
                            if (!remoteConsumerUIDa.contains(cidstr)) {
                                remoteConsumerUIDa.add(cidstr);
                                remoteConsumerUIDs.append(cidstr);
                                remoteConsumerUIDs.append(" ");
                            }
                        }
                    }
                }
            }
            reason = thr.getMessage();
            if (status == Status.ERROR) { // something went wrong
                logger.logStack(Logger.ERROR,  BrokerResources.E_INTERNAL_BROKER_ERROR,
                    "-------------------------------------------" +
                    "Internal Error: Invalid Acknowledge Packet processing\n" +
                    " " + 
                    (msg.getSendAcknowledge() ? " notifying client\n"
                          : " can not notify the client" )
                    + com.sun.messaging.jmq.jmsserver.util.PacketUtil.dumpPacket(msg)
                    + "--------------------------------------------",
                    thr);
            }
        }

        // OK .. handle Fault Injection
        if (!con.isAdminConnection() && fi.FAULT_INJECTION) {
            Map m = new HashMap();
            if (props != null)
                m.putAll(props);
            m.put("mqAckCount", new Integer(ackProcessCnt));
            m.put("mqIsTransacted", Boolean.valueOf(tid != null));
            fi.checkFaultAndExit(FaultInjection.FAULT_ACK_MSG_2,
                 m, 2, false);
        }

          // send the reply (if necessary)
        if (msg.getSendAcknowledge()) {
 
             Packet pkt = new Packet(con.useDirectBuffers());
             pkt.setPacketType(PacketType.ACKNOWLEDGE_REPLY);
             pkt.setConsumerID(msg.getConsumerID());
             Hashtable hash = new Hashtable();
             hash.put("JMQStatus", new Integer(status));
             if (reason != null)
                 hash.put("JMQReason", reason);
             if (remoteStatus) {
                 hash.put("JMQRemote", Boolean.valueOf(true));
                 if (remoteConsumerUIDs != null) { 
                 hash.put("JMQRemoteConsumerIDs", remoteConsumerUIDs.toString());
                 }
             }  
             if (((IMQBasicConnection)con).getDumpPacket() || ((IMQBasicConnection)con).getDumpOutPacket())
                 hash.put("JMQReqID", msg.getSysMessageID().toString());
             pkt.setProperties(hash);
             con.sendControlMessage(pkt);

        }

        // OK .. handle Fault Injection
        if (!con.isAdminConnection() && fi.FAULT_INJECTION) {
            Map m = new HashMap();
            if (props != null)
                m.putAll(props);
            m.put("mqAckCount", new Integer(ackProcessCnt));
            m.put("mqIsTransacted", Boolean.valueOf(tid != null));
            fi.checkFaultAndExit(FaultInjection.FAULT_ACK_MSG_3,
                 m, 2, false);
        }

        // we dont need to clear the memory up until after we reply
        // to the ack
        cleanUp(cleanList);

        return true;
    }

    public void cleanUp(List cleanList)
    {
        if (cleanList != null && !cleanList.isEmpty()) {
            Iterator itr = cleanList.iterator();
            while (itr.hasNext()) {
                PacketReference ref = (PacketReference)itr.next();
                Destination d= ref.getDestination();
                try {
                    if (ref.isDead()) {
                        d.removeDeadMessage(ref);
                    } else {
                        d.removeMessage(ref.getSysMessageID(),
                           RemoveReason.ACKNOWLEDGED);
                    }
                } catch (Exception ex) {
                    Object[] eparam = {(ref == null ? "null":ref.toString()),
                                       (d == null ? "null":d.getUniqueName()), ex.getMessage()};
                    String emsg = Globals.getBrokerResources().getKString(
                                      BrokerResources.E_CLEANUP_MSG_AFTER_ACK, eparam);
                    if (DEBUG) {
                    logger.logStack(Logger.INFO, emsg, ex);
                    } else {
                    logger.log(Logger.INFO, emsg);
                    }
                }
            }
        }
    }

    public List handleAcks(IMQConnection con, SysMessageID[] ids, ConsumerUID[] cids, boolean ackack) 
        throws BrokerException, IOException
    {
        List l = new LinkedList();
        // XXX - we know that everything on this message
        // we could eliminate on of the lookups
        for (int i=0; i < ids.length; i ++) {
            if (DEBUG) {
            logger.log(logger.INFO, "handleAcks["+i+", "+ids.length+"]:sysid="+
                                     ids[i]+", cid="+cids[i]+", on connection "+con);
            }
            Session s = Session.getSession(cids[i]);
            if (s == null) { // consumer does not have session
               Consumer c = Consumer.getConsumer(cids[i]);
               if (c == null) {
                   if (!con.isValid() || con.isBeingDestroyed()) {
                        logger.log(logger.DEBUG,"Received ack for consumer " 
                             + cids[i] +  " on closing connection " + con);
                        continue;
                   }
                   if (BrokerStateHandler.shutdownStarted)
                       throw new BrokerException(
                           BrokerResources.I_ACK_FAILED_BROKER_SHUTDOWN);
                   throw new BrokerException("Internal Error: Unable to complete processing acks:"
                          + " Unknown consumer " + cids[i]);
                } else if (c.getConsumerUID().getBrokerAddress() !=
                           Globals.getClusterBroadcast().getMyAddress())
                {
                     // remote consumer
                     PacketReference ref = Destination.get(ids[i]);
                     ConsumerUID cuid = c.getConsumerUID();
                     // right now we dont store messages for remote
                     // brokers so the sync flag is unnecessary
                     // but who knows if that will change
                     if (ref.acknowledged(cuid, c.getStoredConsumerUID(), 
                                          !cuid.isDupsOK(), true, ackack)) {
                           l.add(ref);
                     }

                 } else {
                     logger.log(Logger.INFO,
                         Globals.getBrokerResources().getString(
                         BrokerResources.E_INTERNAL_BROKER_ERROR,
                         "local consumer does not have "
                         + "associated session " + c));
                     throw new BrokerException("Unknown local consumer " 
                         + cids[i]);
                }
            } else { // we have a session

               if (fi.FAULT_INJECTION) {
                   PacketReference ref = Destination.get(ids[i]);
                   if (ref != null && !ref.getDestination().isAdmin() && !ref.getDestination().isInternal()) {
                       if (fi.checkFault(fi.FAULT_ACK_MSG_1_5, null)) {
                           fi.unsetFault(fi.FAULT_ACK_MSG_1_5);
                           BrokerException bex = new BrokerException("FAULT:"+fi.FAULT_ACK_MSG_1_5, Status.GONE);
                           bex.setRemoteConsumerUIDs(String.valueOf(cids[i].longValue()));
                           bex.setRemote(true);
                           throw bex;
                       }
                   }
               }
               PacketReference ref = s.ackMessage(cids[i], ids[i], ackack);
               if (ref != null) {
                     l.add(ref);
               }
            } 
        }
        return l; 
    }

    public List handleTransaction(IMQConnection con, TransactionUID tid, 
                                  SysMessageID[] ids, ConsumerUID[] cids) 
                                  throws BrokerException {
        for (int i=0; i < ids.length; i ++) {
            Consumer consumer = null;
            try {
                // lookup the session by consumerUID

                Session s = Session.getSession(cids[i]);

                // look up the consumer by consumerUID
                consumer = Consumer.getConsumer(cids[i]);

                if (consumer == null) {
                    throw new BrokerException(
                          Globals.getBrokerResources().getKString(
                          BrokerResources.I_ACK_FAILED_NO_CONSUMER, cids[i]), Status.NOT_FOUND);
                }
                // try and find the stored consumerUID
                ConsumerUID sid = consumer.getStoredConsumerUID();

                // if we still dont have a session, attempt to find one
                if (s == null) {
                    SessionUID suid = consumer.getSessionUID();
                    s = Session.getSession(suid);
                }
                if (s == null)  {
                   if (BrokerStateHandler.shutdownStarted)
                       throw new BrokerException(
                           BrokerResources.I_ACK_FAILED_BROKER_SHUTDOWN);
                   throw new BrokerException("Internal Error: Unable to complete processing transaction:"
                          + " Unknown consumer/session " + cids[i]);
                }
                if (DEBUG) {
                logger.log(logger.INFO, "handleTransaction.addAck["+i+", "+ids.length+"]:tid="+tid+
                           ", sysid="+ids[i]+", cid="+cids[i]+", sid="+sid+" on connection "+con);
                }
                boolean isxa = translist.addAcknowledgement(tid, ids[i], cids[i], sid);
                BrokerAddress addr = s.acknowledgeInTransaction(cids[i], ids[i], tid, isxa);
                if (addr != null && addr != Globals.getMyAddress()) {
                    translist.setAckBrokerAddress(tid, ids[i], cids[i], addr);
                }
                if (fi.FAULT_INJECTION) {
                    if (fi.checkFault(fi.FAULT_TXN_ACK_1_5, null)) {
                        fi.unsetFault(fi.FAULT_TXN_ACK_1_5);
                        TransactionAckExistException tae = new TransactionAckExistException("FAULT:"+fi.FAULT_TXN_ACK_1_5, Status.GONE);
                        tae.setRemoteConsumerUIDs(String.valueOf(cids[i].longValue()));
                        tae.setRemote(true);
                        consumer.recreationRequested();
                        throw tae;
                    }
                }
            } catch (Exception ex) {
                String emsg = "["+ids[i]+", "+cids[i] + "]TUID="+tid;
                if ((ex instanceof BrokerException) && 
                    ((BrokerException)ex).getStatusCode() !=  Status.ERROR) {
                    logger.log(Logger.WARNING , Globals.getBrokerResources().getKString(
                    BrokerResources.E_TRAN_ACK_PROCESSING_FAILED, emsg, ex.getMessage()), ex);
                } else {
                    logger.log(Logger.ERROR , Globals.getBrokerResources().getKString(
                    BrokerResources.E_TRAN_ACK_PROCESSING_FAILED, emsg, ex.getMessage()), ex);
                }
                int state = -1;
                JMQXid xid = null;
                try {
                    TransactionState ts = translist.retrieveState(tid);
                    if (ts != null) {
                        state = ts.getState();
                        xid = ts.getXid();
                    }
                    translist.updateState(tid, TransactionState.FAILED, true);
                } catch (Exception e) {
                    if (!(e instanceof UnknownTransactionException)) {
                    String args[] = { TransactionState.toString(state),
                                      TransactionState.toString(TransactionState.FAILED),
                                      tid.toString(),  (xid == null ? "null": xid.toString()) };
                    logger.log(Logger.WARNING, Globals.getBrokerResources().getKString(
                                      BrokerResources.W_UPDATE_TRAN_STATE_FAIL, args));
                    }
                }
                if (ex instanceof TransactionAckExistException) {
                    PacketReference ref = Destination.get(ids[i]);
                    if (ref != null && (ref.isOverrided() || ref.isOverriding())) {
                        ((BrokerException)ex).overrideStatusCode(Status.GONE);
                        ((BrokerException)ex).setRemoteConsumerUIDs(
                                 String.valueOf(cids[i].longValue()));
                        ((BrokerException)ex).setRemote(true);
                        consumer.recreationRequested();
                    }
                }
                if (ex instanceof BrokerException)  throw (BrokerException)ex;

                throw new BrokerException("Internal Error: Unable to " +
                    " complete processing acknowledgements in a tranaction: " 
                     +ex, ex);
            }
        }
        return null;

    }


    public boolean validateMessages(TransactionUID tid,
            SysMessageID[] ids, ConsumerUID[] cids)
        throws BrokerException
    {

// LKS - XXX need to revisit this
// I'm not 100% sure if we still need this or not (since its really
// targeted at supporting the NEVER rollback option
//
// putting in a mimimal support for the feature

        // OK, get a status on the transaction

        boolean openTransaction = false;
        try {
            translist.getTransactionMap(tid, false);
            openTransaction = true;
        } catch (BrokerException ex) {
        }

       
        // if transaction exists we need to check its information
        if (openTransaction) {
            for (int i=0; i < ids.length; i ++) {
                Consumer c = Consumer.getConsumer(cids[i]);
                if (c == null) { // unknown consumer
                    throw new BrokerException("Internal Error, " +
                       "unknown consumer " + cids[i], 
                       Status.BAD_REQUEST);
                }
                if (!translist.checkAcknowledgement(tid, ids[i], 
                   c.getConsumerUID())) {
                    return false;
                }
           }
        } else { // check packet reference
            for (int i=0; i < ids.length; i ++) {
                Consumer c = Consumer.getConsumer(cids[i]);
                if (c == null) { // unknown consumer
                    throw new BrokerException("Internal Error, " +
                       "unknown consumer " + cids[i], 
                       Status.BAD_REQUEST);
                }
                PacketReference ref = Destination.get(ids[i]);
                if (ref == null) {
                    logger.log(Logger.DEBUG, "in validateMessages: Could not find " + ids[i]);
                    continue;
                }
                if (!ref.hasConsumerAcked(c.getStoredConsumerUID())) {
                    return false;
                }
           }
        }
         
        return true;
    }

    public List handleDeadMsgs(IMQConnection con, 
                               SysMessageID[] ids, ConsumerUID[] cids, 
                               int deadrs, Throwable thr,
                               String comment, int deliverCnt) 
                               throws BrokerException {
        RemoveReason deadReason = RemoveReason.UNDELIVERABLE; 
        if (deadrs == DEAD_REASON_EXPIRED) {
            deadReason = RemoveReason.EXPIRED_BY_CLIENT;
        }
        List l = new ArrayList();
        for (int i=0; i < ids.length; i ++) {
            Session s = Session.getSession(cids[i]);
            if (s == null) { // consumer does not have session
                 // really nothing to do, ignore
                logger.log(Logger.DEBUG,"Dead message for Unknown Consumer/Session"
                        + cids[i]);
                continue;
            }
            if (DEBUG) {
            logger.log(logger.INFO, "handleDead["+i+", "+ids.length+"]:sysid="+
                                    ids[i]+", cid="+cids[i]+", on connection "+con);
            }

            PacketReference ref = s.handleDead(cids[i], ids[i], deadReason,
                                               thr, comment, deliverCnt);

            // if we return the reference, we could not re-deliver it ...
            // no consumers .. so clean it up
            if (ref != null) {
                l.add(ref);
            }
        }
        return l;
    }


    public List handleUndeliverableMsgs(IMQConnection con, 
                                        SysMessageID[] ids, ConsumerUID[] cids) 
                                        throws BrokerException {

        List l = new ArrayList();
        for (int i=0; i < ids.length; i ++) {
            Session s = Session.getSession(cids[i]);
            if (s == null) { // consumer does not have session
                 // really nothing to do, ignore
                logger.log(Logger.DEBUG,"Undeliverable message for Unknown Consumer/Session"
                        + cids[i]);
            }
            if (DEBUG) {
            logger.log(logger.INFO, "handleUndeliverable["+i+", "+ids.length+"]:sysid="+
                                    ids[i]+", cid="+cids[i]+", on connection "+con);
            }
            PacketReference ref = (s == null ? null : s.handleUndeliverable(cids[i], ids[i]));

            // if we return the reference, we could not re-deliver it ...
            // no consumers .. so clean it up
            if (ref != null) {
                l.add(ref);
            }
        }
        return l;
    }
}
