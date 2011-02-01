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

package com.sun.messaging.jmq.jmsserver.data.protocol;


import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.transaction.xa.XAResource;

import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.PacketType;
import com.sun.messaging.jmq.io.Status;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.core.Consumer;
import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;
import com.sun.messaging.jmq.jmsserver.core.Producer;
import com.sun.messaging.jmq.jmsserver.core.ProducerUID;
import com.sun.messaging.jmq.jmsserver.core.Session;
import com.sun.messaging.jmq.jmsserver.core.SessionUID;
import com.sun.messaging.jmq.jmsserver.data.AutoRollbackType;
import com.sun.messaging.jmq.jmsserver.data.PacketHandler;
import com.sun.messaging.jmq.jmsserver.data.PacketRouter;
import com.sun.messaging.jmq.jmsserver.data.RollbackReason;
import com.sun.messaging.jmq.jmsserver.data.TransactionList;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.data.handlers.AckHandler;
import com.sun.messaging.jmq.jmsserver.data.handlers.ClientIDHandler;
import com.sun.messaging.jmq.jmsserver.data.handlers.ConsumerHandler;
import com.sun.messaging.jmq.jmsserver.data.handlers.DataHandler;
import com.sun.messaging.jmq.jmsserver.data.handlers.FlowHandler;
import com.sun.messaging.jmq.jmsserver.data.handlers.ProducerHandler;
import com.sun.messaging.jmq.jmsserver.data.handlers.QBrowseHandler;
import com.sun.messaging.jmq.jmsserver.data.handlers.RedeliverHandler;
import com.sun.messaging.jmq.jmsserver.data.handlers.SessionHandler;
import com.sun.messaging.jmq.jmsserver.data.handlers.TransactionHandler;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.util.DestType;
import com.sun.messaging.jmq.util.JMQXid;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.util.selector.Selector;
import com.sun.messaging.jmq.util.selector.SelectorFormatException;



/**
 * 
 * Api used for direct access to the broker. This code is not integrated into 
 * the handler classes because the interaction (e.g. when callbacks happen) 
 * is different for the jms protocol and other protocols.
 */


public class ProtocolImpl implements Protocol
{

    private static boolean DEBUG = false; 

    PacketRouter pr = null; 

    public ProtocolImpl(PacketRouter router)
    {
        pr = router;
    }

    /**
     * called when a new connection is created.
     * <P>Packet:<B>HELLO,HELLO_REPLY</b></p>
     */

    public void hello() {
        // does nothing
    }

    /**
     * Authenticate with the passed in username, password
     * <P>Packet:<B>AUTHENTICATE</b></p>
     */
    public void authenticate(String username, String password)
        throws BrokerException
    {
        /*
         * TBD - currently does nothing
         */
    }


    /**
     * called when a connection is closed
     * <P>Packet:<B>GOODBYE</b></p>
     */
    public void goodbye() {
        // does nothing
    }

    /**
     * gets license information.
     * <P>Packet:<B>GET_LICENSE</b></p>
     *
     *@returns a hashtable with license info
     */

    public Hashtable getLicense() {
        // does nothing
       return new Hashtable();
    }

    /**
     * gets information about the broker/cluster.
     * <P>Packet:<B>INFO_REQUEST</b></p>
     *
     *@param cluster if true, return cluster information otherwise
     *               return local broker information.
     *@returns a hashtable with broker/cluster information
     */

    public Hashtable getInfo(boolean cluster) {
        // currently does nothing
        return new Hashtable();
    }

    /**
     * handles receiving a ping from a client.
     * <P>Packet:<B>PING</b></p>
     */

    public void ping() {
        // does nothing
    }


    /**
     * handles receiving a flow paused from a client.
     * <P>Packet:<B>FLOW_PAUSED</b></p>
     */
    public void flowPaused(int size) {
       throw new UnsupportedOperationException("flow paused is not supported by the client");
    }


    /**
     * Processes acknowledgements.
     * <P>Packet:<B>ACKNOWLEDGE</b></p>
     *
     *@param tid transaction id (or null if no transaction) associated
     *           with the acknowledgement.
     *@param validate  should the acks just be validated (normally false)
     *@param ackType acknowledge type of the request. One of <UL>
     *               <LI>ACKNOWLEDGE == 0 </LI><LI>UNDELIVERABLE==1</LI>
     *               <LI>DEAD</LI></UL>
     *@param exception exception associated with a dead message (should be null
     *                 if ackType != DEAD)
     *@param deadComment the explaination why a message was marked dead (should be null
     *               if ackType != DEAD)
     *@param deliverCnt number of times a dead message was delivered (should be 0
     *               if ackType != DEAD)
     *@param ids  list of message ids to process
     *@param cids list of consumerIDs associated with a message, should directly 
     *            correspond to the same index in ids
     */
    public void acknowledge(IMQConnection con, TransactionUID tid, 
                boolean validate,
                int ackType, Throwable exception, String deadComment,
                int deliverCnt, SysMessageID ids[], ConsumerUID cids[])
           throws BrokerException, IOException
    {
        if (DEBUG) {
        Globals.getLogger().log(Logger.INFO, 
        "ProtocolImpl.ACKNOWLEDGE:TID="+tid+", ackType="+ackType+", ids="+ids+
        ", cids="+cids+", validate="+validate+", deadComment="+deadComment+
        ", deliverCnt="+deliverCnt+", exception="+exception);
        }

        List cleanList = null;
        AckHandler handler = (AckHandler)
                      pr.getHandler(PacketType.ACKNOWLEDGE);
        if (validate) {
            if (ackType == handler.DEAD_REQUEST ||
                ackType == handler.UNDELIVERABLE_REQUEST) {
                throw new BrokerException("Can not use JMQValidate with"
                        + " an ackType of " + ackType,
                        null,
                        Status.BAD_REQUEST);
            } else if (tid == null) {
                throw new BrokerException("Can not use JMQValidate with"
                        + " no tid",
                        null,
                        Status.BAD_REQUEST);
            } else if (!handler.validateMessages(tid, ids, cids)) {
                throw new BrokerException("Acknowledgement could not be found",
                        null,
                        Status.NOT_FOUND);
            }
        } else if (ackType == handler.DEAD_REQUEST) {
            cleanList = handler.handleDeadMsgs(con, ids, cids,
                            handler.DEAD_REASON_UNDELIVERABLE,
                            exception, deadComment, deliverCnt);
        } else if (ackType == handler.UNDELIVERABLE_REQUEST) {
            cleanList = handler.handleUndeliverableMsgs(con, ids, cids);
        
        } else if (tid != null) {
            cleanList = handler.handleTransaction(con, tid, ids, cids);
        } else {
            cleanList = handler.handleAcks(con, ids, cids, true); //XXX ackack flag
        }

        // cleanup 

        handler.cleanUp(cleanList);
    }

    /**
     *sets/checks the clientID.
     * <P>Packet:<B>SET_CLIENTID</b></p>
     *
     *@param con the connection to set the clientID on
     *@param clientID the clientID to set
     *@param namespace to concatonate to clientID if shared
     *                 (generally set to null)
     *@param share true if the clientID is shared
     *@throws BrokerException if the clientId can not be set
     */

    public void setClientID(IMQConnection con, 
                String clientID, 
                String namespace, 
                boolean share)
        throws BrokerException
    {
        ClientIDHandler handler = (ClientIDHandler)
                      pr.getHandler(PacketType.SET_CLIENTID);
        handler.setClientID(con, clientID, namespace, share);
    }



    /**
     * creates a producer.
     * <P>Packet:<B>ADD_PRODUCER</b></p>
     *
     *@param d the destination to create the producer on
     *@param con the conectio to use for the producer
     *@param uid a unique string used for finding the producer
     *@throws BrokerException if the producer can not be created
     */
    public Producer addProducer(Destination d, IMQConnection con, String uid, boolean acc)
        throws BrokerException
    {
        if (acc)
            checkAccessPermission(PacketType.ADD_PRODUCER, d, con);
        ProducerHandler handler = (ProducerHandler)
                      pr.getHandler(PacketType.ADD_PRODUCER);
        return handler.addProducer(d.getDestinationUID(), con, uid, false);
    }

    /**
     * Destroys a producer.
     * <P>Packet:<B>REMOVE_PRODUCER</b></p>
     *
     *@param d the destination to create the producer on
     *@param con the conectio to use for the producer
     *@param uid a unique string used for finding the producer
     *@throws BrokerException if the producer can not be created
     */
    public void removeProducer(ProducerUID uid, IMQConnection con,
              String suid)
        throws BrokerException
    {
        ProducerHandler handler = (ProducerHandler)
                      pr.getHandler(PacketType.ADD_PRODUCER);
        handler.removeProducer(uid, false, con, suid);
    }

    public void resumeFlow(IMQConnection con, int bufsize)
    {
        FlowHandler handler = (FlowHandler)
                      pr.getHandler(PacketType.RESUME_FLOW);
        handler.connectionFlow(con, bufsize);
    }

    /**
     * resumes flow control on a connection.
     * <P>Packet:<B>RESUME_FLOW</b></p>
     *
     *@param bufsize size of the buffer to receive (-1 indicates unlimited)
     *@param con the consumer to resume
     */
    public void resumeFlow(Consumer con, int bufsize)
    {
        FlowHandler handler = (FlowHandler)
                      pr.getHandler(PacketType.RESUME_FLOW);
        handler.consumerFlow(con, bufsize);
    }

    /**
     * mimics the behavior of DELIVER
     * <P>Packet:<B>DELIVER</b></p>
     *
     *@param cid consumer id to attach to the messages
     *@param ids a list of id's to deliver
     *@return an ArrayList of packets
     */
    public ArrayList deliver(long cid, ArrayList ids)
        throws BrokerException, IOException
    {
        ArrayList returnlist = new ArrayList();
        Iterator itr = ids.iterator();
        while (itr.hasNext()) {
            SysMessageID id = (SysMessageID)itr.next();
            PacketReference ref = Destination.get(id);
            if (ref == null) continue;
            Packet realp = ref.getPacket();
            if (ref.isInvalid()) continue;
            Packet p = new Packet(false /* use direct buffers */);
            p.fill(realp);
            p.setConsumerID(cid);
            returnlist.add(p);
        }
        return returnlist;
    }
    
    /**
     * mimics the behavior of REDELIVER. It retrieves the messages
     * in a similar way but does not retrieve
     * <P>Packet:<B>REDELIVER</b></p>
     *The consumer should stop sessions before requeueing the
     *messages.
     *
     *@param cids consumer id to attach to the messages
     *@param ids a list of id's to deliver
     *@return an ArrayList of packets
     */
    public ArrayList redeliver(TransactionUID tid, ConsumerUID cids[],
            SysMessageID[] ids, boolean redeliver)
        throws BrokerException
    {
        // does nothing currently
        //XXX - TBD
        return new ArrayList();
    }

    

     /**
      * Creates a session and attaches it to the connection
      * <P>Packet:<B>CREATE_SESSION</b></p>
      *@param ackType acknowledge type to use
      *@param con connection to attach the session to
      */
     public Session createSession(int ackType, IMQConnection con)
        throws BrokerException
     {

        Object obj = new Object();
    
        SessionHandler handler = (SessionHandler)
                      pr.getHandler(PacketType.CREATE_SESSION);
    
        return handler.createSession(ackType, obj.toString(), con,
             false);
     }

     /**
      * Destroy a session 
      * <P>Packet:<B>DESTROY_SESSION</b></p>
      *@param uid sessionUID to destroy
      *@param con connection to deattach the session from
      */
     public void destroySession(SessionUID uid, IMQConnection con)
        throws BrokerException
     {
        SessionHandler handler = (SessionHandler)
                      pr.getHandler(PacketType.CREATE_SESSION);
    
        handler.closeSession(uid, con, false);
     }

     /**
      *Pause a session
      *<P>Packet:<B>STOP</b></p>
      *@param uid session to pause
      */
     public void pauseSession(SessionUID uid)
        throws BrokerException
     {
         Session ses = Session.getSession(uid);
         if (ses == null)
             throw new BrokerException("No session for " + uid);
         ses.pause("PROTOCOL");
     }

     /**
      *Resume a session
      *<P>Packet:<B>START</b></p>
      *@param uid session to resume
      */
     public void resumeSession(SessionUID uid)
        throws BrokerException
     {
         Session ses = Session.getSession(uid);
         if (ses == null)
             throw new BrokerException("No session for " + uid);
         ses.resume("PROTOCOL");
     }

     /**
      *Pause a connection
      *<P>Packet:<B>STOP</b></p>
      *@param con connection to pause
      */
     public void pauseConnection(IMQConnection con)
     {
         con.stopConnection();
     }

     /**
      *Resume a connection
      *<P>Packet:<B>START</b></p>
      *@param con connection to start
      */
     public void resumeConnection(IMQConnection con)
     {
         con.startConnection();
     }

     /**
      * Browse a queue
      * <P>Packet:<b>BROWSE</b></p>
      *@param d destination to browse
      *@param sstr selector string to use (or null if none)
      *@return an ordered list of SysMessageIDs
      */
      public ArrayList browseQueue(Destination d, String sstr, IMQConnection con, boolean acc)
          throws BrokerException,SelectorFormatException
      {
          if (acc) 
             checkAccessPermission(PacketType.BROWSE, d, con);
          QBrowseHandler handler = (QBrowseHandler)
                      pr.getHandler(PacketType.BROWSE);
          return handler.getQBrowseList(d, sstr);
      }


      /**
       * Create a consumer
       * <P>Packet:<b>ADD_CONSUMER</b></p>
       *@param d Destination to create the consumer on
       *@param con Connection associated with the consumer
       *@param session session associated with the consumer
       *@param selector selector string (or null if none)
       *@param clientid clientid or null if none
       *@param durablename durable name or null if none
       *@param nolocal is NoLocal turned on (topics only)
       *@param size prefetch size (or -1 if none)
       *@param shared is this a shared connection
       *@param creator_uid a unique id to use as the creator for this consumer
       *                   which is used for indempotence (usually sysmessageid)
       *@return a consumer
       */
      public Consumer createConsumer(Destination d, IMQConnection con,
                        Session session, String selector, String clientid,
                        String durablename, boolean nolocal, int size,
                        boolean shared, String creator_uid, boolean acc, boolean useFlowControl)
        throws BrokerException, SelectorFormatException, IOException
      {
          if (acc)
              checkAccessPermission(PacketType.ADD_CONSUMER, d, con);
          ConsumerHandler handler = (ConsumerHandler)
                      pr.getHandler(PacketType.ADD_CONSUMER);
          Consumer[] c = handler.createConsumer(d.getDestinationUID(), con,
                       session, selector, clientid, durablename,
                       nolocal, size, shared, creator_uid, false, useFlowControl);
          if (c[2] != null)
              c[2].resume("Resuming from protocol");
          if (c[1] != null)
              c[1].resume("Resuming from protocol");
          return c[0];
      }
        

      /**
       * Destroys a durable subscription
       * <P>Packet:<b>DELETE_CONSUMER</b></P>
       *@param durableName durable name associated with the subscription
       *@param clientID clientID associated with the subscription
       */
      public void unsubscribe(String durableName, String clientID)
          throws BrokerException
      {
          ConsumerHandler handler = (ConsumerHandler)
                      pr.getHandler(PacketType.ADD_CONSUMER);
          handler.destroyConsumer(null, null, null,
                  durableName, clientID, null, false, false);
      }

      /**
       * Closes a consumer
       * <P>Packet:<b>DELETE_CONSUMER</b></P>
       *@param uid ConsumerUID to close.
       *@param session session associated with the consumer.
       *@param con Connection associated with the consumer (used
       *          for retrieving protocol version).
       */
      public void destroyConsumer(ConsumerUID uid, Session session,
            IMQConnection con)
          throws BrokerException
      {
          ConsumerHandler handler = (ConsumerHandler)
                      pr.getHandler(PacketType.ADD_CONSUMER);
          handler.destroyConsumer(con, session, uid,
                  null, null, null, true, false);
      }


    /**
     * End a transaction.
     * @param id  The TransactionUID to end
     * @param xid The Xid of the transaction to end. Required if transaction
     *            is an XA transaction. Must be null if it is not an XA
     *            transaction.
     * @param xaFlags  xaFlags passed on END operation. Used only if
     *                 an XA transaction.
     */
     public void endTransaction(TransactionUID id, JMQXid xid,
              Integer xaFlags)
          throws BrokerException
     {
          if (DEBUG) {
          Globals.getLogger().log(Logger.INFO,
          "ProtocolImpl.END TRANSACTION:TID="+id+", XID="+xid+", xaFlags="+xaFlags);
          }
          
          TransactionHandler handler = (TransactionHandler)
                      pr.getHandler(PacketType.START_TRANSACTION);
          TransactionState ts = handler.getTransactionList().retrieveState(id);

          handler.doEnd(PacketType.END_TRANSACTION, xid,
                  xaFlags, ts, id);

     }

    /**
     * Start a transaction.
     * @param xid The Xid of the transaction to start. Required if transaction
     *            is an XA transaction. Must be null if it is not an XA
     *            transaction.
     * @param xaFlags  xaFlags passed on START operation. Used only if
     *                 an XA transaction.
     * @param con       Connection client start packet came in on (or null if internal)
     * @param type  how rollback should be handled (e.g. only not prepared)
     * @param lifetime how long the transaction should live (0 == forever)
     * @return The TransactionUID started
     */
     public TransactionUID startTransaction(JMQXid xid,
              Integer xaFlags, AutoRollbackType type, long lifetime, IMQConnection con)
          throws BrokerException
     {
          if (DEBUG) {
          Globals.getLogger().log(Logger.INFO,
          "ProtocolImpl.START TRANSACTION:XID="+xid+", type="+type+", conn="+con);
          }
          
          List conlist = con.getTransactionListThreadSafe();

          TransactionHandler handler = (TransactionHandler)
                      pr.getHandler(PacketType.START_TRANSACTION);

          // allocated a TID
          TransactionUID id = null;
          if (xaFlags == null || 
              TransactionState.isFlagSet(XAResource.TMNOFLAGS, xaFlags)) {
              id = new TransactionUID();
          } else if (xid != null) {
              TransactionList translist = handler.getTransactionList();

              id = translist.xidToUID(xid);
          
          } else { // XID is null, something is wrong
              throw new BrokerException("Invalid xid");
          }
          

          Object o = new Object();
          handler.doStart(id, conlist, con, 
                  type, xid, xid!= null, lifetime, 0,
                        xaFlags, PacketType.START_TRANSACTION, 
                        false, o.toString());
          if (DEBUG) {
          Globals.getLogger().log(Logger.INFO, 
          "ProtocolImpl.STARTED TRANSACTION:TID="+id+", XID="+xid+", type="+type+", con="+con);
          }

          return id;

     }

    /**
     * Commit a transaction.
     * @param id  The TransactionUID to commit
     * @param xid The Xid of the transaction to commit. Required if transaction
     *            is an XA transaction. Must be null if it is not an XA
     *            transaction.
     * @param xaFlags  xaFlags passed on COMMIT operation. Used only if
     *                 an XA transaction.
     * @param con       Connection client commit packet came in on (or null if internal)
     */
     public void commitTransaction(TransactionUID id, JMQXid xid,
              Integer xaFlags, IMQConnection con)
          throws BrokerException
     {
          if (DEBUG) {
          Globals.getLogger().log(Logger.INFO,
          "ProtocolImpl.COMMIT TRANSACTION:TID="+id+", XID="+xid+", xaFlags="+xaFlags);
          }
          
          List conlist = con.getTransactionListThreadSafe();

          TransactionHandler handler = (TransactionHandler)
                      pr.getHandler(PacketType.START_TRANSACTION);

          if (0L == id.longValue()) {
              if (xid == null) {
                  throw new BrokerException("Unexpected TransactionUID  " + id);
              }
              id = handler.getTransactionList().xidToUID(xid);
              if (id == null) {
                  throw new BrokerException("Unknown XID " + xid, Status.NOT_FOUND);
              }
          }

          TransactionState ts = handler.getTransactionList().retrieveState(id);

          if (ts == null) {
              throw new BrokerException(
              "Unknown transaction "+id+(xid == null ? "":" XID="+xid), Status.NOT_FOUND);
          }
          if (xid != null) {
              if (ts.getXid() == null || !xid.equals(ts.getXid())) {
                  throw new BrokerException(
                  "Transaction XID mismatch "+xid+", expected "+ts.getXid()+" for transaction "+id);
              }
          }

          handler.doCommit(id, xid, xaFlags, ts, conlist, false,con, null);
     }

    /**
     * prepare a transaction.
     * @param id  The TransactionUID to prepare
     * @param xaFlags  xaFlags passed on PREPARE operation. Used only if
     *                 an XA transaction.
     */
     public void prepareTransaction(TransactionUID id, Integer xaFlags)
          throws BrokerException
     {
          if (DEBUG) {
          Globals.getLogger().log(Logger.INFO,
          "ProtocolImpl.PREPARE TRANSACTION:TID="+id+", xaFlags="+xaFlags);
          }

          TransactionHandler handler = (TransactionHandler)
                      pr.getHandler(PacketType.START_TRANSACTION);

          TransactionState ts = handler.getTransactionList().retrieveState(id);
          handler.doPrepare(id, xaFlags, ts, PacketType.PREPARE_TRANSACTION);
     }


    /**
     * Rollback a transaction
     * @param id  The TransactionUID to rollback
     * @param xid The Xid of the transaction to rollback. Required if transaction
     *            is an XA transaction. Must be null if it is not an XA
     *            transaction.
     * @param redeliver should messages be redelivered
     * @param setRedeliver if the messages are redelivered, should the redeliver 
     *                     flag be set on all messages or not
     * @param xaFlags  xaFlags passed on ROLLBACK operation. Used only if
     *                 an XA transaction.
     * @param con       Connection client rollback packet came in on (or null if internal)
     */
     public void rollbackTransaction(TransactionUID id, JMQXid xid,
              Integer xaFlags, IMQConnection con, boolean redeliver, boolean setRedeliver)
          throws BrokerException
     {
          if (DEBUG) {
          Globals.getLogger().log(Logger.INFO, 
          "ProtocolImpl.ROLLBACK TRANSACTION:TID="+id+", XID="+xid+
          ", xaFlags="+xaFlags+", redeliver="+redeliver+", setRedeliver="+setRedeliver);
          }

          List conlist = con.getTransactionListThreadSafe();

          TransactionHandler handler = (TransactionHandler)
                      pr.getHandler(PacketType.START_TRANSACTION);


          if (0L == id.longValue()) {
              if (xid == null) {
                  throw new BrokerException("Unexpected TransactionUID  " + id);
              }
              id = handler.getTransactionList().xidToUID(xid);
              if (id == null) {
                  throw new BrokerException("Unknown XID " + xid, Status.NOT_FOUND);
              }
          }

          TransactionState ts = handler.getTransactionList().retrieveState(id);

          if (ts == null) {
              throw new BrokerException(
              "Unknown transaction "+id+(xid == null ? "":" XID="+xid), Status.NOT_FOUND);
          }
          if (xid != null) {
              if (ts.getXid() == null || !xid.equals(ts.getXid())) {
                  throw new BrokerException(
                  "Transaction XID mismatch "+xid+", expected "+ts.getXid()+" for transaction "+id);
              }
          }

          handler.preRollback(id, xid, xaFlags, ts);

          if (redeliver) {
              handler.redeliverUnacked(id, true, setRedeliver);
          }

          handler.doRollback(id, xid, xaFlags, ts, conlist, con,  RollbackReason.APPLICATION);
     }


     /**
      * Recover a transaction.
      *@param id id to recover or null if all
      */
     public JMQXid[] recoverTransaction(TransactionUID id)
     {
         if (DEBUG) {
         Globals.getLogger().log(Logger.INFO, "ProtocolImpl.RECOVER TRANSACTION:TID="+id);
         }

         TransactionHandler handler = (TransactionHandler)
                      pr.getHandler(PacketType.START_TRANSACTION);

         TransactionList translist = handler.getTransactionList();
         Vector v = null;
         if (id == null) {
             v = translist.getTransactions(TransactionState.PREPARED);
         
         } else { // look at a single transaction
             v = new Vector();
             TransactionState ts = translist.retrieveState(id);
             if (ts.getState() == TransactionState.PREPARED) {
                 v.add(id);
             }
         }

         JMQXid xids[] = new JMQXid[v.size()];
         Iterator itr = v.iterator();
         int i = 0;
         while (itr.hasNext()) {
             TransactionUID tuid = (TransactionUID)itr.next();
             TransactionState _ts = translist.retrieveState(tuid);
             if (_ts == null) {
                 // Should never happen
                 continue;
             }
             JMQXid _xid = _ts.getXid();
             xids[i++]=_xid;

         }
         return xids;
     }

    /**
     * Verify a destination exists.
     * @param destination destination name
     * @param type DestType of the destination
     * @param selectorstr selector string to verify or null if none
     * @see com.sun.messaging.jmq.util.DestType
     * @return a hashmap which contains the following
     *  entries:<UL><LI>JMQStatus</LI><LI>JMQCanCreate</LI><LI>DestType</LI></UL>
     */
     public HashMap verifyDestination(String destination,
               int type, String selectorstr /* may be null */)
        throws BrokerException, IOException
     {

         HashMap returnmap = new HashMap();
         try {
             if (selectorstr != null) {
                Selector selector = Selector.compile(selectorstr);
                selector = null;
             }
         } catch (SelectorFormatException ex) {
              returnmap.put("JMQStatus", new Integer(Status.BAD_REQUEST));
              return returnmap;
         }
         
         Destination d =  Destination.getDestination(destination,
                       DestType.isQueue(type));

         if (d == null) {
             returnmap.put("JMQCanCreate", Boolean.valueOf(
                   Destination.canAutoCreate(
                        DestType.isQueue(type))));
             returnmap.put("JMQStatus", new Integer(Status.NOT_FOUND));
         } else {
             returnmap.put("JMQDestType", new Integer(d.getType()));
             returnmap.put("JMQStatus", new Integer(Status.OK));
         }
         return returnmap;
     }

     /**
      * Verify a transaction is PREPARED
      * @param tuid transaction id to verify
      */
     public Map verifyTransaction(TransactionUID tuid)
         throws BrokerException
     {
         TransactionHandler handler = (TransactionHandler)
                      pr.getHandler(PacketType.START_TRANSACTION);
         TransactionList translist = handler.getTransactionList();
         TransactionState ts = translist.retrieveState(tuid, true);
         if (ts == null) return null; // GONE
         int realstate = ts.getState();

         if (realstate != TransactionState.PREPARED) {
             return null; // GONE
         }
         return  translist.getTransactionMap(tuid, true);
     }


     /**
      * Redeliver messages
      */
     public void redeliver(ConsumerUID ids[], SysMessageID sysids[], 
               IMQConnection con, TransactionUID tid, boolean redeliver)
         throws BrokerException, IOException
     {
         RedeliverHandler handler = (RedeliverHandler)
                      pr.getHandler(PacketType.REDELIVER);
         handler.redeliver(ids, sysids, con, tid, redeliver);
     }


     /**
      * route, store and forward a message
      */
      public void processMessage(IMQConnection con, Packet msg)
          throws BrokerException, SelectorFormatException, IOException
      {
          DataHandler handler = (DataHandler)
                      pr.getHandler(PacketType.MESSAGE);

          Destination d = null;
          PacketReference ref = null;
          Set s = null;
          boolean route = false;
          boolean isadmin = con.isAdminConnection();
          try {
               d = Destination.getDestination(
                      msg.getDestination(), msg.getIsQueue());
               if (d == null) {
                   throw new BrokerException("Unknown Destination:" + msg.getDestination());
               }
    
               Producer pausedProducer = handler.checkFlow(msg, con);
               boolean transacted = (msg.getTransactionID() != 0);
               if (DEBUG) {
               Globals.getLogger().log(Logger.INFO, 
               "ProtocolImpl.PROCESS MESSAGE["+msg+"]TID="+msg.getTransactionID()+" on connection "+con);
               }
    
                // OK generate a ref. This checks message size and
                // will be needed for later operations
                ref = handler.createReference(msg, d.getDestinationUID(), con, isadmin);
    
                // dont bother calling route if there are no messages
                //
                // to improve performance, we route and later forward
                route = handler.queueMessage(d, ref, transacted);
    
                s = handler.routeMessage(transacted, ref, route, d);
    
                // handle producer flow control
                handler.pauseProducer(d, pausedProducer, con);
    
         } catch (BrokerException ex) {
            int status = ex.getStatusCode();
            if (status == Status.ERROR && ref!= null && d != null)
                handler.cleanupOnError(d, ref);
    
            // rethrow
            throw ex;
         }
    
         if (route && d != null && s != null) {
            handler.forwardMessage(d, ref, s);
         }
     

      }


     /**
      * create a destination
      * Implemented CREATE_DESTINATION
      * @param dname name of the destination
      * @param dtype type of the destination as a bit flag from DestType
      */

    public Destination createDestination(String dname, int dtype, IMQConnection con, boolean acc)
        throws BrokerException, IOException
    {
        if (acc)
            checkAccessPermission(PacketType.CREATE_DESTINATION, dname, dtype, con);
        if (DestType.isTemporary(dtype)) {
            boolean storeTemps = con.getConnectionUID().
                            getCanReconnect();
            long reconnectTime = con.getReconnectInterval();
            Destination d = Destination.createTempDestination(
                         dname, dtype, con.getConnectionUID(), 
                         storeTemps, reconnectTime);
            if (con.getConnectionUID().equals(d.getConnectionUID())) {
                        con.attachTempDestination(d.getDestinationUID());
            }
            return d;

        }
        return Destination.getDestination(dname, dtype, true,
                   !con.isAdminConnection());
    }

    /**
     * destroy a destination
     * Implemented DESTROY_DESTINATION
     */
    public void destroyDestination(DestinationUID duid)
         throws BrokerException , IOException
    {
        Destination.removeDestination(duid, true,
               "request from protocol");
    }

    

    void checkAccessPermission(int pktType, Destination d, IMQConnection con)
                    throws AccessControlException,
                           BrokerException
    {

        checkAccessPermission(pktType, d.getName(), d.getType(), con);

    } 
    void checkAccessPermission(int pktType, String dname, int dtype, IMQConnection con)
                    throws AccessControlException, 
                           BrokerException
    {
        String op = PacketType.mapOperation(pktType);
        if (op == null) return;
        PacketHandler.checkPermission(pktType, op, dname, dtype,
                    con);
        
    }
           
}
