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
 * @(#)GetTransactionsHandler.java	1.15 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.data.handlers.admin;

import java.util.Hashtable;
import java.io.IOException;
import java.io.*;
import java.util.Vector;
import java.util.List;
import java.util.Enumeration;

import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.io.*;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.data.TransactionList;
import com.sun.messaging.jmq.jmsserver.data.TransactionBroker;
import com.sun.messaging.jmq.jmsserver.service.ConnectionUID;
import com.sun.messaging.jmq.util.JMQXid;
import com.sun.messaging.jmq.util.admin.MessageType;
import com.sun.messaging.jmq.util.admin.ServiceInfo;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.Globals;

public class GetTransactionsHandler extends AdminCmdHandler
{

    private static boolean DEBUG = getDEBUG();

    /*
     * Transaction types
     */
    private static int LOCAL   = 0;
    private static int CLUSTER    = 1;
    private static int REMOTE   = 2;
    private static int UNKNOWN  = -1;

    TransactionList tl = null;

    public GetTransactionsHandler(AdminDataHandler parent) {
	super(parent);
        tl = parent.tl;
    }

    public static Hashtable getTransactionInfo(TransactionList tl,
                                               TransactionUID id,
					       int type) {
                                               
	Logger logger = Globals.getLogger();
        TransactionState ts = tl.retrieveState(id);

	if (type == LOCAL) {
            ts = tl.retrieveState(id, true);
	} else if (type == CLUSTER)  {
            ts = tl.retrieveState(id, true);
	} else if (type == REMOTE)  {
            ts = tl.getRemoteTransactionState(id);
	}


        if (ts == null) return null;

        JMQXid xid = tl.UIDToXid(id);

        Hashtable table = new Hashtable();

	table.put("type", new Integer(type));

        if (xid != null) {
            table.put("xid", xid.toString());
        }

        table.put("txnid", new Long(id.longValue()));
        if (ts.getUser() != null)
            table.put("user", ts.getUser());

        if (ts.getClientID() != null) {
            table.put("clientid", ts.getClientID());
        }
        table.put("timestamp",
                new Long(System.currentTimeMillis() - id.age()));
        table.put("connection", ts.getConnectionString());

        table.put("nmsgs", new Integer(tl.retrieveNSentMessages(id)));
        if (type != REMOTE) {
            table.put("nacks", new Integer(tl.retrieveNConsumedMessages(id)));
        } else {
            table.put("nacks", new Integer(tl.retrieveNRemoteConsumedMessages(id)));
        }
        table.put("state", new Integer(ts.getState()));

	ConnectionUID cuid = ts.getConnectionUID();
	if (cuid != null)  {
	    table.put("connectionid", new Long(cuid.longValue()));
	}

	TransactionBroker homeBroker = tl.getRemoteTransactionHomeBroker(id);
	String homeBrokerStr = "";
	if (homeBroker != null)  {
	    homeBrokerStr = homeBroker.getBrokerAddress().toString();
	}
        table.put("homebroker", homeBrokerStr);

	    TransactionBroker brokers[] = null;

        if (type != REMOTE) { 
            try {
                brokers = tl.getClusterTransactionBrokers(id);
            } catch (BrokerException be)  {
                logger.log(Logger.WARNING, 
                "Exception caught while obtaining list of brokers in transaction", be);
            }
        }

	    String allBrokers = "", pendingBrokers = "";

	    if (brokers != null)  {
		for  (int i = 0; i < brokers.length; ++i)  {
	            TransactionBroker oneBroker = brokers[i];
		    
		    BrokerAddress addr = oneBroker.getBrokerAddress();

		    if (allBrokers.length() != 0)  {
		        allBrokers += ", ";
		    }
		    allBrokers += addr.toString();

		    if (oneBroker.isCompleted())  {
			continue;
		    }

		    if (pendingBrokers.length() != 0)  {
		        pendingBrokers += ", ";
		    }
		    pendingBrokers += addr.toString();
		}
	    }

	    table.put("allbrokers", allBrokers);
	    table.put("pendingbrokers", pendingBrokers);

        return table;
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

	if ( DEBUG ) {
            logger.log(Logger.DEBUG, this.getClass().getName() + ": " +
                            "Getting transactions " + cmd_props);
        }

        int status = Status.OK;
	String errMsg = null;
        TransactionUID tid = null;

	Long id = (Long)cmd_props.get(MessageType.JMQ_TRANSACTION_ID);

        if (id != null) {
            tid = new TransactionUID(id.longValue());
        }

        Hashtable table = null;
	Vector v = new Vector();
        if (tid != null) {
            // Get info for one transaction
	    int type = UNKNOWN;

	    if (tl.isRemoteTransaction(tid))  {
		type = REMOTE;
	    } else if (tl.isClusterTransaction(tid))  {
		type = CLUSTER;
	    } else if (tl.isLocalTransaction(tid))  {
		type = LOCAL;
	    }

            table = getTransactionInfo(tl, tid, type);
            if (table != null) {
                v.add(table);
            }
        } else {
            // Get info for all transactions (-1 means get transactions
            // no matter what the state).

	    /*
	     * Get list of local transactions
	     */
            TransactionUID ttid = null;  

            Vector transactions = tl.getTransactions(-1);
            if (transactions != null) {
                Enumeration e = transactions.elements();
                while (e.hasMoreElements()) {
                    ttid = (TransactionUID)e.nextElement();
                    table = getTransactionInfo(tl, ttid, LOCAL);
                    if (table != null) {
                        v.add(table);
                    }
                }
            }

	    /*
	     * Get list of cluster transactions
	     */
            transactions = tl.getClusterTransactions(-1);
            if (transactions != null) {
                Enumeration e = transactions.elements();
                while (e.hasMoreElements()) {
                    ttid = (TransactionUID)e.nextElement();
                    table = getTransactionInfo(tl, ttid, CLUSTER);
                    if (table != null) {
                        v.add(table);
                    }
                }
            }

	    /*
	     * Get list of remote transactions
	     */
            transactions = tl.getRemoteTransactions(-1);
            if (transactions != null) {
                Enumeration e = transactions.elements();
                while (e.hasMoreElements()) {
                    ttid = (TransactionUID)e.nextElement();
                    table = getTransactionInfo(tl, ttid, REMOTE);
                    if (table != null) {
                        v.add(table);
                    }
                }
            }
        }

        if (tid != null && v.size() == 0) {
            // Specified transaction did not exist
            status = Status.NOT_FOUND;
	    errMsg = rb.getString(rb.E_NO_SUCH_TRANSACTION, tid);
        }

        // Write reply
	Packet reply = new Packet(con.useDirectBuffers());
	reply.setPacketType(PacketType.OBJECT_MESSAGE);

	setProperties(reply, MessageType.GET_TRANSACTIONS_REPLY, status, errMsg);
        // Add JMQQuantity property
        try {
            reply.getProperties().put(MessageType.JMQ_QUANTITY,
                            new Integer(v.size()));
        } catch (IOException e) {
	    // Programming error. No need to I18N
	    logger.log(Logger.WARNING, rb.E_INTERNAL_BROKER_ERROR,
                "Admin: GetTransactions: Could not extract properties from pkt",
                e);
        } catch (ClassNotFoundException e) {
	    // Programming error. No need to I18N
	    logger.log(Logger.WARNING, rb.E_INTERNAL_BROKER_ERROR,
                "Admin: GetTransactions: Could not extract properties from pkt",
                e);
        }

        // Always set the body even if vector is empty
	setBodyObject(reply, v);

	parent.sendReply(con, cmd_msg, reply);
        return true;
    }
}
