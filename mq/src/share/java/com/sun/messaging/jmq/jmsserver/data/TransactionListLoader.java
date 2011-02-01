/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2010 Oracle and/or its affiliates. All rights reserved.
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

package com.sun.messaging.jmq.jmsserver.data;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;
import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.util.DestType;
import com.sun.messaging.jmq.util.log.Logger;

public class TransactionListLoader {

	static Logger logger = Globals.getLogger();
	static boolean loaded = false;


	// txn log

	public static void loadTransactions(Store store, TransactionList transactionList) throws BrokerException, IOException {
		logger.log(Logger.INFO, BrokerResources.I_PROCESSING_TRANS);
		// This method is called after the message store has been loaded
		// and after the transaction log has been replayed.

		// The state of the system should be :
		// 1) All committed transactions since the last checkpoint should
		// have been replayed and persisted to the message store.
		// 2) There should be no incomplete transactions
		// (a partial write of a transaction will have been discarded).
		// 3) The message store will contain a set of prepared transactions
		// either from XA or from clustered/remote transactions.
		loadLocalTransactions(store,transactionList);
		loadClusterTransactions(store,transactionList);
		loadRemoteTransactions(store,transactionList);

	}
	
	
	
	public static void rollbackAllTransactions(Store store)
	{
		logger.log(Logger.INFO, "rolling back all transactions");
		store.rollbackAllTransactions();
		
	}
	
	
	
	private static void loadDestinations() throws BrokerException {
		if (!loaded) {
			loaded = true;
			Destination.loadDestinations();
			Iterator itr = Destination.getAllDestinations();
			while (itr.hasNext()) {
				Destination d = (Destination) itr.next();
				d.load();
			}
		}
	}

	private static void loadLocalTransactions(Store store, TransactionList transactionList) throws BrokerException, IOException {

		List<BaseTransaction> incompleteTxns = store
				.getIncompleteTransactions(BaseTransaction.LOCAL_TRANSACTION_TYPE);

		String msg = " loading " + incompleteTxns.size()
				+ " incomplete Local transactions:  ";
		logger.log(Logger.DEBUG, msg);

		Iterator<BaseTransaction> iter = incompleteTxns.iterator();
		while (iter.hasNext()) {
			BaseTransaction baseTxn = iter.next();
			TransactionUID tid = baseTxn.getTid();

			msg = " loadTransactions: processing local transaction " + tid;
			logger.log(Logger.DEBUG, msg);

			TransactionState state = baseTxn.getTransactionState();
			transactionList.addTransactionID(tid, state, false);
			TransactionWork txnWork = baseTxn.getTransactionWork();
			handleTransactionWork(transactionList,tid, txnWork);
		}
	}

	private static void loadClusterTransactions(Store store, TransactionList transactionList) throws BrokerException, IOException {

		List<BaseTransaction> incompleteTxns = store
				.getIncompleteTransactions(BaseTransaction.CLUSTER_TRANSACTION_TYPE);
		String msg = " loading " + incompleteTxns.size()
				+ " incomplete cluster transactions:  ";
		logger.log(Logger.DEBUG, msg);
		Iterator<BaseTransaction> iter = incompleteTxns.iterator();
		while (iter.hasNext()) {
			ClusterTransaction clusterTxn = (ClusterTransaction) iter.next();
			TransactionUID tid = clusterTxn.getTid();

			msg = " loadTransactions: processing cluster transaction " + tid;
			logger.log(Logger.DEBUG, msg);

			TransactionState state = clusterTxn.getTransactionState();
			transactionList.addTransactionID(tid, state, false);

			TransactionBroker[] transactionBrokers = clusterTxn
					.getTransactionBrokers();
			transactionList.logClusterTransaction(tid, state,
					transactionBrokers, true, false);
			TransactionWork txnWork = clusterTxn.getTransactionWork();
			if (state.getState() == TransactionState.PREPARED) {
				handleTransactionWork(transactionList, tid, txnWork);
			} else if (state.getState() == TransactionState.COMMITTED) {
				// incomplete cluster transaction
				// the work has already been committed locally but
				// there may be remote brokers that have not confirmed they have
				// committed this transaction
				// Add to transaction reaper
				transactionList.removeTransaction(tid, false);
			}
				
		}
	}
	
	private static void loadRemoteTransactions(Store store, TransactionList transactionList) throws BrokerException, IOException {

		List<BaseTransaction> incompleteTxns = store
				.getIncompleteTransactions(BaseTransaction.REMOTE_TRANSACTION_TYPE);
		String msg = " loading " + incompleteTxns.size()
				+ " incomplete remote transactions:  ";
		logger.log(Logger.DEBUG, msg);
		Iterator<BaseTransaction> iter = incompleteTxns.iterator();
		while (iter.hasNext()) {
			RemoteTransaction remoteTxn = (RemoteTransaction)iter.next();
			TransactionUID tid = remoteTxn.getTid();

		
			TransactionState state = remoteTxn.getTransactionState();
			TransactionAcknowledgement  tas[] = remoteTxn.getTxnAcks();
			DestinationUID  destIds[] = remoteTxn.getDestIds();
			msg = " loadTransactions: processing remote transaction " + tid + " state= "+state;
			logger.log(Logger.DEBUG, msg);
			BrokerAddress remoteTransactionHomeBroker = remoteTxn.getTxnHomeBroker();
			
			transactionList.logRemoteTransaction(tid, state, tas, 
					remoteTransactionHomeBroker, true, true, false);
			
			for(int i=0;i<tas.length;i++)
			{
				TransactionAcknowledgement ta = tas[i];
				DestinationUID destId = destIds[i];
				unrouteLoadedTransactionAckMessage(destId,ta.getSysMessageID(),ta.getStoredConsumerUID());
			}
		}
	}

	static void  handleTransactionWork(TransactionList transactionList,TransactionUID tid, TransactionWork txnWork)
			throws BrokerException {
		
		
		
		handleSentMessages(transactionList, txnWork);
		handleMessageAcks(transactionList,tid,txnWork);
		
	}
	
	static void  handleSentMessages(TransactionList transactionList, TransactionWork txnWork) throws BrokerException
	{
		for (int i = 0; i < txnWork.numSentMessages(); i++) {

			TransactionWorkMessage msg = txnWork.getSentMessages().get(i);
			Packet packet = msg.getMessage();

			DestinationUID duid = msg.getDestUID();

			logger.log(Logger.DEBUG,
					" handleSentMessages: duid= "+duid);
						
			PacketReference pr = PacketReference.createReference(packet, duid,
					null);
			Destination d = Destination.getDestination(duid);
			if (d == null) {
				// Could be an auto-created dest that was reaped on load.
				// Lets recreate it here.
				try {
					int type = (duid.isQueue() ? DestType.DEST_TYPE_QUEUE
							: DestType.DEST_TYPE_TOPIC);
					d = Destination.getDestination(duid.getName(), type, true,
							true);
				} catch (IOException e) {
					throw new BrokerException("Could not recreate destination "
							+ duid, e);
				}
			}
			
			// check it is loaded
			d.load();
			
			logger.log(Logger.DEBUG,
					" loadTransactions: processing prepared sent message "
							+ packet.getMessageID()) ;

			// queue message
			boolean result = d.queueMessage(pr, true);

			// store (should not really be persisted as we are using txnLog)
			// pr.store();

			// add message to transaction
			transactionList.addMessage(pr.getTransactionID(), pr
					.getSysMessageID(), true);

		}
	}
	
	static void handleMessageAcks(TransactionList transactionList,TransactionUID tid, TransactionWork txnWork)
			throws BrokerException {
		for (int i = 0; i < txnWork.numMessageAcknowledgments(); i++) {
			TransactionWorkMessageAck msgAck = txnWork
					.getMessageAcknowledgments().get(i);			
			DestinationUID destID = msgAck.getDestUID();
			handleAck(transactionList,tid, destID, msgAck.getSysMessageID(), msgAck.getConsumerID());

		}
	}
	
	
	static void handleAck(TransactionList transactionList,TransactionUID tid, DestinationUID destID, SysMessageID ackedSysMsgID,
			ConsumerUID consumerID) throws BrokerException {
		logger.log(Logger.DEBUG,
				" loadTransactions: processing prepared acknowledged message "
						+ ackedSysMsgID);

		transactionList.addAcknowledgement(tid, ackedSysMsgID, consumerID,
				consumerID, true, false);
		transactionList
				.addOrphanAck(tid, ackedSysMsgID, consumerID, consumerID);

		unrouteLoadedTransactionAckMessage(destID, ackedSysMsgID, consumerID);

	}

	static void unrouteLoadedTransactionAckMessage(DestinationUID destID, SysMessageID ackedSysMsgID,
			ConsumerUID consumerID) throws BrokerException {
		logger.log(Logger.DEBUG,
				" trying to unroute prepared acknowledged message: destID =  "
						+ destID + " ackedMsgId=" + ackedSysMsgID);
		
		Destination dest = null;
		PacketReference ackedMessage = null;
		if (destID != null) {
			dest = Destination.getDestination(destID);
			if (dest != null) {
				dest.load();
				ackedMessage = dest.getMessage(ackedSysMsgID);
				if (ackedMessage == null) {

					String msg = "Could not find packet for " + ackedSysMsgID
							+ "in dest " + dest;
					logger.log(Logger.WARNING, msg);
					return;
				}
			} else {
				// this could happen e.g. if empty auto dest has been destroyed
				String msg = "Could not find destination for " + destID;
				logger.log(Logger.WARNING, msg);
				return;
			}

		} else {
			// no destid stored for some reason so need to load all dests
			logger.log(Logger.WARNING,
					"No dest ID for acked message. Will need to load all dests "
							+ ackedSysMsgID);
			loadDestinations();

			ackedMessage = Destination.get(ackedSysMsgID);
			dest = ackedMessage.getDestination();
		}

		// need to unroute messages that have been consumed in a prepared
		// transaction.
		// they cannot be redelivered unless the transaction rolls back.

		dest.unrouteLoadedTransactionAckMessage(ackedMessage, consumerID);

	}

}
