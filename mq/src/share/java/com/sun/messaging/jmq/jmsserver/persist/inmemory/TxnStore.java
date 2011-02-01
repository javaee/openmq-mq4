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
 * @(#)TxnStore.java	1.39 08/30/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.inmemory;

import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.io.Status;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.data.TransactionAcknowledgement;
import com.sun.messaging.jmq.jmsserver.data.TransactionBroker;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.persist.TransactionInfo;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Keep track of all persisted transaction states & acknowledgements by using HashMap.
 */
class TxnStore {

    Logger logger = Globals.getLogger();
    BrokerResources br = Globals.getBrokerResources();

    // cache all persisted transaction ids
    // maps tid -> txn info
    private ConcurrentHashMap tidMap = null;

    // maps tid -> HashSet of TransactionAcknowledgement
    private ConcurrentHashMap ackMap = null;

    private HashMap emptyHashMap = new HashMap();
    private TransactionAcknowledgement[] emptyAckArray =
        new TransactionAcknowledgement[0];

    // when instantiated, all data are loaded
    TxnStore() {

        tidMap = new ConcurrentHashMap(1024);
	ackMap = new ConcurrentHashMap(1024);
    }

    void close(boolean cleanup) {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH,
		"TxnStore: closing, " + tidMap.size() +
                " in-memory transactions");
	}

	tidMap.clear();

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUGHIGH,
                "TxnStore: closing, " + ackMap.size() +
                " in-memory transactions with acks");
        }

	ackMap.clear();
    }

    /**
     * Store a transaction.
     *
     * @param id	the id of the transaction to be persisted
     * @param ts	the transaction's state to be persisted
     * @exception BrokerException if an error occurs while persisting or
     * the same transaction id exists the store already
     * @exception NullPointerException	if <code>id</code> is <code>null</code>
     */
    void storeTransaction(TransactionUID id, TransactionState ts, boolean sync)
	throws IOException, BrokerException {

        try {
            // TransactionState is mutable, so we must store a copy
            // See bug 4989708
            Object oldValue = tidMap.putIfAbsent(
                id, new TransactionInfo(new TransactionState(ts)));

            if (oldValue != null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_TRANSACTIONID_EXISTS_IN_STORE, id);
                throw new BrokerException(br.getString(
                    BrokerResources.E_TRANSACTIONID_EXISTS_IN_STORE, id));
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR,
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, id, e);
            throw new BrokerException(br.getString(
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, id), e);
        }
    }

    /**
     * Store a cluster transaction.
     *
     * @param id	the id of the transaction to be persisted
     * @param ts	the transaction's state to be persisted
     * @param brokers	the transaction's participant brokers
     * @exception BrokerException if an error occurs while persisting or
     * the same transaction id exists the store already
     * @exception NullPointerException	if <code>id</code> is <code>null</code>
     */
    public void storeClusterTransaction(TransactionUID id, TransactionState ts,
        TransactionBroker[] brokers, boolean sync) throws BrokerException {

        TransactionInfo txnInfo = null;
        try {
            // TransactionState is mutable, so we must store a copy
            // See bug 4989708
            txnInfo = new TransactionInfo(new TransactionState(ts), null,
                brokers, TransactionInfo.TXN_CLUSTER);

            Object oldValue = tidMap.putIfAbsent(id, txnInfo);
            if (oldValue != null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_TRANSACTIONID_EXISTS_IN_STORE, id);
                throw new BrokerException(br.getString(
                    BrokerResources.E_TRANSACTIONID_EXISTS_IN_STORE, id));
            }
        } catch (RuntimeException e) {
            String msg = (txnInfo != null) ?
                id + " " + txnInfo.toString() : id.toString();
            logger.log(Logger.ERROR,
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, msg, e);
            throw new BrokerException(br.getString(
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, msg), e);
        }
    }

    /**
     * Store a remote transaction.
     *
     * @param id	the id of the transaction to be persisted
     * @param ts	the transaction's state to be persisted
     * @param acks	the transaction's participant brokers
     * @param txnHomeBroker the transaction's home broker
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if an error occurs while persisting or
     * the same transaction id exists the store already
     * @exception NullPointerException	if <code>id</code> is <code>null</code>
     */
    public void storeRemoteTransaction(TransactionUID id, TransactionState ts,
        TransactionAcknowledgement[] acks, BrokerAddress txnHomeBroker,
        boolean sync) throws BrokerException {

        TransactionInfo txnInfo = null;
        boolean removedAcksFlag = false;
        try {
            if (tidMap.containsKey(id)) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_TRANSACTIONID_EXISTS_IN_STORE, id);
                throw new BrokerException(br.getString(
                    BrokerResources.E_TRANSACTIONID_EXISTS_IN_STORE, id));
            }

            // We must store the acks first if provided
            if (acks != null && acks.length > 0) {
                storeTransactionAcks(id, acks);
                removedAcksFlag = true;
            }

            // Now we store the txn;
            // TransactionState is mutable, so we must store a copy
            txnInfo = new TransactionInfo(new TransactionState(ts), txnHomeBroker,
                    null, TransactionInfo.TXN_REMOTE);
            tidMap.put(id, txnInfo);
        } catch (RuntimeException e) {
            String msg = (txnInfo != null) ?
                id + " " + txnInfo.toString() : id.toString();
            logger.log(Logger.ERROR,
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, msg, e);

            try {
                if (removedAcksFlag) {
                    removeTransactionAck(id);
                }
            } catch (Exception ex) {
                // Just ignore because error has been logged at lower level
            }

            throw new BrokerException(br.getString(
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, msg), e);
        }
    }

    /**
     * Store a transaction - should only be used for imqdbmgr
     * backup/restore operation.
     */
    void storeTransaction(TransactionUID id, TransactionInfo txnInfo, boolean sync)
	throws BrokerException {

        try {
            tidMap.put(id, txnInfo);
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR,
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, id, e);
            throw new BrokerException(br.getString(
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, id), e);
        }
    }

    /**
     * Remove the transaction. The associated acknowledgements
     * will not be removed.
     *
     * @param id	the id of the transaction to be removed
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the transaction is not found in the store
     */
    void removeTransaction(TransactionUID id, boolean sync)
	throws BrokerException {

        try {
            Object txnInfo = tidMap.remove(id);

            if (txnInfo == null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id);
                throw new BrokerException(
                    br.getString(BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id),
                    Status.NOT_FOUND);
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR, BrokerResources.X_REMOVE_TRANSACTION_FAILED, id, e);
            throw new BrokerException(
                br.getString(BrokerResources.X_REMOVE_TRANSACTION_FAILED, id), e);
        }
    }

    /**
     * Update the state of a transaction
     *
     * @param id	the transaction id to be updated
     * @param ts	the new transaction state
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if an error occurs while persisting or
     * the same transaction id does NOT exists the store already
     * @exception NullPointerException	if <code>id</code> is <code>null</code>
     */
    void updateTransactionState(TransactionUID id, int ts, boolean sync)
	throws IOException, BrokerException {

        try {
            TransactionInfo txnInfo = (TransactionInfo)tidMap.get(id);
            if (txnInfo == null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id);
                throw new BrokerException(
                    br.getString(BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id),
                    Status.NOT_FOUND);
            }

            TransactionState txnState = txnInfo.getTransactionState();
            if (txnState.getState() != ts) {
                txnState.setState(ts);

                tidMap.put(id, txnInfo);
            }
        } catch (Exception e) {
            logger.log(Logger.ERROR, BrokerResources.X_UPDATE_TXNSTATE_FAILED, id, e);
            throw new BrokerException(
                br.getString(BrokerResources.X_UPDATE_TXNSTATE_FAILED, id), e);
        }
    }

    /**
     * Update transaction's participant brokers.
     *
     * @param id        the id of the transaction to be updated
     * @param brokers   the transaction's participant brokers
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the transaction is not found in the store
     */
    public void updateClusterTransaction(TransactionUID id,
        TransactionBroker[] brokers, boolean sync) throws BrokerException {

        try {
            TransactionInfo txnInfo = (TransactionInfo)tidMap.get(id);
            if (txnInfo == null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id);
                throw new BrokerException(br.getString(
                    BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id),
                    Status.NOT_FOUND);
            }

            txnInfo.setType(TransactionInfo.TXN_CLUSTER);
            txnInfo.setTransactionBrokers(brokers);

            tidMap.put(id, txnInfo);
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR, BrokerResources.X_PERSIST_TRANSACTION_FAILED, id, e);
            throw new BrokerException(
                br.getString(BrokerResources.X_PERSIST_TRANSACTION_FAILED, id), e);
        }
    }

    /**
     * Update transaction's participant broker state if the txn's state
     * matches the expected state.
     *
     * @param id the id of the transaction to be updated
     * @param expectedTxnState the expected transaction state
     * @param txnBkr the participant broker to be updated
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the transaction is not found in the store
     * or the txn's state doesn't match the expected state (Status.CONFLICT)
     */
    void updateTransactionBrokerState(TransactionUID id, int expectedTxnState,
        TransactionBroker txnBkr, boolean sync) throws BrokerException {

        try {
            TransactionInfo txnInfo = (TransactionInfo)tidMap.get(id);

            if (txnInfo == null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id);
                throw new BrokerException(br.getString(
                    BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id),
                    Status.NOT_FOUND);
            }

            TransactionState txnState = txnInfo.getTransactionState();
            if (txnState.getState() != expectedTxnState) {
                Object[] args = { txnBkr, id,
                    TransactionState.toString(expectedTxnState),
                    TransactionState.toString(txnState.getState()) };
                throw new BrokerException(br.getKString( BrokerResources.E_UPDATE_TXNBROKER_FAILED,
                    args ), Status.CONFLICT);
            }

            txnInfo.updateBrokerState(txnBkr);

            tidMap.put(id, txnInfo);
        } catch (Exception e) {
            logger.log(Logger.ERROR,
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, id, e);
            throw new BrokerException(br.getString(
                BrokerResources.X_PERSIST_TRANSACTION_FAILED, id), e);
        }
    }

    /**
     * Return transaction state object for the specified transaction.
     * @param id id of the transaction
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the transaction id is not in the store
     */
    TransactionState getTransactionState(TransactionUID id)
        throws BrokerException {

        TransactionInfo txnInfo = (TransactionInfo)tidMap.get(id);

        if (txnInfo == null) {
            logger.log(Logger.ERROR,
                BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id);
            throw new BrokerException(br.getString(
                BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id),
                Status.NOT_FOUND);
        }

        return new TransactionState(txnInfo.getTransactionState());
    }

    /**
     * Return transaction state for the specified transaction.
     * @param id id of the transaction
     */
    int getTransactionStateValue(TransactionUID id)
        throws BrokerException {

        TransactionInfo txnInfo = (TransactionInfo)tidMap.get(id);
        if (txnInfo != null) {
            TransactionState txnState = txnInfo.getTransactionState();
            if (txnState != null) {
                return txnState.getState();
            }
        }

        return TransactionState.NULL;
    }    

    /**
     * Return transaction info object for the specified transaction.
     * @param id id of the transaction
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the transaction id is not in the store
     */
    TransactionInfo getTransactionInfo(TransactionUID id)
        throws BrokerException {

        TransactionInfo txnInfo = (TransactionInfo)tidMap.get(id);

        if (txnInfo == null) {
            logger.log(Logger.ERROR,
                BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id);
            throw new BrokerException(br.getString(
                BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id),
                Status.NOT_FOUND);
        }

        return (TransactionInfo)txnInfo.clone();
    }

    /**
     * Return transaction home broker for the specified transaction.
     * @param id id of the transaction
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the transaction id is not in the store
     */
    BrokerAddress getRemoteTransactionHomeBroker(TransactionUID id)
        throws BrokerException {

        TransactionInfo txnInfo = (TransactionInfo)tidMap.get(id);

        if (txnInfo == null) {
            logger.log(Logger.ERROR,
                BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id);
            throw new BrokerException(br.getString(
                BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id),
                Status.NOT_FOUND);
        }

        return txnInfo.getTransactionHomeBroker();
    }

    /**
     * Return transaction's participant brokers for the specified transaction.
     * @param id id of the transaction whose participant brokers are to be returned
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the transaction id is not in the store
     */
    TransactionBroker[] getClusterTransactionBrokers(TransactionUID id)
	throws BrokerException {

        TransactionInfo txnInfo = (TransactionInfo)tidMap.get(id);

        if (txnInfo == null) {
            logger.log(Logger.ERROR,
                BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id);
            throw new BrokerException(br.getString(
                BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, id),
                Status.NOT_FOUND);
        }

        TransactionBroker[] txnBrokers = txnInfo.getTransactionBrokers();
        if (txnBrokers != null) {
            // Make a copy
            txnBrokers = (TransactionBroker[])txnBrokers.clone();
        }

        return txnBrokers;
    }

    /**
     * Retrieve all local and cluster transaction ids with their state
     * in the store.
     *
     * @return A HashMap. The key is a TransactionUID.
     * The value is a TransactionState.
     * @exception java.io.IOException if an error occurs while getting the data
     */
    HashMap getAllTransactionStates() throws IOException {
	HashMap map = new HashMap(tidMap.size());
        Iterator itr = tidMap.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry entry = (Map.Entry)itr.next();
            TransactionInfo txnInfo = (TransactionInfo)entry.getValue();
            int type = txnInfo.getType();
            if (type == TransactionInfo.TXN_LOCAL ||
                type == TransactionInfo.TXN_CLUSTER) {
                map.put(entry.getKey(), (new TransactionInfo(txnInfo)));
            }
        }

        return map;
    }

    /**
     * Retrieve all remote transaction ids with their state in the store.
     *
     * @return A HashMap. The key is a TransactionUID.
     * The value is a TransactionState.
     * @exception java.io.IOException if an error occurs while getting the data
     */
    HashMap getAllRemoteTransactionStates() throws IOException {
	HashMap map = new HashMap(tidMap.size());
        Iterator itr = tidMap.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry entry = (Map.Entry)itr.next();
            TransactionInfo txnInfo = (TransactionInfo)entry.getValue();
            int type = txnInfo.getType();
            if (type == TransactionInfo.TXN_REMOTE) {
                map.put(entry.getKey(), txnInfo.getTransactionState());
            }
        }

        return map;
    }

    Collection getAllTransactions() {
        return tidMap.keySet();
    }

    /**
     * Clear all transaction ids and the associated ack lists(clear the store);
     * when this method returns, the store has a state that is the same as
     * an empty store (same as when TxnAckList is instantiated with the
     * clear argument set to true.
     */
    void clearAll() {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH, "TxnStore.clearAll() called");
	}

        tidMap.clear();
        ackMap.clear();
    }

    /**
     * Clear all transactions that are NOT in the specified state.
     *
     * @param state State of transactions to spare
     */
    void clear(int state, boolean sync) throws BrokerException {

        boolean error = false;
	Exception exception = null;

        Iterator itr = tidMap.entrySet().iterator();
        while (itr.hasNext()) {
            try {
                Map.Entry entry = (Map.Entry)itr.next();
                TransactionUID tid = (TransactionUID)entry.getKey();
                TransactionState ts = (TransactionState)entry.getValue();
                // Remove if not in prepared state
                if (ts.getState() != state) {
                    itr.remove();
                    removeTransactionAck(tid);
                }
            } catch (RuntimeException e) {
                error = true;
                exception = e;
                logger.log(Logger.ERROR, BrokerResources.X_CLEAR_TXN_NOTIN_STATE_FAILED,
                    new Integer(state), e);
            }
        }

        if (!error) {
            // We may have transactions left in the ackMap that did not
            // have an entry in the tidMap (maybe it was removed by a commit
            // that never had a chance to complete processing the acks).
            // Check for those "orphaned" ack transactions here. We check
            // state here too just to be anal.
            itr = ackMap.keySet().iterator();
            while (itr.hasNext()) {
                TransactionUID tid = (TransactionUID)itr.next();
                TransactionInfo txnInfo = (TransactionInfo)tidMap.get(tid);
                if (txnInfo == null || txnInfo.getTransactionStateValue() != state) {
                    // Orphan. Remove from txnList
                    removeTransactionAck(tid);
                }
            }
        }

        // If we got an error just clear all transactions. 
        if (error) {
            clearAll();
	    throw new BrokerException(
                br.getString(BrokerResources.X_CLEAR_TXN_NOTIN_STATE_FAILED,
                new Integer(state)), exception);
        }
    }

    /**
     * Store an acknowledgement for the specified transaction.
     */
    void storeTransactionAck(TransactionUID tid, TransactionAcknowledgement ack,
	boolean sync) throws BrokerException {

        if (!tidMap.containsKey(tid)) {
            logger.log(Logger.ERROR,
                BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE, tid.toString());
            throw new BrokerException(
                br.getString(BrokerResources.E_TRANSACTIONID_NOT_FOUND_IN_STORE,
                tid.toString()));
        }

        try {
            boolean putIfAbsent = false;
            HashSet acks = (HashSet)ackMap.get(tid);

            if (acks == null) {
                putIfAbsent = true;
                acks = new HashSet();
            } else {
                if (acks.contains(ack)) {
                    logger.log(Logger.ERROR,
                        BrokerResources.E_ACK_EXISTS_IN_STORE, ack, tid);
                    throw new BrokerException(
                        br.getString(BrokerResources.E_ACK_EXISTS_IN_STORE, ack, tid));
                }
            }
            acks.add(ack);

            Object oldValue = ackMap.putIfAbsent(tid, acks);
            if (putIfAbsent && (oldValue != null)) {
                // If we're unable to update the map, then another ack
                // has been added before we we get a chance; so try again!
                acks = (HashSet)ackMap.get(tid);
                acks.add(ack);
                ackMap.put(tid, acks);
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR, BrokerResources.X_PERSIST_TXNACK_FAILED,
                ack.toString(), tid.toString());
            throw new BrokerException(
                br.getString(BrokerResources.X_PERSIST_TXNACK_FAILED,
                ack.toString(), tid.toString()), e);
	}
    }

    /**
     * Store the acknowledgements for the specified transaction.
     *
     * @param tid	the transaction id with which the acknowledgments are
     *			to be stored
     * @param txnAcks	the acknowledgements to be stored
     * @exception BrokerException if the transaction id is not found in the
     *			store, or if it failed to persist the data
     */
    void storeTransactionAcks(TransactionUID tid,
        TransactionAcknowledgement[] txnAcks) throws BrokerException {

        List ackList = Arrays.asList(txnAcks); // Convert array to a List

        try {
            boolean putIfAbsent = false;
            HashSet acks = (HashSet)ackMap.get(tid);

            if (acks == null) {
                putIfAbsent = true;
                acks = new HashSet(ackList.size());
            }
            acks.addAll(ackList);

            Object oldValue = ackMap.putIfAbsent(tid, acks);
            if (putIfAbsent && (oldValue != null)) {
                // If we're unable to update the map, then another ack
                // has been added before we we get a chance; so try again!
                acks = (HashSet)ackMap.get(tid);
                acks.addAll(ackList);
                ackMap.put(tid, acks);
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR, BrokerResources.X_PERSIST_TXNACK_FAILED,
                ackList.toString(), tid.toString());
            throw new BrokerException(
                br.getString(BrokerResources.X_PERSIST_TXNACK_FAILED,
                ackList.toString(), tid.toString()), e);
        }
    }

    /**
     * Remove all acknowledgements associated with the specified
     * transaction from the persistent store.
     */
    void removeTransactionAck(TransactionUID tid) throws BrokerException {

        try {
            HashSet acks = (HashSet)ackMap.remove(tid);
            if (acks != null) {
                acks.clear();
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR,
                BrokerResources.X_REMOVE_TXNACK_FAILED, tid.toString());
            throw new BrokerException(
                br.getString(BrokerResources.X_REMOVE_TXNACK_FAILED, tid.toString()), e);
        }
    }

    /**
     * Retrieve all acknowledgements for the specified transaction.
     */
    TransactionAcknowledgement[] getTransactionAcks(TransactionUID tid)
	throws BrokerException {

        HashSet acks = (HashSet)ackMap.get(tid);
        if (acks != null) {
            return (TransactionAcknowledgement[])acks.toArray(emptyAckArray);
        } else {
            return emptyAckArray;
        }
    }

    /**
     * Retrieve all acknowledgement list in the persistence store
     * together with their associated transaction id.
     */
    HashMap getAllTransactionAcks() {
        if (ackMap.size() == 0) {
            return emptyHashMap;
        }

        HashMap allacks = new HashMap(ackMap.size());

        Set entries = ackMap.entrySet();
        Iterator itr = entries.iterator();
        while (itr.hasNext()) {
            Map.Entry entry = (Map.Entry)itr.next();

            // get acks into an array of TransactionAcknowledgement
            HashSet set = (HashSet)entry.getValue();
            TransactionAcknowledgement[] acks = (TransactionAcknowledgement[])
                set.toArray(emptyAckArray);

            allacks.put(entry.getKey(), acks);
        }

        return allacks;
    }

    /**
     * @return the total number of transaction acknowledgements in the store
     */
    public int getNumberOfTxnAcks() {
	int size = 0;
	Iterator itr = ackMap.entrySet().iterator();
	while (itr.hasNext()) {
	    Map.Entry entry = (Map.Entry)itr.next();
	    HashSet acks = (HashSet)entry.getValue();
	    size += acks.size();
	}
	return size;
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */  
    Hashtable getDebugState() {
	Hashtable t = new Hashtable();
	t.put("Transactions", String.valueOf(tidMap.size()));
        t.put("Txn acks", String.valueOf(ackMap.size()));
	return t;
    }

    void printInfo(PrintStream out) {
	// transacation ids
	out.println("\nTransaction IDs");
	out.println("---------------");
	out.println("number of transaction ids: " + tidMap.size());
        out.println("\nTransaction acknowledgements");
        out.println("----------------------------");
        out.println("Number of transactions containing acknowledgements: " + ackMap.size());
    }
}


