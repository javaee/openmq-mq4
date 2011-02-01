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
 * @(#)Store.java	1.123 06/29/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.sun.messaging.bridge.service.DupKeyException;
import com.sun.messaging.bridge.service.JMSBridgeStore;
import com.sun.messaging.bridge.service.KeyNotFoundException;
import com.sun.messaging.bridge.service.UpdateOpaqueDataCallback;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.util.UID;
import com.sun.messaging.jmq.jmsserver.Broker;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.cluster.BrokerState;
import com.sun.messaging.jmq.jmsserver.config.BrokerConfig;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;
import com.sun.messaging.jmq.jmsserver.core.Consumer;
import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;
import com.sun.messaging.jmq.jmsserver.data.BaseTransaction;
import com.sun.messaging.jmq.jmsserver.data.ClusterTransaction;
import com.sun.messaging.jmq.jmsserver.data.TransactionAcknowledgement;
import com.sun.messaging.jmq.jmsserver.data.TransactionBroker;
import com.sun.messaging.jmq.jmsserver.data.TransactionList;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;
import com.sun.messaging.jmq.jmsserver.service.TakingoverTracker;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsservice.BrokerEvent;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.util.txnlog.TransactionLogWriter;
import com.sun.messaging.jmq.jmsserver.persist.sharecc.ShareConfigChangeStore;

/**
 * Store provides API for storing and retrieving
 * various kinds of data used by the broker: message,
 * per message interest list, interest objects and destination objects.
 * Classes implementing this interface provide the actual mechanism
 * to persist the data.
 */
public abstract class Store implements JMSBridgeStore {

    public static final String STORE_PROP_PREFIX
                        = Globals.IMQ + ".persist.store";
    public static final String CREATE_STORE_PROP
                        = STORE_PROP_PREFIX + "create.all";
    public static final String REMOVE_STORE_PROP
                        = STORE_PROP_PREFIX + "remove.all";
    public static final String RESET_STORE_PROP
			= STORE_PROP_PREFIX + "reset.all";
    public static final String RESET_MESSAGE_PROP
			= STORE_PROP_PREFIX + "reset.messages";
    public static final String RESET_INTEREST_PROP
			= STORE_PROP_PREFIX + "reset.durables";
    public static final String UPGRADE_NOBACKUP_PROP
			= STORE_PROP_PREFIX + "upgrade.nobackup";

    public static final boolean CREATE_STORE_PROP_DEFAULT = false;

    public static final String FILE_STORE_TYPE = "file";
    public static final String JDBC_STORE_TYPE = "jdbc";
    public static final String INMEMORY_STORE_TYPE = "inmemory";

    // control printing debug output by property file
    private static boolean DEBUG = false;
    private static boolean DEBUG_SYNC = 
        Globals.getConfig().getBooleanProperty(
            Globals.IMQ + ".persist.store.debug.sync") || DEBUG;

    /** Message has been routed to this interest */
    public static final int INTEREST_STATE_ROUTED = 0;

    /** Message has been delivered to this interest */
    public static final int INTEREST_STATE_DELIVERED = 1;

    /** Interest has acknowledged the message */
    public static final int INTEREST_STATE_ACKNOWLEDGED = 2;

    public static final Logger logger = Globals.getLogger();
    public static final BrokerResources br = Globals.getBrokerResources();
    public static final BrokerConfig config = Globals.getConfig();

    /**
     * Variables to make sure no Store method is called after the
     * store is closed and that the close operation won't start
     * until all store operations are done.
     */
    // boolean flag indicating whether the store is closed or not
    private boolean closed = false;
    private Object closedLock = new Object();	// lock for closed

    // number indicating the number of store operations in progress
    private int inprogressCount = 0;
    private Object inprogressLock = new Object(); // lock for inprogressCount

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    public Lock txnLogSharedLock = lock.readLock();
    public Lock txnLogExclusiveLock = lock.writeLock();
    

    
    protected boolean createStore = false;
    protected boolean resetStore = false;
    protected boolean resetMessage = false;
    protected boolean resetInterest = false;
    protected boolean removeStore = false;
    protected boolean upgradeNoBackup = false;

    /**
     * Default Constructor.
     */
    protected Store() {
        createStore = config.getBooleanProperty(CREATE_STORE_PROP, 
                                                CREATE_STORE_PROP_DEFAULT);
        removeStore = config.getBooleanProperty(REMOVE_STORE_PROP, false);
        resetStore = config.getBooleanProperty(RESET_STORE_PROP, false);
        resetMessage = config.getBooleanProperty(RESET_MESSAGE_PROP, false);
        resetInterest = config.getBooleanProperty(RESET_INTEREST_PROP, false);
        upgradeNoBackup = config.getBooleanProperty(UPGRADE_NOBACKUP_PROP, false);

        if (removeStore) {
	        logger.logToAll(Logger.INFO, BrokerResources.I_REMOVE_PERSISTENT_STORE);
        } else {
            if (resetStore) {
                logger.logToAll(Logger.INFO, BrokerResources.I_RESET_PERSISTENT_STORE);
            } else {
                if (resetMessage) {
                    logger.logToAll(Logger.INFO, BrokerResources.I_RESET_MESSAGE);
                }
                if (resetInterest) {
                    logger.logToAll(Logger.INFO, BrokerResources.I_RESET_INTEREST);
                }
	        }
            if (!resetStore && (resetMessage||resetInterest)) {
                logger.logToAll(Logger.INFO, BrokerResources.I_LOAD_REMAINING_STORE_DATA);
            } else if (!resetStore) {
                logger.logToAll(Logger.INFO, BrokerResources.I_LOAD_PERSISTENT_STORE);
            }
        }
    }

    public static boolean getDEBUG() {
        return DEBUG;
    }
    
    public static boolean getDEBUG_SYNC() {
        return DEBUG_SYNC;
    }


    /**
     * Get the store version.
     * @return store version
     */
    public abstract int getStoreVersion();

    

    public void init() throws BrokerException
    {
    	 // no op unless file store 
    }

    public ShareConfigChangeStore getShareConfigChangeStore()
        throws BrokerException {

        if ( !Globals.useSharedConfigRecord() ) {
            throw new UnsupportedOperationException(
            br.getKString(br.E_INTERNAL_ERROR)+" Unexpected call");
        }
        return StoreManager.getShareConfigChangeStore();
    }

    
    public  List<BaseTransaction> getIncompleteTransactions(int type)
    {
    	return null;
    }
    
    public void rollbackAllTransactions()
    {
    	
    }
    
    /**
     * Store a message, which is uniquely identified by it's SysMessageID,
     * and it's list of interests and their states.
     *
     * @param dID	the destination the message is associated with
     * @param message	the message to be persisted
     * @param iIDs	an array of interest ids whose states are to be
     *			stored with the message
     * @param states	an array of states
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting the data
     * @exception BrokerException if a message with the same id exists
     *			in the store already
     * @exception NullPointerException	if <code>message</code>,
     *			<code>iIDs</code>, or <code>states</code> is
     *			<code>null</code>
     */
    public abstract void storeMessage(DestinationUID dID,
	Packet message, ConsumerUID[] iIDs,
	int[] states, boolean sync) throws IOException, BrokerException;

    /**
     * Store a message which is uniquely identified by it's SysMessageID.
     *
     * @param dID	the destination the message is associated with
     * @param message	the readonly packet to be persisted
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting the message
     * @exception BrokerException if a message with the same id exists
     *			in the store already
     * @exception NullPointerException	if <code>message</code> is
     *			<code>null</code>
     */
    public abstract void storeMessage(DestinationUID dID,
	Packet message, boolean sync) throws IOException, BrokerException;

    /**
     * Remove the message from the persistent store.
     * If the message has an interest list, the interest list will be
     * removed as well.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the message
     * @exception BrokerException if the message is not found in the store
     * @exception NullPointerException	if <code>dID</code> is
     *			<code>null</code>
     */
    public void removeMessage(DestinationUID dID,
	SysMessageID mID, boolean sync) throws IOException, BrokerException
	{    	 
    	  removeMessage(dID,mID,sync, false);    	    
	}
    

    /**
     * Remove the message from the persistent store.
     * If the message has an interest list, the interest list will be
     * removed as well.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message to be removed
     * @param sync	if true, will synchronize data to disk
     * @param onRollback if true, removal is being requested as part of a transaction rollback
     * @exception IOException if an error occurs while removing the message
     * @exception BrokerException if the message is not found in the store
     * @exception NullPointerException	if <code>dID</code> is
     *			<code>null</code>
     */
    public abstract void removeMessage(DestinationUID dID,
    		SysMessageID mID, boolean sync, boolean onRollback) throws IOException, BrokerException;
   
    /**
     * Move the message from one destination to another.
     * The message will be stored in the target destination with the
     * passed in consumers and their corresponding states.
     * After the message is persisted successfully, the message in the
     * original destination will be removed.
     *
     * @param message	the message to be moved
     * @param fromDID	the destination the message is currently in
     * @param toDID	the destination to move the message to
     * @param iIDs	an array of interest ids whose states are to be
     *			stored with the message
     * @param states	an array of states
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while moving the message
     * @exception BrokerException if the message is not found in source
     *		destination
     * @exception NullPointerException	if <code>message</code>, 
     *			<code>fromDID</code>, <code>toDID</code>,
     *			<code>iIDs</code>, or <code>states</code> is
     *			<code>null</code>
     */
    public abstract void moveMessage(Packet message, DestinationUID fromDID,
	DestinationUID toDID, ConsumerUID[] iIDs, int[] states, boolean sync)
	throws IOException, BrokerException;

    /**
     * Remove all messages associated with the specified destination
     * from the persistent store.
     *
     * @param destination   the destination whose messages are to be removed
     * @param sync          if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the messages
     * @exception BrokerException if the destination is not found in the store
     * @exception NullPointerException	if <code>destination</code> is
     *			<code>null</code>
     */
    public abstract void removeAllMessages(Destination destination,
	boolean sync) throws IOException, BrokerException;

    /**
     * Return an enumeration of all persisted messages for the given
     * destination.
     * Use the Enumeration methods on the returned object to fetch
     * and load each message sequentially.
     *
     * <p>
     * This method is to be used at broker startup to load persisted
     * messages on demand.
     *
     * @param destination   the destination whose messages are to be returned
     * @return an enumeration of all persisted messages, an empty
     *		enumeration will be returned if no messages exist for the
     *		destionation
     * @exception BrokerException if an error occurs while getting the data
     */
    public abstract Enumeration messageEnumeration(Destination destination)
	throws BrokerException;

    /**
     * To close an enumeration retrieved from the store
     */ 
    public void closeEnumeration(Enumeration en) {
    }

    /**
     * Check if a a message has been acknowledged by all interests (HA support).
     *
     * @param mID   the system message id of the message to be checked
     * @return true if all interests have acknowledged the message
     * @throws BrokerException
     */
    public boolean hasMessageBeenAcked(SysMessageID mID)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Check if a a message has been acknowledged by all interests.
     * @param dst  the destination the message is associated with
     * @param id   the system message id of the message to be checked
     * @return true if all interests have acknowledged the message;
     * false if message has not been routed or acknowledge by all interests
     * @throws BrokerException
     */
    public abstract boolean hasMessageBeenAcked(DestinationUID dst,
        SysMessageID id) throws BrokerException;


    /**
     * Return the number of persisted messages for the given broker (HA support).
     *
     * @param brokerID the broker ID
     * @return the number of persisted messages for the given broker
     * @exception BrokerException if an error occurs while getting the data
     */
    public int getMessageCount(String brokerID) throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Return the number of persisted messages and total number of bytes for
     * the given destination. The constant DestMetricsCounters.CURRENT_MESSAGES
     * and DestMetricsCounters.CURRENT_MESSAGE_BYTES will be used as keys for
     * the HashMap.
     *
     * @param destination the destination whose messages are to be counted
     * @return A HashMap of name value pair of information
     * @throws BrokerException if an error occurs while getting the data
     */
    public abstract HashMap getMessageStorageInfo(Destination destination)
        throws BrokerException;

    /**
     * Return the message with the specified system message id.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the message id of the message to be retrieved
     * @return a message
     * @exception BrokerException if the message is not found in the store
     *			or if an error occurs while getting the data
     */
    public abstract Packet getMessage(DestinationUID dID, String mID)
	throws BrokerException;

    /**
     * Return the message with the specified system message id.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message to be retrieved
     * @return a message
     * @exception BrokerException if the message is not found in the store
     *			or if an error occurs while getting the data
     */
    public abstract Packet getMessage(DestinationUID dID, SysMessageID mID)
	throws BrokerException;

    /**
     * Store the given list of interests and their states with the
     * specified message.  The message should not have an interest
     * list associated with it yet.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message that the interest
     *			is associated with
     * @param iIDs	an array of interest ids whose states are to be stored
     * @param states	an array of states
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if the message is not in the store;
     *				if there's an interest list associated with
     *				the message already; or if an error occurs
     *				while persisting the data
     */
    public abstract void storeInterestStates(DestinationUID dID,
	SysMessageID mID, ConsumerUID[] iIDs, int[] states, boolean sync, Packet msg)
	throws BrokerException;
    
    public  void storeInterestStates(DestinationUID dID,
    		SysMessageID mID, ConsumerUID[] iIDs, int[] states, boolean sync)
    		throws BrokerException
    {
    	storeInterestStates(dID,mID,iIDs,states,sync,null);
    }

    /**
     * Update the state of the interest associated with the specified
     * message.  The interest should already be in the interest list
     * of the message.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message that the interest
     *			is associated with
     * @param iID	the interest id whose state is to be updated
     * @param state	state of the interest
     * @param sync	if true, will synchronize data to disk
     * @param txid	txId if in a transaction, otherwise null
     * @param isLastAck	Is this the last ack for this message. 
     * @exception BrokerException if the message is not in the store; if the
     *			interest is not associated with the message; or if
     *			an error occurs while persisting the data
     */
    public abstract void updateInterestState(DestinationUID dID,
	SysMessageID mID, ConsumerUID iID, int state, boolean sync, TransactionUID txid, boolean islastAck)
	throws BrokerException;

    
    /**
     * @deprecated
     * keep to support tests for old API
     * Now use method with transaction parameter
     */
    public  void updateInterestState(DestinationUID dID,
    		SysMessageID mID, ConsumerUID iID, int state,  boolean sync)
    		throws BrokerException
    {
    	updateInterestState(dID,mID,iID,state,sync,null,false);
    }
    
    /**
     * Update the state of the interest associated with the specified
     * message (HA support).  The interest should already be in the interest
     * list of the message.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message that the interest
     *			is associated with
     * @param iID	the interest id whose state is to be updated
     * @param newState	state of the interest
     * @param expectedState the expected state
     * @exception BrokerException if the message is not in the store; if the
     *			interest is not associated with the message; if the
     *			current state doesn't match the expected state; or if
     *			an error occurs while persisting the data
     */
    public void updateInterestState(DestinationUID dID, SysMessageID mID,
        ConsumerUID iID, int newState, int expectedState) throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Get the state of the interest associated with the specified message.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message that the interest
     *			is associated with
     * @param iID	the interest id whose state is to be returned
     * @return state of the interest
     * @exception BrokerException if the specified interest is not
     *		associated with the message; or if the message is not in the
     *		store
     */
    public abstract int getInterestState(DestinationUID dID,
	SysMessageID mID, ConsumerUID iID) throws BrokerException;

    /**
     * Retrieve all interests and states associated with the specified message.
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message that the interest
     * @return HashMap of containing all consumer's state
     * @throws BrokerException
     */
    public abstract HashMap getInterestStates(DestinationUID dID,
        SysMessageID mID) throws BrokerException;

    /**
     * Retrieve all interest IDs associated with the message
     * <code>mID</code> in destination <code>dID</code>.
     * Note that the state of the interests returned is either
     * INTEREST_STATE_ROUTED or INTEREST_STATE_DELIVERED, and interest
     * whose state is INTEREST_STATE_ACKNOWLEDGED will not be returned in
     * the array.
     *
     * @param dID   the destination the message is associated with
     * @param mID   the system message id of the message whose interests
     *			are to be returned
     * @return an array of ConsumerUID objects associated with the message; a
     *		zero length array will be returned if no interest is
     *		associated with the message
     * @exception BrokerException if the message is not in the store or if
     *				an error occurs while getting the data
     */
    public abstract ConsumerUID[] getConsumerUIDs(
	DestinationUID dID, SysMessageID mID) throws BrokerException;

    /**
     * Store an Consumer which is uniquely identified by it's id.
     *
     * @param interest  the interest to be persisted
     * @param sync      if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting the interest
     * @exception BrokerException if an interest with the same id exists in
     *			the store already
     * @exception NullPointerException	if <code>interest</code> is
     *			<code>null</code>
     */
    public abstract void storeInterest(Consumer interest, boolean sync)
	throws IOException, BrokerException;

    /**
     * Remove the interest from the persistent store.
     *
     * @param interest	the interest to be removed from persistent store
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the interest
     * @exception BrokerException if the interest is not found in the store
     */
    public abstract void removeInterest(Consumer interest, boolean sync)
	throws IOException, BrokerException;

    /**
     * Retrieve all interests in the store.
     *
     * @return an array of Interest objects; a zero length array is
     * returned if no interests exist in the store
     * @exception IOException if an error occurs while getting the data
     */
    public abstract Consumer[] getAllInterests()
	throws IOException, BrokerException;

    /**
     * Store a Destination.
     *
     * @param destination   the destination to be persisted
     * @param sync          if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting the destination
     * @exception BrokerException if the same destination exists
     * in the store already
     * @exception NullPointerException	if <code>destination</code> is
     *			<code>null</code>
     */
    public abstract void storeDestination(Destination destination, boolean sync)
	throws IOException, BrokerException;

    /**
     * Update the specified destination.
     *
     * @param destination   the destination to be updated
     * @param sync	    if true, will synchronize data to disk
     * @exception BrokerException if the destination is not found in the store
     *				or if an error occurs while updating the
     *				destination
     */
    public abstract void updateDestination(Destination destination, boolean sync)
        throws BrokerException;

    /**
     * Update the connected timestamp for a temporary destination (HA support).
     *
     * @param destination   the temporary destination to be updated
     * @param timestamp     the time when a consumer re-attached to the destination
     * @exception BrokerException if the destination is not found in the store
     *            or if an error occurs while updating the destination
     */
    public void updateDestinationConnectedTime(Destination destination,
        long timestamp) throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );

    }

    /**
     * Remove the destination from the persistent store.
     * All messages associated with the destination will be removed as well.
     *
     * @param destination   the destination to be removed
     * @param sync          if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the destination
     * @exception BrokerException if the destination is not found in the store
     */
    public abstract void removeDestination(Destination destination,
	boolean sync) throws IOException, BrokerException;

    /**
     * Reap auto-created destinations (HA support).
     *
     * @exception BrokerException
     */
    public void reapAutoCreatedDestinations() throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Retrieve the timestamp when a consumer (owner of the connection that
     * creates this temporary destination) connected/re-attached to a
     * temporary destination or when it was created (HA support).
     *
     * @param destination   the temporary destination
     * @return the timestamp
     * @exception BrokerException if the destination is not found in the store
     *            or if an error occurs while updating the destination
     */
    public long getDestinationConnectedTime(Destination destination)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Retrieve a destination in the store.
     *
     * @param dID the destination ID
     * @return a Destination object
     * @throws BrokerException if no destination exist in the store
     */
    public abstract Destination getDestination(DestinationUID dID) 
        throws IOException, BrokerException;

    /**
     * Retrieve all destinations in the store. In HA mode, retrieve all global
     * destinations and local destinations for the calling broker.
     *
     * @return an array of Destination objects; a zero length array is
     * returned if no destinations exist in the store
     * @exception IOException if an error occurs while getting the data
     */
    public abstract Destination[] getAllDestinations()
	throws IOException, BrokerException;

    /**
     * Store a transaction.
     *
     * @param txnID	id of the transaction to be persisted
     * @param txnState	the transaction state to be persisted
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting
     *		the transaction
     * @exception BrokerException if the same transaction id exists
     *			the store already
     * @exception NullPointerException	if <code>txnID</code> is
     *			<code>null</code>
     */
    public abstract void storeTransaction(TransactionUID txnID,
        TransactionState txnState, boolean sync)
        throws IOException, BrokerException;

    /**
     * Store a transaction - should only be used for imqdbmgr
     * backup/restore operation.
     *
     * @param txnID	id of the transaction to be persisted
     * @param txnInfo	the transaction info to be persisted
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting
     *		the transaction
     * @exception BrokerException if the same transaction id exists
     *			the store already
     * @exception NullPointerException	if <code>txnID</code> is
     *			<code>null</code>
     */
    public abstract void storeTransaction(TransactionUID txnID,
        TransactionInfo txnInfo, boolean sync)
        throws IOException, BrokerException;

    /**
     * Store a cluster transaction.
     *
     * @param txnID	the id of the transaction to be persisted
     * @param txnState	the transaction's state to be persisted
     * @param txnBrokers the transaction's participant brokers
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if the same transaction id exists
     *			the store already
     * @exception NullPointerException	if <code>txnID</code> is
     *			<code>null</code>
     */
    public abstract void storeClusterTransaction(TransactionUID txnID,
        TransactionState txnState, TransactionBroker[] txnBrokers, boolean sync)
        throws BrokerException;

    /**
     * Store a remote transaction.
     *
     * @param id	the id of the transaction to be persisted
     * @param txnState	the transaction's state to be persisted
     * @param txnAcks	the transaction's participant brokers
     * @param txnHomeBroker the transaction's home broker
    */
    public abstract void storeRemoteTransaction(TransactionUID id,
        TransactionState txnState, TransactionAcknowledgement[] txnAcks,
        BrokerAddress txnHomeBroker, boolean sync) throws BrokerException;

    /**
     * Remove the transaction. The associated acknowledgements
     * will not be removed.
     *
     * @param txnID	the id of transaction to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the transaction
     * @exception BrokerException if the transaction is not found
     *			in the store
     */
    public abstract void removeTransaction(TransactionUID txnID, boolean sync)
	throws IOException, BrokerException;

    /**
     * Remove the transaction. The associated acknowledgements
     * will be removed if removeAcks is true.
     *
     * @param txnID	the id of transaction to be removed
     * @param removeAcks if true, will remove all associated acknowledgements
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the transaction
     * @exception BrokerException if the transaction is not found
     *			in the store
     */
    public abstract void removeTransaction(TransactionUID txnID,
        boolean removeAcks, boolean sync) throws IOException, BrokerException;

    /**
     * Update the state of a transaction
     *
     * @param txnID	the transaction id to be updated
     * @param state	the new transaction state
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting
     *		the transaction id
     * @exception BrokerException if the transaction id does NOT exists in
     *			the store already
     * @exception NullPointerException	if <code>txnID</code> is
     *			<code>null</code>
     */
    public abstract void updateTransactionState(TransactionUID txnID,
        TransactionState state, boolean sync) throws IOException, BrokerException;

    /**
     * Update transaction's participant brokers for the specified cluster
     * transaction.
     *
     * @param txnUID       the id of the transaction to be updated
     * @param txnBrokers   the transaction's participant brokers
     * @exception BrokerException if the transaction is not found in the store
     */
    public abstract void updateClusterTransaction(TransactionUID txnUID,
        TransactionBroker[] txnBrokers, boolean sync) throws BrokerException;

    /**
     * Update transaction's participant brokers for the specified cluster
     * transaction.
     * This method is only used for fast transaction logging
     *
     * @param txnUID       the id of the transaction to be updated
     * @param txnBrokers   the transaction's participant brokers
     * @param clusterTransaction   the cluster transaction
     * @exception BrokerException if the transaction is not found in the store
     */
    public void updateClusterTransaction(TransactionUID txnUID,
        TransactionBroker[] txnBrokers, boolean sync, ClusterTransaction clusterTransaction) throws BrokerException{
    throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }
    
    /**
     * Update transaction's participant broker state for the specified cluster
     * transaction if the txn's state matches the expected state.
     *
     * @param txnUID the id of the transaction to be updated
     * @param expectedTxnState the expected transaction state
     * @param txnBroker the participant broker to be updated
     * @exception BrokerException if the transaction is not found in the store
     * or the txn's state doesn't match the expected state (Status.CONFLICT)
     */
    public abstract void updateClusterTransactionBrokerState(
        TransactionUID txnUID, int expectedTxnState, TransactionBroker txnBroker,
        boolean sync) throws BrokerException;

    /**
     * Update the transaction home broker for the specified remote transaction
     * (HA support).
     *
     * In HA mode, the txn is owned by another broker so we'll only update
     * the txn home broker.

     * @param txnUID the transaction ID
     * @param txnHomeBroker the home broker for a REMOTE txn
     * @throws BrokerException if transaction does not exists in the store
     */
    public void updateRemoteTransaction(TransactionUID txnUID, 
        TransactionAcknowledgement[] txnAcks,
        BrokerAddress txnHomeBroker, boolean sync) throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Retrieve the state of a transaction.
     *
     * @param txnID	the transaction id to be retrieved
     * @return the TransactionState
     * @exception BrokerException if the transaction id does NOT exists in
     *			the store already
     */
    public abstract TransactionState getTransactionState(TransactionUID txnID)
        throws BrokerException;

    /**
     * Update the transaction accessed time (HA support).
     *
     * @param txnID	the transaction id to be updated
     * @param timestamp the time the transaction was accessed
     * @exception BrokerException if the transaction id does NOT exists in
     *			the store already
     */
    public void updateTransactionAccessedTime(TransactionUID txnID,
        long timestamp) throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Retrieve the transaction accessed time (HA support).
     *
     * @param txnID	the transaction id to be retrieved
     * @return the last time the transaction was accessed
     * @exception BrokerException if the transaction id does NOT exists in
     *			the store already
     */
    public long getTransactionAccessedTime(TransactionUID txnID)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Retrieve all transaction ids owned by a broker (HA support)
     * 
     * @return a collection of TransactionUIDs; an empty list will be returned
     *		if the broker does not own any transaction.
     * @exception BrokerException if an error occurs while getting the data
     */
    public Collection getTransactions(String brokerID)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Return the number of messages and the number of consumer states that
     * that associate with the specified transaction ID (HA support).
     *
     * @param txnID the transaction ID
     * @return an array of int whose first element contains the number of messages
     * and the second element contains the number of consumer states.
     * @exception BrokerException if an error occurs while getting the data
     */
    public int[] getTransactionUsageInfo(TransactionUID txnID)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Return transaction's participant brokers for the specified transaction.
     *
     * @param txnID id of the transaction whose participant brokers are to be returned
     * @exception BrokerException if the transaction id is not in the store
     */
    public abstract TransactionBroker[] getClusterTransactionBrokers(
        TransactionUID txnID) throws BrokerException;

    /**
     * Return transaction home broker for the specified remote transaction.
     *
     * @param txnID the transaction ID
     * @exception BrokerException if the transaction id is not in the store
     */
    public abstract BrokerAddress getRemoteTransactionHomeBroker(
        TransactionUID txnID) throws BrokerException;

    /**
     * Return transaction info object for the specified transaction.
     *
     * @param txnID the transaction ID
     * @exception BrokerException if the transaction id is not in the store
     */
    public abstract TransactionInfo getTransactionInfo(TransactionUID txnID)
        throws BrokerException;

    /**
     * Retrieve all local and cluster transaction ids in the store
     * with their state
     *
     * @return A HashMap. The key of is a TransactionUID.
     * The value of each entry is a TransactionState.
     * @exception BrokerException if an error occurs while getting the data
     */
    public abstract HashMap getAllTransactionStates()
        throws IOException, BrokerException;

    /**
     * Retrieve all remote transaction ids in the store with their state;
     * transactions this broker participates in but doesn't owns.
     *
     * @return A HashMap. The key of is a TransactionUID.
     * The value of each entry is a TransactionState.
     * @exception BrokerException if an error occurs while getting the data
     */
    public abstract HashMap getAllRemoteTransactionStates()
        throws IOException, BrokerException;

    /**
     * Close the store and releases any system resources associated with
     * it. The store will be cleaned up. All data files trimed to the
     * length of valid data.
     */
    public void close() {
	close(true);
    }

    /**
     * Close the store and releases any system resources associated with
     * it.
     * @param cleanup if this is false, the store will not be cleaned up
     *			when it is closed.  The default behavior is that
     *			the store is cleaned up.
     */
    public abstract void close(boolean cleanup);

    /**
     * Store the acknowledgement for the specified transaction.
     *
     * @param txnID	the transaction id with which the acknowledgment is to
     *			be stored
     * @param txnAck	the acknowledgement to be stored
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if the transaction id is not found in the
     *				store, if the acknowledgement already
     *				exists, or if it failed to persist the data
     */
    public abstract void storeTransactionAck(TransactionUID txnID,
	TransactionAcknowledgement txnAck, boolean sync) throws BrokerException;

    /**
     * Remove all acknowledgements associated with the specified
     * transaction from the persistent store.
     *
     * @param txnID	the transaction id whose acknowledgements are
     *			to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if error occurs while removing the
     *			acknowledgements
     */
    public abstract void removeTransactionAck(TransactionUID txnID, boolean sync)
	throws BrokerException;

    /**
     * Retrieve all acknowledgements for the specified transaction.
     *
     * @param txnID	id of the transaction whose acknowledgements
     *			are to be returned
     * @exception BrokerException if the transaction id is not in the store
     */
    public abstract TransactionAcknowledgement[] getTransactionAcks(
	TransactionUID txnID) throws BrokerException;

    /**
     * Retrieve all acknowledgement list in the persistence store together
     * with their associated transaction id. The data is returned in the
     * form a HashMap. Each entry in the HashMap has the transaction id as
     * the key and an array of the associated TransactionAcknowledgement
     * objects as the value.
     * @return a HashMap object containing all acknowledgement lists in the
     *		persistence store
     */
    public abstract HashMap getAllTransactionAcks() throws BrokerException;

    /**
     * Persist the specified property name/value pair.
     * If the property identified by name exists in the store already,
     * it's value will be updated with the new value.
     * If value is null, the property will be removed.
     * The value object needs to be serializable.
     *
     * @param name  the name of the property
     * @param value the value of the property
     * @param sync  if true, will synchronize data to disk
     * @exception BrokerException if an error occurs while persisting the
     *			data
     * @exception NullPointerException if <code>name</code> is
     *			<code>null</code>
     */
    public abstract void updateProperty(String name, Object value, boolean sync)
	throws BrokerException;

    /**
     * Retrieve the value for the specified property.
     *
     * @param name name of the property whose value is to be retrieved
     * @return the property value; null is returned if the specified
     *		property does not exist in the store
     * @exception BrokerException if an error occurs while retrieving the
     *			data
     * @exception NullPointerException if <code>name</code> is
     *			<code>null</code>
     */
    public abstract Object getProperty(String name) throws BrokerException;

    /**
     * Return the names of all persisted properties.
     *
     * @return an array of property names; an empty array will be returned
     *		if no property exists in the store.
     */
    public abstract String[] getPropertyNames() throws BrokerException;

    /**
     * Return all persisted properties.
     *
     * @return a properties object.
     */
    public abstract Properties getAllProperties() throws BrokerException;

    /**
     * Append a new record to the config change record store.
     * The timestamp is also persisted with the recordData.
     * The config change record store is an ordered list (sorted
     * by timestamp).
     *
     * @param timestamp     The time when this record was created.
     * @param recordData    The record data.
     * @param sync	    if true, will synchronize data to disk
     * @exception BrokerException if an error occurs while persisting
     *			the data or if the timestamp is less than 0
     * @exception NullPointerException if <code>recordData</code> is
     *			<code>null</code>
     */
    public abstract void storeConfigChangeRecord(
	long timestamp, byte[] recordData, boolean sync) throws BrokerException;

    /**
     * Get all the config change records since the given timestamp.
     * Retrieves all the entries with recorded timestamp greater than
     * the specified timestamp.
     * 
     * @return a list of ChangeRecordInfo, empty list if no records 
     * @exception BrokerException if an error occurs while getting
     * the data.
     */
    public abstract ArrayList<ChangeRecordInfo> getConfigChangeRecordsSince(long timestamp)
	throws BrokerException;

    /**
     * Return all config records with their corresponding timestamps.
     *
     * @return a list of ChangeRecordInfo
     * @exception BrokerException if an error occurs while getting the data
     */
    public abstract List<ChangeRecordInfo> getAllConfigRecords() throws BrokerException;

    /**
     * Clear all config change records in the store.
     *
     * @param sync  if true, will synchronize data to disk
     * @exception BrokerException if an error occurs while clearing the data
     */
    public abstract void clearAllConfigChangeRecords(boolean sync)
	throws BrokerException;

    /**
     * Clear the store. Remove all persistent data.
     */
    public abstract void clearAll(boolean sync) throws BrokerException;

    /**
     * Compact the message file associated with the specified destination.
     * If null is specified, message files assocated with all persisted
     * destinations will be compacted..
     */
    public abstract void compactDestination(Destination destination)
	throws BrokerException;

    /**
     * Get information about the underlying storage for the specified
     * destination.
     * @return A HashMap of name value pair of information
     */
    public abstract HashMap getStorageInfo(Destination destination)
	throws BrokerException;

    /**
     * Return the type of store.
     * @return A String
     */
    public abstract String getStoreType();

    /**
     * Return true if the store is a JDBC Store.
     * @return true if the store is a JDBC Store
     */
    public boolean isJDBCStore() {
        return true;
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */
    public abstract Hashtable getDebugState()
	throws BrokerException;

    // HA cluster support APIs

    /**
     * Get the last heartbeat timestamp for a broker (HA support).
     *
     * @param brokerID the broker ID
     * @return the broker last heartbeat timestamp
     * @throws BrokerException
     */
    public long getBrokerHeartbeat(String brokerID)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Get the last heartbeat timestamps for all the brokers (HA support).
     *
     * @return a HashMap object where the key is the broker ID and the entry
     * value is the broker's heartbeat timestamps
     * @throws BrokerException
     */
    public HashMap getAllBrokerHeartbeats() throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Update the broker heartbeat timestamp to the current time (HA support).
     *
     * @param brokerID the broker ID
     * @return new heartbeat timestamp if successfully updated else null or exception
     * @throws BrokerException
     */
    public Long updateBrokerHeartbeat(String brokerID)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Update the broker heartbeat timestamp only if the specified
     * lastHeartbeat match the value store in the DB (HA support).
     *
     * @param brokerID the broker ID
     * @param lastHeartbeat the last heartbeat timestamp
     * @return new heartbeat timestamp if successfully updated else null or exception
     * @throws BrokerException
     */
    public Long updateBrokerHeartbeat(String brokerID,
                                      long lastHeartbeat)
                                      throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Add a broker to the store and set the state to INITIALIZED (HA support).
     *
     * @param brokerID the broker ID
     * @param sessionID the store session ID
     * @param URL the broker's URL
     * @param version the current version of the running broker
     * @param heartbeat heartbeat timestamp
     * @throws BrokerException
     */
    public void addBrokerInfo(String brokerID, String URL,
        BrokerState state, int version, long sessionID, long heartbeat)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }


    /**
     * Add a broker to the store and set the state to INITIALIZED (HA support).
     *
     * @param brokerID the broker ID
     * @param takeOverBkrID the broker ID taken over the store.
     * @param sessionID the store session ID
     * @param URL the broker's URL
     * @param version the current version of the running broker
     * @param heartbeat heartbeat timestamp
     * @throws BrokerException
     */
    public void addBrokerInfo(String brokerID, String takeOverBkrID, String URL,
        BrokerState state, int version, long sessionID, long heartbeat)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Update the broker info for the specified broker ID (HA support).
     *
     * @param brokerID the broker ID
     * @param updateType update Type
     * @param oldValue (depending on updateType)
     * @param newValue (depending on updateType)
     * @return current active store session UID if requested by updateType
     * @throws BrokerException
     */
    public UID updateBrokerInfo( String brokerID, int updateType,
                                  Object oldValue, Object newValue)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Remove inactive store sessions (HA support).
     *
     * Note: Will be called from the store session reaper thread.
     *
     * @throws BrokerException
     */
    public void removeInactiveStoreSession() throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Get the broker info for all brokers (HA support).
     *
     * @return a HashMap object consisting of HABrokerInfo objects.
     * @throws BrokerException
     */
     public HashMap getAllBrokerInfos()
        throws BrokerException {
        return getAllBrokerInfos(false);
    }

    /**
     * Get the broker info for all brokers (HA support).
     *
     * @param loadSession specify if store sessions should be retrieved
     * @return a HashMap object consisting of HABrokerInfo objects.
     * @throws BrokerException
     */
     public HashMap getAllBrokerInfos(boolean loadSession)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Get the broker info for the specified broker ID (HA support).
     *
     * @param brokerID the broker ID
     * @return a HABrokerInfo object that encapsulates general information
     * about broker in an HA cluster
     * @throws BrokerException
     */
    public HABrokerInfo getBrokerInfo(String brokerID)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Get broker info for all brokers in the HA cluster with the specified
     * state (HA support).
     *
     * @param state the broker state
     * @return A HashMap. The key is the broker ID and the value of each entry
     * is a HABrokerInfo object.
     * @throws BrokerException
     */
    public HashMap getAllBrokerInfoByState(BrokerState state)
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Get the broker that owns the specified store session ID (HA support).
     * @param sessionID store session ID
     * @return the broker ID
     * @throws BrokerException
     */
    public String getStoreSessionOwner( long sessionID )
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    public boolean ifOwnStoreSession( long sessionID, String brokerID )
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Get the broker that creates the specified store session ID (HA support).
     * @param sessionID store session ID
     * @return the broker ID
     * @throws BrokerException
     */
    public String getStoreSessionCreator( long sessionID )
        throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Update the state of a broker only if the current state matches the
     * expected state (HA support).
     *
     * @param brokerID the broker ID
     * @param newState the new state
     * @param expectedState the expected state
     * @return true if the state of the broker has been updated
     * @throws BrokerException
     */
    public boolean updateBrokerState(String brokerID, BrokerState newState,
        BrokerState expectedState, boolean local) throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Get the state of a broker (HA support).
     *
     * @param brokerID the broker ID
     * @return the state of the broker
     * @throws BrokerException
     */
    public BrokerState getBrokerState(String brokerID) throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Get the state for all brokers (HA support).
     *
     * @return an array of Object whose 1st element contains an ArrayList of
     *   broker IDs and the 2nd element contains an ArrayList of BrokerState.
     * @throws BrokerException
     */
    public Object[] getAllBrokerStates() throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Try to obtain the takeover lock by updating the target broker entry
     * with the new broker, heartbeat timestamp and state in the broker
     * table. A lock can only be obtained if the target broker is not being
     * takeover by another broker, and the specified lastHeartbeat and
     * expectedState match the value store in the DB. An exception is thrown
     * if we are unable to get the lock. (HA Support)
     *
     * @param brokerID the new broker ID
     * @param targetBrokerID the broker ID of the store being taken over
     * @param lastHeartbeat the last heartbeat timestamp of the broker being takenover
     * @param expectedState the expected state of the broker being takenover
     * @throws TakeoverLockException if the current broker is unable to acquire
     *      the takeover lock
     * @throws BrokerException
     */
    public void getTakeOverLock(String brokerID, String targetBrokerID,
        long lastHeartbeat, BrokerState expectedState,
        long newHeartbeat, BrokerState newState, boolean force,
        TakingoverTracker tracker)
        throws TakeoverLockException, BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Take over a store by updating all relevant info of the target
     * broker with the new broker only if the current broker has already
     * obtained the takeover lock. This include updating the Message Table,
     * and Transaction Table (HA support).
     *
     * Note: This method should only be called after a broker is successfull
     * acquired a takeover lock by calling getTakeOverLock() method.
     *
     * @param brokerID the new broker ID
     * @param targetBrokerID the broker ID of the store being taken over
     * @return takeOverStoreInfo object that contains relevant info of the
     *      broker being taken over.
     * @throws BrokerException
     */
    public TakeoverStoreInfo takeOverBrokerStore(String brokerID,
        String targetBrokerID, TakingoverTracker tracker) 
        throws TakeoverLockException, BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Synchronize data associated with the specified destination to disk.
     * If null is specified, data assocated with all persisted destinations
     * will be synchronized.
     */
    public void syncDestination(Destination destination)
	throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Synchronize data associated with the specified interest to disk.
     * If null is specified, data assocated with all persisted interests
     * will be synchronized.
     */
    public void syncInterest(Consumer interest) throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Synchronize data associated with the specified transaction to disk.
     * If null is specified, data assocated with all persisted transactions
     * will be synchronized.
     */
    public void syncTransaction(TransactionUID txnID) throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Synchronize data associated with all persisted configuration change
     * records to disk.
     */
    public void syncConfigRecord() throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Synchronize all persisted data.
     */
    public void sync() throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Synchronize the store for txn log checkpoint.
     */
    public void syncStore(TransactionLogWriter[] logWriters)
        throws IOException, BrokerException {
        // Holds on to the closedLock so that no new operation will start
        synchronized (closedLock) {

            // wait until all current store operations are done
            synchronized (inprogressLock) {
                while (inprogressCount > 0) {
                    try {
                        inprogressLock.wait();
                    } catch (Exception e) {
                    }
                }
            }

            // sync msg and txn stores
            syncDestination(null);
            syncTransaction(null);

            // Indicate store is in sync with the log files; this needs to
            // be done while the store is still locked.
            for (int i = 0, len = logWriters.length; i < len; i++) {
                logWriters[i].checkpoint();
            }
        }
    }
    
    /**
     * Synchronize the store for txn log checkpoint.
     * Used by the new transaction log implementation
     */
    public void syncStoreOnCheckpoint()
			throws IOException, BrokerException {

		try {

			// prevent other transactions from starting
			// and wait for all in progress transactions complete
			// System.out.println("waiting for exclusive txn log lock");
			txnLogExclusiveLock.lock();

			
/*	
 * 
 * dont lock store as it can cause deadlock
 * Instead we will rely on txnLogExclusiveLock to stop other threads
 * 		
			// Holds on to the closedLock so that no new operation will start
			synchronized (closedLock) {

				// wait until all current store operations are done
				synchronized (inprogressLock) {
					while (inprogressCount > 0) {
						try {
							inprogressLock.wait();
						} catch (Exception e) {
						}
					}
				}
*/				
				// sync msg and txn stores
				syncDestination(null);
				syncTransaction(null);
/*				
			}
*/			
		
		
		} finally {
			// System.out.println("releasing exclusive txn log lock");
			txnLogExclusiveLock.unlock();
		}
	}

    public boolean initTxnLogger() throws BrokerException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }
    
    public void logTxn(BaseTransaction txnWork) throws BrokerException {
    	  throw new UnsupportedOperationException(
    	            "Operation not supported by the " + getStoreType() + " store" );
    }
      
      public void logTxnCompletion(TransactionUID tid, int state, int type) throws BrokerException {
    	  throw new UnsupportedOperationException(
    	            "Operation not supported by the " + getStoreType() + " store" );
    }
      
      public void loggedCommitWrittenToMessageStore(TransactionUID tid, int type) {
    	  throw new UnsupportedOperationException(
    	            "Operation not supported by the " + getStoreType() + " store" );
    }
      

    public void logTxn(int type, byte[] data) throws IOException {
        throw new UnsupportedOperationException(
            "Operation not supported by the " + getStoreType() + " store" );
    }

    /**
     * Return the LoadException for loading destinations; null if there's
     * none.
     */
    public LoadException getLoadDestinationException() {
	return null;
    }

    /**
     * Return the LoadException for loading consumers; null if there's none.
     */
    public LoadException getLoadConsumerException() {
	return null;
    }

    /**
     * Return the LoadException for loading Properties; null if there's none.
     */
    public LoadException getLoadPropertyException() {
	return null;
    }

    /**
     * Return the LoadException for loading transactions; null if there's none.
     */
    public LoadException getLoadTransactionException() {
	return null;
    }

    /**
     * Return the LoadException for loading transaction acknowledgements;
     * null if there's none.
     */
    public LoadException getLoadTransactionAckException() {
	return null;
    }

    // all close() and close(boolean) implemented by subclasses should
    // call this first to make sure all store operations are done
    // before preceeding to close the store.
    protected void setClosedAndWait() {
	// set closed to true so that no new operation will start
	synchronized (closedLock) {
	    closed = true;
	}

    beforeWaitOnClose();

	// wait until all current store operations are done
	synchronized (inprogressLock) {
	    while (inprogressCount > 0) {
		try {
		    inprogressLock.wait();
		} catch (Exception e) {
		}
	    }
	}
    }

    protected void beforeWaitOnClose() {
    }

    /**
     * This method should be called by all store apis to make sure
     * that the store is not closed before doing the operation.
     * @throws BrokerException
     */
    protected void checkClosedAndSetInProgress() throws BrokerException {
	synchronized (closedLock) {
	    if (closed) {
		logger.log(Logger.ERROR, BrokerResources.E_STORE_ACCESSED_AFTER_CLOSED);
		throw new BrokerException(
			br.getString(BrokerResources.E_STORE_ACCESSED_AFTER_CLOSED));
	    } else {
		// increment inprogressCount
		setInProgress(true);
	    }
	}
    }

    /**
     * If the flag is true, the inprogressCount is incremented;
     * if the flag is false, the inprogressCount is decremented;
     * when inprogressCount reaches 0; it calls notify on inprogressLock
     * to wait up anyone waiting on that
     *
     * @param flag
     */
    protected void setInProgress(boolean flag) {
	synchronized (inprogressLock) {
	    if (flag) {
		inprogressCount++;
	    } else {
		inprogressCount--;
	    }

	    if (inprogressCount == 0) {
		inprogressLock.notify();
	    }
	}
    }

    // used internally to check whether the store is closed
    public boolean closed() {
	synchronized (closedLock) {
	    return closed;
	}
    }

    public boolean upgradeNoBackup() {
	return upgradeNoBackup;
    }

    protected boolean getConfirmation() throws BrokerException {
	try {
	    // get confirmation
	    String yes = br.getString(BrokerResources.M_RESPONSE_YES);
	    String yes_s = br.getString(BrokerResources.M_RESPONSE_YES_SHORT);
	    String no_s = br.getString(BrokerResources.M_RESPONSE_NO_SHORT);

	    String objs[] = { yes_s, no_s };

	    System.out.print(br.getString(
				BrokerResources.M_UPGRADE_NOBACKUP_CONFIRMATION, objs));
	    System.out.flush();

	    String val = (new BufferedReader(new InputStreamReader
				(System.in))).readLine();

	    // if not positive confirmation, just exit!
	    if (!yes_s.equalsIgnoreCase(val) && !yes.equalsIgnoreCase(val)) {

		System.err.println(br.getString(BrokerResources.I_STORE_NOT_UPGRADED));
                Broker.getBroker().exit(1,
                      br.getString(BrokerResources.I_STORE_NOT_UPGRADED),
                      BrokerEvent.Type.FATAL_ERROR);
	    }
	    return true;

	} catch (IOException ex) {
            logger.log(Logger.ERROR, ex.toString());
	    throw new BrokerException(ex.toString(), ex);
	}
    }
    
    /**
     * Perform a checkpoint
     * Only applicable to FileSTore with new txn log
     *
     * @param sync Flag to determine whther method block until checpoint is complete    
     * @return status of checkpoint. Will return 0 if completed ok.
     */
    public int doCheckpoint(boolean sync) {
    	 throw new UnsupportedOperationException("doCheckpoint method only implemented for FileStore");
    	
    }


    /******************************************************************
     * Extended Store Interface methods, JMSBridgeStore (JDBC only)
     ******************************************************************/

    /**
     * Store a log record
     *
     * @param xid the global XID 
     * @param logRecord the log record data for the xid
     * @param name the jmsbridge name
     * @param sync - not used
     * @param logger_ can be null
     * @exception DupKeyException if already exist
     *            else Exception on error
     */
    public void storeTMLogRecord(String xid, byte[] logRecord,
                                 String name, boolean sync,
                                 java.util.logging.Logger logger_)
                                 throws DupKeyException, Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    /**
     * Update a log record
     *
     * @param xid the global XID 
     * @param logRecord the new log record data for the xid
     * @param name the jmsbridge name
     * @param callback to obtain updated data
     * @param addIfNotExist
     * @param sync - not used
     * @param logger_ can be null
     * @exception KeyNotFoundException if not found and addIfNotExist false
     *            else Exception on error
     */
    public void updateTMLogRecord(String xid, byte[] logRecord, 
                                  String name,
                                  UpdateOpaqueDataCallback callback,
                                  boolean addIfNotExist,
                                  boolean sync,
                                  java.util.logging.Logger logger_)
                                  throws DupKeyException, Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    /**
     * Remove a log record
     *
     * @param xid the global XID 
     * @param name the jmsbridge name
     * @param sync - not used
     * @param logger_ can be null
     * @exception KeyNotFoundException if not found 
     *            else Exception on error
     */
    public void removeTMLogRecord(String xid, String name,
                                  boolean sync,
                                  java.util.logging.Logger logger_)
                                  throws KeyNotFoundException, Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    /**
     * Get a log record
     *
     * @param xid the global XID 
     * @param name the jmsbridge name
     * @param logger_ can be null
     * @return null if not found
     * @exception Exception if error
     */
    public byte[] getTMLogRecord(String xid, String name,
                                 java.util.logging.Logger logger_)
                                 throws Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");

    }


    /**
     * Get last update time of a log record
     *
     * @param xid the global XID 
     * @param name the jmsbridge name
     * @param logger_ can be null
     * @exception KeyNotFoundException if not found 
     *            else Exception on error
     */
    public long getTMLogRecordUpdatedTime(String xid, String name,
                                          java.util.logging.Logger logger_)
                                          throws KeyNotFoundException, Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    /**
     * Get a log record creation time
     *
     * @param xid the global XID 
     * @param name the jmsbridge name
     * @param logger_ can be null
     * @exception KeyNotFoundException if not found 
     *            else Exception on error
     */
    public long getTMLogRecordCreatedTime(String xid, String name,
                                          java.util.logging.Logger logger_)
                                          throws KeyNotFoundException, Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    /**
     * Get all log records for a JMS bridge in this broker
     *
     * @param name the jmsbridge name
     * @param logger_ can be null
     * @return a list of log records
     * @exception Exception if error
     */
    public List getTMLogRecordsByName(String name,
                                      java.util.logging.Logger logger_)
                                      throws Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");

    }

    /**
     * Get all log records for a JMS bridge in a broker
     *
     * @param name the jmsbridge name
     * @param logger_ can be null
     * @return a list of log records
     * @exception Exception if error
     */
    public List getTMLogRecordsByNameByBroker(String name,
                                              String brokerID,
                                              java.util.logging.Logger logger_)
                                              throws Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");

    }

    /**
     * Get JMS bridge names in all log records owned by a broker
     *
     * @param brokerID
     * @param logger_ can be null
     * @return a list of log records
     * @exception Exception if error
     */
    public List getNamesByBroker(String brokerID,
                                 java.util.logging.Logger logger_)
                                 throws Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");

    }

    /**
     * Get keys for all log records for a JMS bridge in this broker
     *
     * @param name the jmsbridge name
     * @param logger_ can be null
     * @return a list of keys
     * @exception Exception if error
     */
    public List getTMLogRecordKeysByName(String name,
                                         java.util.logging.Logger logger_)
                                         throws Exception {
        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }


    /**
     * Add a JMS Bridge
     *
     * @param name jmsbridge name
     * @param sync - not used
     * @param logger_ can be null
     * @exception DupKeyException if already exist 
     *            else Exception on error
     */
    public void addJMSBridge(String name, boolean sync,
                             java.util.logging.Logger logger_)
                             throws DupKeyException, Exception { 

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    /**
     * Get JMS bridges owned by this broker
     *
     * @param logger_ can be null
     * @return list of names
     * @exception Exception if error
     */
    public List getJMSBridges(java.util.logging.Logger logger_)
                             throws Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    /**
     * Get JMS bridges owned by a broker
     *
     * @param brokerID name
     * @param logger_ can be null
     * @return list of names
     * @exception Exception if error
     */
    public List getJMSBridgesByBroker(String brokerID,
                                      java.util.logging.Logger logger_)
                                      throws Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }


    /**
     * @param name jmsbridge name
     * @param logger_ can be null;
     * @return updated time
     * @throws KeyNotFoundException if not found
     *         else Exception on error
     */
    public long getJMSBridgeUpdatedTime(String name,
                                        java.util.logging.Logger logger_)
                                        throws KeyNotFoundException, Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    /**
     * @param name jmsbridge name
     * @param logger_ can be null;
     * @return created time
     * @throws KeyNotFoundException if not found
     *         else Exception on error
     */
    public long getJMSBridgeCreatedTime(String name,
                                        java.util.logging.Logger logger_)
                                        throws KeyNotFoundException, Exception {

        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    public void closeJMSBridgeStore() throws Exception {
        throw new UnsupportedOperationException(
        "Operation not supported by the "+getStoreType() + " store");
    }

    public boolean isTxnConversionRequired()
    {
    	return false;
    }
    
    public void convertTxnFormats(TransactionList transactionList) throws BrokerException, IOException
    {
    	
    }

}


