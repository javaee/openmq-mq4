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
 * @(#)InMemoryStore.java	1.0 10/28/08
 */ 

package com.sun.messaging.jmq.jmsserver.persist.inmemory;

import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.DestMetricsCounters;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.core.*;
import com.sun.messaging.jmq.jmsserver.data.TransactionAcknowledgement;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.data.TransactionBroker;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * InMemoryStore provides file based persistence.
 * <br>
 * Note that some methods are NOT synchronized.
 */
public class InMemoryStore extends Store {

    // current version of store
    public static final int STORE_VERSION = 100;

    static final String INMEMORY_PROP_PREFIX = Globals.IMQ + ".persist.inmemory.";

    static final private ConsumerUID[] emptyiid = new ConsumerUID[0];
    static final private int[] emptystate = new int[0];

    /**
     * Instance variables
     */

    private AtomicBoolean isClosed = new AtomicBoolean(true);

    // object encapsulates persistence of messages and their interest lists
    private MsgStore msgStore = null;

    // object encapsulates persistence of interests
    private InterestStore intStore = null;

    // object encapsulates persistence of destinations
    private DestinationStore dstStore = null;

    // object encapsulates persistence of transaction ids
    private TxnStore txnStore = null;

    // object encapsulates persistence of configuration change record
    private ConfigRecordStore configStoreStore = null;

    // object encapsulates persistence of properties
    private PropertiesStore propStore = null;

    /**
     * When instantiated, the object configures itself by reading the
     * properties specified in BrokerConfig.
     */
    public InMemoryStore() {

        logger.logToAll(Logger.WARNING,
            "Using in-memory store - ALL DATA WILL BE LOST WHEN THE BROKER IS SHUT DOWN!");

        // destinations lists
        dstStore = new DestinationStore(this);

        msgStore = new MsgStore(this);

        intStore = new InterestStore();

        // transaction ids and associated ack lists
        txnStore = new TxnStore();

        // configuration change record
        configStoreStore = new ConfigRecordStore();

        // properties
        propStore = new PropertiesStore();

        isClosed.set(false); // store is available!
    }

    /**
     * Get the file store version.
     * @return file store version
     */
    public final int getStoreVersion() {
        return STORE_VERSION;
    }

    /**
     * Close the store and releases any system resources associated with it.
     */
    public void close(boolean cleanup) {

	// make sure all operations are done before we proceed to close
	setClosedAndWait();

        dstStore.close(cleanup);
        msgStore.close(cleanup);
        intStore.close(cleanup);
        txnStore.close(cleanup);
        configStoreStore.close(cleanup);
        propStore.close(cleanup);

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.close("+ cleanup +") done.");
	}
    }

    /**
     * Clear the store. Remove all persistent data.
     * Note that this method is not synchronized.
     */
    public void clearAll(boolean sync) throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.clearAll() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            msgStore.clearAll();
            intStore.clearAll();
            dstStore.clearAll(false);// don't worry about messages since
                                        // they are removed already
            txnStore.clearAll();
            configStoreStore.clearAll();
            propStore.clearAll();

	    if (Store.getDEBUG()) {
		logger.log(Logger.DEBUG, "InMemory store cleared");
	    }
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Store a message, which is uniquely identified by it's SysMessageID,
     * and it's list of interests and their states.
     *
     * @param message	the message to be persisted
     * @param iids	an array of interest ids whose states are to be
     *			stored with the message
     * @param states	an array of states
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting the data
     * @exception BrokerException if a message with the same id exists
     *			in the store already
     * @exception NullPointerException	if <code>dst</code>,
     *			<code>message</code>,
     *			<code>iids</code>, or <code>states</code> is
     *			<code>null</code>
     */
    public void storeMessage(DestinationUID dst, Packet message,
	ConsumerUID[] iids, int[] states, boolean sync)
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
                "InMemoryStore.storeMessage() called for " + message.getSysMessageID());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {

            if (dst == null || message == null || iids == null || states == null) {
                throw new NullPointerException();
            }

            if (iids.length == 0 || iids.length != states.length) {
                throw new BrokerException(br.getString(BrokerResources.E_BAD_INTEREST_LIST));
            }

            msgStore.storeMessage(dst, message, iids, states);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Store a message which is uniquely identified by it's system message id.
     *
     * @param message	the readonly packet to be persisted
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting the message
     * @exception BrokerException if a message with the same id exists
     *			in the store already
     */
    public void storeMessage(DestinationUID dst, Packet message, boolean sync)
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.storeMessage() called for "
			+ message.getSysMessageID());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    msgStore.storeMessage(dst, message, emptyiid, emptystate);

	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove the message from the persistent store.
     * If the message has an interest list, the interest list will be
     * removed as well.
     *
     * @param id	the system message id of the message to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the message
     * @exception BrokerException if the message is not found in the store
     */
    public void removeMessage(DestinationUID dst, SysMessageID id, boolean sync, boolean onRollback)
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "FileStore.removeMessage() called for "
			+ dst + ";" + id);
	}

	if (id == null) {
	    throw new NullPointerException();
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    msgStore.removeMessage(dst, id);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Move the message from one destination to another.
     * The message will be stored in the target destination with the
     * passed in consumers and their corresponding states.
     * After the message is persisted successfully, the message in the
     * original destination will be removed.
     *
     * @param message	message to be moved
     * @param from	destination the message is currently in 
     * @param to	destination to move the message to
     * @param ints	an array of interest ids whose states are to be
     *			stored with the message
     * @param states	an array of states
     * @param sync	if true, will synchronize data to disk.
     * @exception IOException if an error occurs while moving the message
     * @exception BrokerException if the message is not found in source
     *		destination
     * @exception NullPointerException	if <code>message</code>, 
     *			<code>from</code>, <code>to</code>,
     *			<code>iids</code>, or <code>states</code> is
     *			<code>null</code>
     */
    public void moveMessage(Packet message, DestinationUID from,
	DestinationUID to, ConsumerUID[] ints, int[] states, boolean sync)
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
		"InMemoryStore.moveMessage() called for: "
			+ message.getSysMessageID() + " from "
			+ from + " to " + to);
	}

	if (message == null || from == null || to == null) {
	    throw new NullPointerException();
	}

	if (ints == null) {
	    ints = emptyiid;
	    states = emptystate;
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    msgStore.moveMessage(message, from, to, ints, states);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove all messages associated with the specified destination
     * from the persistent store.
     *
     * @param destination	the destination whose messages are to be
     *				removed
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the messages
     * @exception BrokerException if the destination is not found in the store
     * @exception NullPointerException	if <code>destination</code> is
     *			<code>null</code>
     */
    public void removeAllMessages(Destination destination, boolean sync)
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
		"InMemoryStore.removeAllMessages(Destination) called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    msgStore.removeAllMessages(destination.getDestinationUID(), sync);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

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
     * @return an enumeration of all persisted messages, an empty
     *		enumeration will be returned if no messages exist for the
     *		destionation
     * @exception BrokerException if an error occurs while getting the data
     */
    public Enumeration messageEnumeration(Destination dst)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
		"InMemoryStore.messageEnumeration(Destination) called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return msgStore.messageEnumeration(dst.getDestinationUID());
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Return the message with the specified message id.
     *
     * @param id	the message id of the message to be retrieved
     * @return a message
     * @exception BrokerException if the message is not found in the store
     *			or if an error occurs while getting the data
     */
    public Packet getMessage(DestinationUID dst, String id)
	throws BrokerException {
        return getMessage(dst, SysMessageID.get(id));
    }

    /**
     * Return the message with the specified message id.
     *
     * @param id	the system message id of the message to be retrieved
     * @return a message
     * @exception BrokerException if the message is not found in the store
     *			or if an error occurs while getting the data
     */
    public Packet getMessage(DestinationUID dst, SysMessageID id)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.getMessage() called");
	}

	if (id == null) {
	    throw new NullPointerException();
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return msgStore.getMessage(dst, id);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Store the given list of interests and their states with the
     * specified message.  The message should not have an interest
     * list associated with it yet.
     *
     * @param mid	system message id of the message that the interest
     *			is associated with
     * @param iids	an array of interest ids whose states are to be
     *			stored
     * @param states	an array of states
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if the message is not in the store;
     *				if there's an interest list associated with
     *				the message already; or if an error occurs
     *				while persisting the data
     */
    public void storeInterestStates(DestinationUID dst,
	SysMessageID mid, ConsumerUID[] iids, int[] states, boolean sync, Packet msg)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.storeInterestStates() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (mid == null || iids == null || states == null) {
		throw new NullPointerException();
	    }

	    if (iids.length == 0 || iids.length != states.length) {
		throw new BrokerException(br.getString(BrokerResources.E_BAD_INTEREST_LIST));
	    }

	    msgStore.storeInterestStates(dst, mid, iids, states, sync);

	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Update the state of the interest associated with the specified
     * message.  The interest should already be in the interest list
     * of the message.
     *
     * @param mid	system message id of the message that the interest
     *			is associated with
     * @param id	id of the interest whose state is to be updated
     * @param state	state of the interest
     * @param sync	if true, will synchronize data to disk
     * @param txid  current transaction id. Unused by inMemoryStore store
     * @param isLastAck Unused by inMemoryStore store
     * @exception BrokerException if the message is not in the store; if the
     *			interest is not associated with the message; or if
     *			an error occurs while persisting the data
     */
    public void updateInterestState(DestinationUID dst,
	SysMessageID mid, ConsumerUID id, int state, boolean sync, TransactionUID txid, boolean isLastAck)
    	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.updateInterestState() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (mid == null || id == null) {
		throw new NullPointerException();
	    }

	    msgStore.updateInterestState(dst, mid, id, state, sync);

	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Get the state of the interest associated with the specified
     * message.
     *
     * @param mid	system message id of the message that the interest
     *			is associated with
     * @param id	id of the interest whose state is to be returned
     * @return state of the interest
     * @exception BrokerException if the specified interest is not
     *		associated with the message; or if the message is not in the
     *		store
     */
    public int getInterestState(DestinationUID dst, SysMessageID mid,
	ConsumerUID id) throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.getInterestState() called");
	}

	if (mid == null || id == null) {
	    throw new NullPointerException();
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return msgStore.getInterestState(dst, mid, id);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Retrieve all interests and states associated with the specified message.
     * @param did	the destination the message is associated with
     * @param mid	the system message id of the message that the interest
     * @return HashMap of containing all consumer's state
     * @throws BrokerException
     */
    public HashMap getInterestStates(DestinationUID did,
        SysMessageID mid) throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG, "InMemoryStore.getInterestStates() called");
        }

        if (mid == null) {
            throw new NullPointerException();
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            return msgStore.getInterestStates(did, mid);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }

    }

    /**
     * Retrieve all interest IDs associated with the specified message.
     * Note that the state of the interests returned is either
     * INTEREST_STATE_ROUTED or INTEREST_STATE_DELIVERED, i.e., interest
     * whose state is INTEREST_STATE_ACKNOWLEDGED will not be returned in the
     * array.
     *
     * @param mid	system message id of the message whose interests
     *			are to be returned
     * @return an array of ConsumerUID objects associated with the message; a
     *		zero length array will be returned if no interest is
     *		associated with the message
     * @exception BrokerException if the message is not in the store
     */
    public ConsumerUID[] getConsumerUIDs(DestinationUID dst, SysMessageID mid)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.getConsumerUIDs() called");
	}

	if (mid == null) {
	    throw new NullPointerException();
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return msgStore.getConsumerUIDs(dst, mid);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Check if a a message has been acknowledged by all interests.
     *
     * @param dst  the destination the message is associated with
     * @param id   the system message id of the message to be checked
     * @return true if all interests have acknowledged the message;
     * false if message has not been routed or acknowledge by all interests
     * @throws BrokerException
     */
    public boolean hasMessageBeenAcked(DestinationUID dst, SysMessageID id)
        throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.hasMessageBeenAcked() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            return msgStore.hasMessageBeenAcked(dst, id);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    /**
     * Store an Interest which is uniquely identified by it's id.
     *
     * @param interest the interest to be persisted
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting the interest
     * @exception BrokerException if an interest with the same id exists in
     *			the store already
     */
    public void storeInterest(Consumer interest, boolean sync)
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.storeInterest() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    intStore.storeInterest(interest);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove the interest from the persistent store.
     *
     * @param interest	the interest to be removed from persistent store
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the interest
     * @exception BrokerException if the interest is not found in the store
     */
    public void removeInterest(Consumer interest, boolean sync)
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.removeInterest() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    intStore.removeInterest(interest);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Retrieve all interests in the store.
     * Will return as many interests as we can read.
     * Any interests that are retrieved unsuccessfully will be logged as a
     * warning.
     *
     * @return an array of Interest objects; a zero length array is
     * returned if no interests exist in the store
     * @exception IOException if an error occurs while getting the data
     */
    public Consumer[] getAllInterests() throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.getAllInterests() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return intStore.getAllInterests();
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Store a Destination.
     *
     * @param destination	the destination to be persisted
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting
     *		the destination
     * @exception BrokerException if the same destination exists
     *			the store already
     * @exception NullPointerException	if <code>destination</code> is
     *			<code>null</code>
     */
    public void storeDestination(Destination destination, boolean sync)
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.storeDestination() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (destination == null) {
		throw new NullPointerException();
	    }

	    dstStore.storeDestination(destination);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Update the specified destination.
     *
     * @param destination	the destination to be updated
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if the destination is not found in the store
     *				or if an error occurs while updating the
     *				destination
     */
    public void updateDestination(Destination destination, boolean sync)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.updateDestination() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (destination == null) {
		throw new NullPointerException();
	    }

	    dstStore.updateDestination(destination);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove the destination from the persistent store.
     * All messages associated with the destination will be removed as well.
     *
     * @param destination	the destination to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the destination
     * @exception BrokerException if the destination is not found in the store
     */
    public void removeDestination(Destination destination, boolean sync)
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.removeDestination() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (destination == null) {
		throw new NullPointerException();
	    }

	    dstStore.removeDestination(destination);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Retrieve a destination in the store.
     *
     * @param id the destination ID
     * @return a Destination object
     * @throws BrokerException if no destination exist in the store
     */
    public Destination getDestination(DestinationUID id)
        throws IOException, BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG, "InMemoryStore.getDestination() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            if (id == null) {
                throw new NullPointerException();
            }

            return dstStore.getDestination(id);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    /**
     * Retrieve all destinations in the store.
     *
     * @return an array of Destination objects; a zero length array is
     * returned if no destinations exist in the store
     * @exception IOException if an error occurs while getting the data
     */
    public Destination[] getAllDestinations()
	throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.getAllDestinations() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return dstStore.getAllDestinations();
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Store a transaction.
     *
     * @param id	the id of the transaction to be persisted
     * @param ts	the transaction state to be persisted
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting
     *		the transaction
     * @exception BrokerException if the same transaction id exists
     *			the store already
     * @exception NullPointerException	if <code>id</code> is
     *			<code>null</code>
     */
    public void storeTransaction(TransactionUID id, TransactionState ts,
        boolean sync) throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.storeTransaction() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (id == null || ts == null) {
		throw new NullPointerException();
	    }

	    txnStore.storeTransaction(id, ts, sync);

	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove the transaction. The associated acknowledgements
     * will not be removed.
     *
     * @param id	the id of the transaction to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the transaction
     * @exception BrokerException if the transaction is not found
     *			in the store
     */
    public void removeTransaction(TransactionUID id, boolean sync)
	throws IOException, BrokerException {

        removeTransaction(id, false, sync);
    }

    /**
     * Remove the transaction. The associated acknowledgements
     * will not be removed.
     *
     * @param id	the id of the transaction to be removed
     * @param removeAcks if true, will remove all associated acknowledgements
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the transaction
     * @exception BrokerException if the transaction is not found
     *			in the store
     */
    public void removeTransaction(TransactionUID id, boolean removeAcks,
        boolean sync) throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.removeTransaction() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (id == null) {
		throw new NullPointerException();
	    }

            if (removeAcks) {
                txnStore.removeTransactionAck(id);
            }

	    txnStore.removeTransaction(id, sync);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Update the state of a transaction
     *
     * @param id	the transaction id to be updated
     * @param ts	the new transaction state
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while persisting
     *		the transaction id
     * @exception BrokerException if the transaction id does NOT exists in
     *			the store already
     * @exception NullPointerException	if <code>id</code> is
     *			<code>null</code>
     */
    public void updateTransactionState(TransactionUID id, TransactionState ts,
        boolean sync) throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.updateTransactionState( id=" +
                id + ", ts=" + ts.getState() + ") called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (id == null) {
		throw new NullPointerException();
	    }

	    txnStore.updateTransactionState(id, ts.getState(), sync);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Retrieve all local and cluster transaction ids in the store
     * with their state
     *
     * @return A HashMap. The key is a TransactionUID.
     * The value is a TransactionState.
     * @exception IOException if an error occurs while getting the data
     */
    public HashMap getAllTransactionStates()
        throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
			"InMemoryStore.getAllTransactionStates() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return txnStore.getAllTransactionStates();
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Store the acknowledgement for the specified transaction.
     *
     * @param tid	the transaction id with which the acknowledgment is to
     *			be stored
     * @param ack	the acknowledgement to be stored
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if the transaction id is not found in the
     *				store, if the acknowledgement already
     *				exists, or if it failed to persist the data
     */
    public void storeTransactionAck(TransactionUID tid,
	TransactionAcknowledgement ack, boolean sync) throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.storeTransactionAck() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (tid == null || ack == null) {
		throw new NullPointerException();
	    }

	    txnStore.storeTransactionAck(tid, ack, sync);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove all acknowledgements associated with the specified
     * transaction from the persistent store.
     *
     * @param id	the transaction id whose acknowledgements are
     *			to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if error occurs while removing the
     *			acknowledgements
     */
    public void removeTransactionAck(TransactionUID id, boolean sync)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.removeTransactionAck() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (id == null)
		throw new NullPointerException();

	    txnStore.removeTransactionAck(id);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Retrieve all acknowledgements for the specified transaction.
     *
     * @param tid	id of the transaction whose acknowledgements
     *			are to be returned
     * @exception BrokerException if the operation fails for some reason
     */
    public TransactionAcknowledgement[] getTransactionAcks(
	TransactionUID tid) throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.getTransactionAcks() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (tid == null) {
		throw new NullPointerException();
	    }

	    return txnStore.getTransactionAcks(tid);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Retrieve all acknowledgement list in the persistence store together
     * with their associated transaction id. The data is returned in the
     * form a HashMap. Each entry in the HashMap has the transaction id as
     * the key and an array of the associated TransactionAcknowledgement
     * objects as the value.
     * @return a HashMap object containing all acknowledgement lists in the
     *		persistence store
     */
    public HashMap getAllTransactionAcks() throws BrokerException {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
			"InMemoryStore.getAllTransactionAcks() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return txnStore.getAllTransactionAcks();
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    public void storeTransaction(TransactionUID id, TransactionInfo txnInfo,
        boolean sync) throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.storeTransaction() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            txnStore.storeTransaction(id, txnInfo, sync);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void storeClusterTransaction(TransactionUID id, TransactionState ts,
        TransactionBroker[] txnBrokers, boolean sync) throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.storeClusterTransaction() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            txnStore.storeClusterTransaction(id, ts, txnBrokers, sync);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void updateClusterTransaction(TransactionUID id,
        TransactionBroker[] txnBrokers, boolean sync) throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.updateClusterTransaction() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            txnStore.updateClusterTransaction(id, txnBrokers, sync);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public TransactionBroker[] getClusterTransactionBrokers(TransactionUID id)
        throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.updateClusterTransactionBrokerState() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            return txnStore.getClusterTransactionBrokers(id);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void updateClusterTransactionBrokerState(TransactionUID id,
        int expectedTxnState, TransactionBroker txnBroker, boolean sync)
        throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.updateClusterTransactionBrokerState() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            txnStore.updateTransactionBrokerState(
                id, expectedTxnState, txnBroker, sync);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void storeRemoteTransaction(TransactionUID id, TransactionState ts,
        TransactionAcknowledgement[] txnAcks, BrokerAddress txnHomeBroker,
        boolean sync) throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.storeRemoteTransaction() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            txnStore.storeRemoteTransaction(id, ts, txnAcks, txnHomeBroker, sync);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public BrokerAddress getRemoteTransactionHomeBroker(TransactionUID id)
        throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.getRemoteTransactionHomeBroker() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            return txnStore.getRemoteTransactionHomeBroker(id);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public HashMap getAllRemoteTransactionStates()
        throws IOException, BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.getAllRemoteTransactionStates() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            return txnStore.getAllRemoteTransactionStates();
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public TransactionState getTransactionState(TransactionUID id)
        throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.getTransactionState() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            return txnStore.getTransactionState(id);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public TransactionInfo getTransactionInfo(TransactionUID id)
        throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.getTransactionInfo() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            return txnStore.getTransactionInfo(id);
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public Collection getTransactions(String brokerID)
        throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.getTransactions() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            return txnStore.getAllTransactions();
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    /**
     * Persist the specified property name/value pair.
     * If the property identified by name exists in the store already,
     * it's value will be updated with the new value.
     * If value is null, the property will be removed.
     * The value object needs to be serializable.
     *
     * @param name name of the property
     * @param value value of the property
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if an error occurs while persisting the
     *			data
     * @exception NullPointerException if <code>name</code> is
     *			<code>null</code>
     */
    public void updateProperty(String name, Object value, boolean sync)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.updateProperty() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (name == null)
		throw new NullPointerException();

	    propStore.updateProperty(name, value);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

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
    public Object getProperty(String name) throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.getProperty() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (name == null)
		throw new NullPointerException();

	    return propStore.getProperty(name);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Return the names of all persisted properties.
     *
     * @return an array of property names; an empty array will be returned
     *		if no property exists in the store.
     */
    public String[] getPropertyNames() throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.getPropertyNames() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return propStore.getPropertyNames();
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Return all persisted properties.
     *
     * @return a properties object.
     */
    public Properties getAllProperties() throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG, "InMemoryStore.getAllProperties() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            return propStore.getProperties();
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    /**
     * Append a new record to the config change record store.
     * The timestamp is also persisted with the recordData.
     * The config change record store is an ordered list (sorted
     * by timestamp).
     *
     * @param timestamp The time when this record was created.
     * @param recordData The record data.
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if an error occurs while persisting
     *			the data or if the timestamp is less than or
     *			equal to 0
     * @exception NullPointerException if <code>recordData</code> is
     *			<code>null</code>
     */
    public void storeConfigChangeRecord(
	long timestamp, byte[] recordData, boolean sync)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
			"InMemoryStore.storeConfigChangeRecord() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    if (timestamp <= 0) {
		logger.log(Logger.ERROR, BrokerResources.E_INVALID_TIMESTAMP,
			new Long(timestamp));
		throw new BrokerException(
			br.getString(BrokerResources.E_INVALID_TIMESTAMP,
					new Long(timestamp)));
	    }

	    if (recordData == null) {
		throw new NullPointerException();
	    }

	    configStoreStore.storeConfigChangeRecord(timestamp, recordData, sync);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Get all the config change records since the given timestamp.
     * Retrieves all the entries with recorded timestamp greater than
     * the specified timestamp.
     * 
     * @return a list of ChangeRecordInfo, empty list if no record
     */
    public ArrayList<ChangeRecordInfo> getConfigChangeRecordsSince(long timestamp)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
			"InMemoryStore.getConfigChangeRecordsSince() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return configStoreStore.getConfigChangeRecordsSince(timestamp);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Return all config records with their corresponding timestamps.
     *
     * @return a list of ChangeRecordInfo
     * @exception BrokerException if an error occurs while getting the data
     */
    public List<ChangeRecordInfo> getAllConfigRecords() throws BrokerException {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
			"InMemoryStore.getAllConfigRecords() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return configStoreStore.getAllConfigRecords();
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Clear all config change records in the store.
     *
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if an error occurs while clearing the data
     */
    public void clearAllConfigChangeRecords(boolean sync)
	throws BrokerException {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
			"InMemoryStore.clearAllConfigChangeRecords() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    configStoreStore.clearAll();
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Return the number of persisted messages and total number of bytes for
     * the given destination.
     *
     * @param dst the destination whose messages are to be counted
     * @return A HashMap of name value pair of information
     * @throws BrokerException if an error occurs while getting the data
     */
    public HashMap getMessageStorageInfo(Destination dst)
        throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "InMemoryStore.getMessageStorageInfo(Destination) called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            DestinationUID dstID = dst.getDestinationUID();
            HashMap data = new HashMap(2);
            data.put( DestMetricsCounters.CURRENT_MESSAGES,
                new Integer(msgStore.getMessageCount(dstID)) );
            data.put( DestMetricsCounters.CURRENT_MESSAGE_BYTES,
                new Long(msgStore.getByteCount(dstID)) );
            return data;
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public String getStoreType() {
	return INMEMORY_STORE_TYPE;
    }

    public boolean isJDBCStore() {
        return false;
    }

    /**
     * Get information about the underlying storage for the specified
     * destination.
     * @return A HashMap of name value pair of information
     */
    public HashMap getStorageInfo(Destination destination)
	throws BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "InMemoryStore.getStorageInfo(" +
			destination + ") called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
	    return msgStore.getStorageInfo(destination);
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */
    public Hashtable getDebugState() {
	Hashtable t = new Hashtable();
	t.put("InMemoryStore version", String.valueOf(STORE_VERSION));

	t.putAll(dstStore.getDebugState());
	t.putAll(msgStore.getDebugState());
	t.putAll(intStore.getDebugState());
	t.putAll(txnStore.getDebugState());
	t.putAll(propStore.getDebugState());
        t.putAll(configStoreStore.getDebugState());
	return t;
    }

    /**
     * Compact the message file associated with the specified destination.
     * If null is specified, message files assocated with all persisted
     * destinations will be compacted..
     */
    public void compactDestination(Destination destination)
	throws BrokerException {
    }

    MsgStore getMsgStore() {
	return msgStore;
    }

    DestinationStore getDstStore() {
	return dstStore;
    }

    // all close() and close(boolean) implemented by subclasses should
    // call this first to make sure all store operations are done
    // before preceeding to close the store.
    protected void setClosedAndWait() {
	// set closed to true so that no new operation will start
        isClosed.set(true);

	// No need to wait until all current store operations are done
    }

    /**
     * This method should be called by all store apis to make sure
     * that the store is not closed before doing the operation.
     * @throws BrokerException
     */
    protected void checkClosedAndSetInProgress() throws BrokerException {
        if (isClosed.get()) {
            logger.log(Logger.ERROR, BrokerResources.E_STORE_ACCESSED_AFTER_CLOSED);
            throw new BrokerException(
                    br.getString(BrokerResources.E_STORE_ACCESSED_AFTER_CLOSED));
        }
    }

    /**
     * For in-memory store, this is a no-op
     *
     * @param flag
     */
    protected void setInProgress(boolean flag) {
    }
}

