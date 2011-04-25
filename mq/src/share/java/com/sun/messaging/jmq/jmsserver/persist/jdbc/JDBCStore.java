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
 * @(#)JDBCStore.java	1.163 07/24/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.jdbc;

import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.util.UID;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.core.*;
import com.sun.messaging.jmq.jmsserver.data.TransactionUID;
import com.sun.messaging.jmq.jmsserver.data.TransactionState;
import com.sun.messaging.jmq.jmsserver.data.TransactionAcknowledgement;
import com.sun.messaging.jmq.jmsserver.data.TransactionBroker;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.*;
import com.sun.messaging.jmq.jmsserver.service.TakingoverTracker;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.*;
import com.sun.messaging.jmq.jmsserver.Broker;
import com.sun.messaging.jmq.jmsserver.cluster.BrokerState;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.comm.BaseDAO;
import java.io.*;
import java.sql.*;
import java.util.*;

import com.sun.messaging.bridge.service.DupKeyException;
import com.sun.messaging.bridge.service.KeyNotFoundException;
import com.sun.messaging.bridge.service.UpdateOpaqueDataCallback;

/**
 * JDBCStore provides JDBC based persistence.
 * <br>
 * Note that some methods are NOT synchronized.
 */
public class JDBCStore extends Store implements DBConstants {

    private static boolean DEBUG = getDEBUG();

    public static final String LOCK_STORE_PROP =
        DBManager.JDBC_PROP_PREFIX + ".lockstore.enabled";

    private static final String MSG_ENUM_USE_CURSOR_PROP =
        DBManager.JDBC_PROP_PREFIX + ".msgEnumUseResultSetCursor";

    // current version of store
    public static final int OLD_STORE_VERSION_350 = 350;
    public static final int OLD_STORE_VERSION_370 = 370;
    public static final int OLD_STORE_VERSION_400 = 400;
    public static final int STORE_VERSION = 410;

    // database connection
    DBManager dbmgr;
    DAOFactory daoFactory;

    private HashMap pendingDeleteDsts = new HashMap(5);

    private HashMap takeoverLockMap = new HashMap();

    private StoreSessionReaperTask sessionReaper = null;
    private boolean msgEnumUseCursor = true;
    private List<Enumeration> dataEnums = Collections.synchronizedList(
                                          new ArrayList<Enumeration>()); 

    /**
     * When instantiated, the object configures itself by reading the
     * properties specified in BrokerConfig.
     */
    public JDBCStore() throws BrokerException {

        dbmgr = DBManager.getDBManager();
        daoFactory = dbmgr.getDAOFactory();

	// print out info messages
        String url = dbmgr.getOpenDBURL();
        if (url == null) {
            url = "not specified";
        }

        String user = dbmgr.getUser();
        if (user == null) {
            user = "not specified";
        }

        String msgArgs[] = { String.valueOf(STORE_VERSION), dbmgr.getBrokerID(), url, user };
        logger.logToAll(Logger.INFO,
            br.getString(BrokerResources.I_JDBC_STORE_INFO, msgArgs));

        if (createStore) {
            logger.log(Logger.INFO, BrokerResources.I_STORE_AUTOCREATE_ENABLED);
        } else {
            logger.log(Logger.INFO, BrokerResources.I_STORE_AUTOCREATE_DISABLED);
        }

        msgEnumUseCursor = config.getBooleanProperty( MSG_ENUM_USE_CURSOR_PROP, !dbmgr.isHADB() );

        Connection conn = null;
        Exception myex = null;
        try {
            conn = dbmgr.getConnection( true );

            // this will create, remove, reset, or upgrade old store
            if ( !checkStore( conn ) ) {
                // false=dont unlock; since tables are dropped already
                closeDB(false);
                return;
            }

            if ( Globals.getHAEnabled() ) {
                try {
                    // Schedule inactive store session reaper for every 24 hrs.
                    long period = 86400000; // 24 hours
                    long delay = 60000 + (long)(Math.random() * 240000); // 1 - 5 mins
                    sessionReaper = new StoreSessionReaperTask(this);
                    Globals.getTimer().schedule(sessionReaper, delay, period);
                    if (DEBUG) {
                        logger.log(Logger.DEBUG,
                            "Store session reaper task has been successfully scheduled [delay=" +
                            delay + ", period=" +period + "]" );
                    }
                } catch (IllegalStateException e) {
                    logger.log(Logger.INFO, BrokerResources.E_INTERNAL_BROKER_ERROR,
                        "Cannot schedule inactive store session reaper task, " +
                        "the broker is probably shutting down", e);
                }
            } else {
                // Lock the tables so that no other processes will access them
                if ( config.getBooleanProperty( LOCK_STORE_PROP, true ) ) {
                    DBManager.lockTables( conn, true );
                }
            }
        } catch (BrokerException e) {
            myex = e;
            throw e;
        } finally {
            Util.close( null, null, conn, myex );
        }

        dbmgr.setStoreInited(true);

        if (DEBUG) {
	        logger.log(Logger.DEBUG, "JDBCStore instantiated.");
        }
    }

    /**
     * Get the JDBC store version.
     * @return JDBC store version
     */
    public final int getStoreVersion() {
        return STORE_VERSION;
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
     * @exception BrokerException if a message with the same id exists
     *			in the store already
     */
    public void storeMessage(DestinationUID dst, Packet message,
	ConsumerUID[] iids, int[] states, boolean sync)
	throws BrokerException {

        if (message == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.storeMessage() called with message: " +
                message.getSysMessageID().getUniqueName());
	}

        if (iids.length == 0 || iids.length != states.length) {
            throw new BrokerException(br.getKString(
                BrokerResources.E_BAD_INTEREST_LIST));
        }

        storeMessage(dst, message, iids, states, getStoreSession(), true);
    }

    /**
     * Store a message which is uniquely identified by it's system message id.
     *
     * @param message	the readonly packet to be persisted
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if a message with the same id exists
     *			in the store already
     */
    public void storeMessage(DestinationUID dst, Packet message, boolean sync)
	throws BrokerException {

        if (message == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.storeMessage() called with message: " +
                message.getSysMessageID().getUniqueName());
	}

        storeMessage(dst, message, null, null, getStoreSession(), true);
    }

    protected void storeMessage(DestinationUID dst, Packet message,
	ConsumerUID[] iids, int[] states, long storeSessionID,
        boolean checkMsgExist) throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getMessageDAO().insert(null, dst, message, iids,
                        states, storeSessionID, message.getTimestamp(), checkMsgExist);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
     * @param iids	an array of interest ids whose states are to be
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
	DestinationUID to, ConsumerUID[] iids, int[] states, boolean sync)
	throws IOException, BrokerException {

        if (message == null || from == null || to == null) {
            throw new NullPointerException();
        }

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.moveMessage() called for message: "
                + message.getSysMessageID().getUniqueName() + " from "
                + from + " to " + to);
	}

	// make sure store is not closed then increment in progress count
	super.checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getMessageDAO().moveMessage(
                        null, message, from, to, iids, states);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    super.setInProgress(false);
	}
    }

    /**
     * Remove the message from the persistent store.
     * If the message has an interest list, the interest list will be
     * removed as well.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if the message is not found in the store
     */
    public void removeMessage(DestinationUID dID, SysMessageID mID, boolean sync, boolean onRollback)
	throws BrokerException {

        if (mID == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.removeMessage() called with message: " +
		mID.getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getMessageDAO().delete(null, dID, mID);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove all messages associated with the specified destination
     * from the persistent store.
     *
     * @param dst	the destination whose messages are to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the messages
     * @exception BrokerException if the destination is not found in the store
     * @exception NullPointerException	if <code>destination</code> is
     *			<code>null</code>
     */
    public void removeAllMessages(Destination dst, boolean sync)
	throws IOException, BrokerException {

        if (dst == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.removeAllMessages() for destination: " +
                dst.getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getMessageDAO().deleteByDestination(
                        null, dst.getDestinationUID());
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
        return messageEnumeration(dst, dbmgr.getBrokerID());
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
     * @param dst the destination
     * @param brokerID the broker ID
     * @return an enumeration of all persisted messages, an empty
     *		enumeration will be returned if no messages exist for the
     *		destionation
     * @exception BrokerException if an error occurs while getting the data
     */
    public Enumeration messageEnumeration(Destination dst, String brokerID)
	throws BrokerException {

        if (dst == null) {
            throw new NullPointerException();
        }

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.messageEnumeration() called for destination: " +
                dst.getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

    if (!msgEnumUseCursor) {

    try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getMessageDAO().messageEnumeration(
                        dst, brokerID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
    } finally {
        // decrement in progress count
        setInProgress(false);
    }

    } else {

    Enumeration en = null;
	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    en = daoFactory.getMessageDAO().
                             messageEnumerationCursor(dst, brokerID);
                    dataEnums.add(en);
                    return en;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} catch (Throwable e) {
        try {

        if (en != null) {
            dataEnums.remove(en);
            ((MessageEnumeration)en).close();
        }
        if (e instanceof BrokerException) throw (BrokerException)e;
        throw new BrokerException(e.toString(), e);

        } finally {
	    setInProgress(false);
        }
    }
    } 

    }

    public void closeEnumeration(Enumeration en) {
        if (!(en instanceof MessageEnumeration)) {
            return;
        }
        try {
            dataEnums.remove(en);
            ((MessageEnumeration)en).close();
        } finally { 
            setInProgress(false);
        }
    }

    /**
     * Return the number of persisted messages for the given broker.
     *
     * @return the number of persisted messages for the given broker
     * @exception BrokerException if an error occurs while getting the data
     */
    public int getMessageCount(String brokerID) throws BrokerException {

        if (brokerID == null) {
            throw new NullPointerException();
        }

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.getMessageCount() called for broker: " + brokerID);
	}
        // make sure store is not closed then increment in progress count
        super.checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getMessageDAO().getMessageCount(null, brokerID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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

        if (dst == null) {
            throw new NullPointerException();
        }

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "JDBCStore.getMessageStorageInfo() called for destination: " +
                dst.getUniqueName());
        }

        // make sure store is not closed then increment in progress count
        super.checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getMessageDAO().getMessageStorageInfo(null, dst);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    /**
     * Return the message with the specified message id.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the system message id of the message to be retrieved
     * @return a message
     * @exception BrokerException if the message is not found in the store
     *			or if an error occurs while getting the data
     */
    public Packet getMessage(DestinationUID dID, SysMessageID mID)
	throws BrokerException {

        if (mID == null) {
            throw new NullPointerException();
        }

        return getMessage(dID, mID.toString());
    }

    /**
     * Return the message with the specified message id.
     *
     * @param dID	the destination the message is associated with
     * @param mID	the message id of the message to be retrieved
     * @return a message
     * @exception BrokerException if the message is not found in the store
     *			or if an error occurs while getting the data
     */
    public Packet getMessage(DestinationUID dID, String mID)
	throws BrokerException {

        if (mID == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG, "JDBCStore.getMessage() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getMessageDAO().getMessage(null, dID, mID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
     * @param dID	the destination the message is associated with
     * @param mID	system message id of the message that the interest
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
    public void storeInterestStates(DestinationUID dID,
	SysMessageID mID, ConsumerUID[] iids, int[] states, boolean sync, Packet msg)
	throws BrokerException {

        if (mID == null || iids == null || states == null) {
            throw new NullPointerException();
        }

        if (iids.length == 0 || iids.length != states.length) {
            throw new BrokerException(br.getKString(BrokerResources.E_BAD_INTEREST_LIST));
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.storeInterestStates() called with message: " +
	        mID.getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getConsumerStateDAO().insert(
                        null, dID.toString(), mID, iids, states, true);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Update the state of the interest associated with the specified
     * message.  If the message does not have an interest list associated
     * with it, the interest list will be created. If the interest is not
     * in the interest list, it will be added to the interest list.
     *
     * @param dID	the destination the message is associated with
     * @param mID	system message id of the message that the interest
     *			is associated with
     * @param iID	id of the interest whose state is to be updated
     * @param state	state of the interest
     * @param sync	if true, will synchronize data to disk
     * @param txid  current transaction id. Unused by jdbc store
     * @param isLastAck Unused by jdbc store
     * @exception BrokerException if the message is not in the store
     */
    public void updateInterestState(DestinationUID dID,
	SysMessageID mID, ConsumerUID iID, int state, boolean sync, TransactionUID txid, boolean isLastAck)
    	throws BrokerException {

        if (mID == null || iID == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.updateInterestState() called with message: " +
		mID.getUniqueName() + ", consumer: " + iID.getUniqueName() +
                ", state=" + state);
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getConsumerStateDAO().updateState(null, dID, mID, iID, state);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Update the state of the interest associated with the specified
     * message.  If the message does not have an interest list associated
     * with it, the interest list will be created. If the interest is not
     * in the interest list, it will be added to the interest list.
     *
     * @param dID	the destination the message is associated with
     * @param mID	system message id of the message that the interest
     *			is associated with
     * @param iID	id of the interest whose state is to be updated
     * @param newState	state of the interest
     * @param expectedState the expected state
     * @exception BrokerException if the message is not in the store
     */
    public void updateInterestState(DestinationUID dID,
	SysMessageID mID, ConsumerUID iID, int newState, int expectedState)
	throws BrokerException {

        if (mID == null || iID == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.updateInterestState() called with message: " +
		mID.getUniqueName() + ", consumer: " + iID.getUniqueName() +
                ", state=" + newState + ", expected: " + expectedState);
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getConsumerStateDAO().updateState(
                        null, dID, mID, iID, newState, expectedState);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Get the state of the interest associated with the specified message.
     *
     * @param dID	the destination the message is associated with
     * @param mID	system message id of the message that the interest
     *			is associated with
     * @param iID	id of the interest whose state is to be returned
     * @return state of the interest
     * @exception BrokerException if the specified interest is not
     *		associated with the message; or if the message is not in the
     *		store
     */
    public int getInterestState(
	DestinationUID dID, SysMessageID mID, ConsumerUID iID)
    	throws BrokerException {

        if (mID == null || dID == null || iID == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.getInterestState() called with message: " +
                mID.getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

        Connection conn = null;
        Exception myex = null;
	try {
            conn = dbmgr.getConnection(true);

            Util.RetryStrategy retry = null;
            do {
                try {
                    // check existence of message
                    daoFactory.getMessageDAO().checkMessage(
                        conn, dID.toString(), mID.getUniqueName());
                    return daoFactory.getConsumerStateDAO().getState(conn, mID, iID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
    } catch (BrokerException e) {
            myex = e;
            throw e;
	} finally {
            try {
                Util.close(null, null, conn, myex);
            } finally {
                // decrement in progress count
                setInProgress(false);
            }
	}
    }

    public HashMap getInterestStates(DestinationUID dID, SysMessageID mID)
    	throws BrokerException {

        if (mID == null || dID == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.getInterestStates() called with message: " +
                mID.getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

        Connection conn = null;
        Exception myex = null;
	try {
            conn = dbmgr.getConnection(true);

            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getConsumerStateDAO().getStates(conn, mID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
    } catch (BrokerException e) {
            myex = e;
            throw e;
	} finally {
            try {
                Util.close(null, null, conn, myex);
            } finally {
                // decrement in progress count
                setInProgress(false);
            }
	}
    }

    /**
     * Retrieve all interest IDs associated with the specified message.
     * Note that the state of the interests returned is either
     * INTEREST_STATE_ROUTED or INTEREST_STATE_DELIVERED, i.e., interest
     * whose state is INTEREST_STATE_ACKNOWLEDGED will not be returned in the
     * array.
     *
     * @param dID	the destination the message is associated with
     * @param mID	system message id of the message whose interests
     *			are to be returned
     * @return an array of ConsumerUID objects associated with the message; a
     *		zero length array will be returned if no interest is
     *		associated with the message
     * @exception BrokerException if the message is not in the store
     */
    public ConsumerUID[] getConsumerUIDs(DestinationUID dID, SysMessageID mID)
	throws BrokerException {

        if (mID == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.getConsumerUIDs() called with message: " +
                mID.getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

        Connection conn = null;
        Exception myex = null;
	try {
            conn = dbmgr.getConnection(true);

            Util.RetryStrategy retry = null;
            do {
                try {
                    // check existence of message
                    daoFactory.getMessageDAO().checkMessage(
                        conn, dID.toString(), mID.getUniqueName());
                    return (ConsumerUID[])
                        daoFactory.getConsumerStateDAO().getConsumerUIDs(conn, mID).toArray(
                            new ConsumerUID[0]);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
    } catch (BrokerException e) {
            myex = e;
            throw e;
	} finally {
            try {
                Util.close( null, null, conn, myex );
            } finally {
                // decrement in progress count
                setInProgress(false);
            }
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

        if (interest == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.storeInterest() called with interest: " +
		interest.getConsumerUID().getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getConsumerDAO().insert(null, interest,
                        System.currentTimeMillis());
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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

        if (interest == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.removeInterest() called with interest: " +
		interest.getConsumerUID().getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getConsumerDAO().delete(null, interest);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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

	if (DEBUG) {
	    logger.log(Logger.DEBUG, "JDBCStore.getAllInterests() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return (Consumer[])
                        daoFactory.getConsumerDAO().getAllConsumers(null).toArray(
                            new Consumer[0]);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
     * @exception BrokerException if the same destination exists
     *			the store already
     * @exception NullPointerException	if <code>destination</code> is
     *			<code>null</code>
     */
    public void storeDestination(Destination destination, boolean sync)
	throws BrokerException {

        storeDestination(destination, getStoreSession());
    }

    protected void storeDestination(Destination destination, long storeSessionID)
        throws BrokerException {

        if (destination == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.storeDestination() called with destination: " +
                destination.getUniqueName());
	}

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getDestinationDAO().insert(null, destination,
                        storeSessionID, 0, System.currentTimeMillis());
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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

        if (destination == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.updateDestination() called with destination: " +
                destination.getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getDestinationDAO().update(null, destination);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove the destination from the persistent store.
     *
     * @param destination	the destination to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the destination
     * @exception BrokerException if the destination is not found in the store
     */
    public void removeDestination(Destination destination, boolean sync)
	throws IOException, BrokerException {

        if (destination == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.removeDestination() called with destination: " +
                destination.getUniqueName());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    boolean isDeleted =
                        daoFactory.getDestinationDAO().delete(null, destination);
                    if (destination.isAutoCreated() && Globals.getHAEnabled()) {
                        DestinationUID dstID = destination.getDestinationUID();
                        synchronized(pendingDeleteDsts) {
                            if (isDeleted) {
                                // Remove from pending list if exists
                                pendingDeleteDsts.remove(dstID);
                            } else {
                                // Save to pending list to reap at a later time
                                pendingDeleteDsts.put(dstID,
                                    Integer.valueOf(destination.getType()));
                            }
                        }
                    }
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Reap auto-created destinations.
     *
     * Only applicable for broker running in HA mode and will should be invoked
     * when a broker is removed from the cluster.
     *
     * @exception BrokerException
     */
    public void reapAutoCreatedDestinations()
	throws BrokerException {

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.reapAutoCreatedDestinations() called" );
	}

        if (!Globals.getHAEnabled()) {
            return; // No-op
        }

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    synchronized (pendingDeleteDsts) {
                        if (!pendingDeleteDsts.isEmpty()) {
                            // Iterate over the auto-created dst pending removal
                            // list and see if any can be removed from the DB
                            DestinationDAO dao = daoFactory.getDestinationDAO();
                            Iterator itr = pendingDeleteDsts.entrySet().iterator();
                            while (itr.hasNext()) {
                                Map.Entry e = (Map.Entry)itr.next();
                                DestinationUID dst = (DestinationUID)e.getKey();
                                int type = ((Integer)e.getValue()).intValue();
                                if (dao.delete(null, dst, type)) {
                                    logger.log(Logger.DEBUG,
                                        "Auto-created destination " + dst +
                                        " has been removed from HA Store");

                                    // Remove from pending list if it is reaped
                                    itr.remove();
                                }
                            }
                        }
                    }
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
    public Destination getDestination(DestinationUID id) throws BrokerException {

        if (id == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.getDestination() called with destination ID: " +
                id.toString());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getDestinationDAO().getDestination(null, id.toString());
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
        return getAllDestinations(dbmgr.getBrokerID());
    }

    /**
     * Retrieve all destinations in the store for the specified broker ID.
     *
     * @param brokerID the broker ID
     * @return an array of Destination objects; a zero length array is
     * returned if no destinations exist in the store
     * @exception IOException if an error occurs while getting the data
     */
    public Destination[] getAllDestinations(String brokerID)
	throws IOException, BrokerException {

	if (DEBUG) {
	    logger.log(Logger.DEBUG, "JDBCStore.getAllDestinations() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return (Destination[])
                        daoFactory.getDestinationDAO().getAllDestinations(
                            null, brokerID).toArray(new Destination[0]);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
     * @exception BrokerException if the same transaction id exists
     *			the store already
     * @exception NullPointerException	if <code>id</code> is
     *			<code>null</code>
     */
    public void storeTransaction(TransactionUID id, TransactionState ts,
        boolean sync) throws BrokerException {

        if (id == null || ts == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.storeTransaction() called with txn: " + id.longValue());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTransactionDAO().insert(
                        null, id, ts, getStoreSession());
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove the transaction from the persistent store.
     *
     * @param id	the id of the transaction to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the txn
     * @exception BrokerException if the transaction is not found
     *			in the store
     */
    public void removeTransaction(TransactionUID id, boolean sync)
        throws IOException, BrokerException {

        removeTransaction( id, false, sync );
    }

    /**
     * Remove the transaction from the persistent store.
     *
     * @param id	the id of the transaction to be removed
     * @param removeAcks if true, will remove all associated acknowledgements
     * @param sync	if true, will synchronize data to disk
     * @exception IOException if an error occurs while removing the txn
     * @exception BrokerException if the transaction is not found
     *			in the store
     */
    public void removeTransaction(TransactionUID id, boolean removeAcks,
        boolean sync) throws IOException, BrokerException {

        if (id == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.removeTransaction() called with id=" +
                id.longValue() + ", removeAcks=" + removeAcks);
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

        Connection conn = null;
        Exception myex = null;
	try {
            conn = dbmgr.getConnection(false);

            Util.RetryStrategy retry = null;
            do {
                try {
                    // First, remove the acks
                    if (removeAcks) {
                        daoFactory.getConsumerStateDAO().clearTransaction(conn, id);
                    }

                    // Now, remove the txn
                    daoFactory.getTransactionDAO().delete(conn, id);

                    conn.commit();

                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
    } catch (BrokerException e) {
            myex = e;
            throw e;
	} finally {
            try {
                Util.close(null, null, conn, myex);
            } finally {
                // decrement in progress count
                setInProgress(false);
            }
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

        if (id == null || ts == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
            logger.log(Logger.DEBUG, "JDBCStore.updateTransactionState called with id=" +
                id.longValue() + ", ts=" + ts.getState());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTransactionDAO().updateTransactionState(null, id, ts);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Retrieve all transaction ids in the store with their state
     *
     * @return a HashMap. The key is a TransactionUID.
     * The value is a TransactionState.
     * @exception IOException if an error occurs while getting the data
     */
    public HashMap getAllTransactionStates()
	throws IOException, BrokerException {

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.getAllTransactionStates() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTransactionDAO().getTransactionStatesByBroker(
                        null, dbmgr.getBrokerID());
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    public HashMap getAllRemoteTransactionStates()
        throws IOException, BrokerException {

        if (DEBUG) {
            logger.log(Logger.DEBUG,
                "JDBCStore.getAllRemoteTransactionStates() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTransactionDAO().getRemoteTransactionStatesByBroker(
                        null, dbmgr.getBrokerID());
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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

        if (tid == null || ack == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.storeTransactionAck() called with txn: " +
                tid.longValue());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getConsumerStateDAO().updateTransaction(null,
                        ack.getSysMessageID(), ack.getStoredConsumerUID(), tid);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Remove all acknowledgements associated with the specified
     * transaction from the persistent store.
     *
     * From 4.0 onward, acknowledgement is implemented as a TRANSACTION_ID
     * column on the message and consumer state table so to remove the acks
     * is the same as clear txnID from the column.
     *
     * @param tid	the transaction id whose acknowledgements are
     *			to be removed
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if error occurs while removing the
     *			acknowledgements
     */
    public void removeTransactionAck(TransactionUID tid, boolean sync)
	throws BrokerException {

        if (tid == null) {
            throw new NullPointerException();
        }

        if (DEBUG) {
            if (DEBUG) {
                logger.log(Logger.DEBUG,
                    "JDBCStore.removeTransactionAck() called with txn: " +
                    tid.longValue());
            }
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getConsumerStateDAO().clearTransaction(null, tid);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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

	if (DEBUG) {
	    logger.log(Logger.DEBUG, "JDBCStore.getAllTransactionAcks() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getConsumerStateDAO().getAllTransactionAcks(null);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Retrieve all acknowledgements for the specified transaction.
     *
     * @param id	id of the transaction whose acknowledgements
     *			are to be returned
     * @exception BrokerException if the transaction id is not in the store
     */
    public TransactionAcknowledgement[] getTransactionAcks(
	TransactionUID id) throws BrokerException {

        if (id == null) {
            throw new NullPointerException();
        }

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.getTransactionAcks() called with txn: " +
                id.longValue());
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return (TransactionAcknowledgement[])
                        daoFactory.getConsumerStateDAO().getTransactionAcks(null, id).toArray(
                            new TransactionAcknowledgement[0]);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    public void storeTransaction(TransactionUID tid, TransactionInfo txnInfo,
        boolean sync) throws BrokerException {

        storeTransaction(tid, txnInfo, getStoreSession());
    }

    protected void storeTransaction(TransactionUID tid, TransactionInfo txnInfo,
        long storeSessionID) throws BrokerException {

        if (tid == null || txnInfo == null) {
            throw new NullPointerException();
        }

        int type = TransactionInfo.TXN_NOFLAG;
        BrokerAddress txnHomeBroker = null;
        TransactionBroker[] txnBrokers = null;
        if (txnInfo.isCluster()) {
            type = TransactionInfo.TXN_CLUSTER;
            txnBrokers = txnInfo.getTransactionBrokers();
        } else if (txnInfo.isRemote()) {
            type = TransactionInfo.TXN_REMOTE;
            txnHomeBroker = txnInfo.getTransactionHomeBroker();
        } else if (txnInfo.isLocal()) {
            type = TransactionInfo.TXN_LOCAL;
        } else {
            String errorMsg = "Illegal transaction type: " + txnInfo.getType();
            logger.log( Logger.ERROR, BrokerResources.E_INTERNAL_BROKER_ERROR,
                errorMsg );
            throw new BrokerException(
                br.getKString( BrokerResources.E_INTERNAL_BROKER_ERROR, errorMsg ) );
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTransactionDAO().insert(null, tid,
                        txnInfo.getTransactionState(), txnHomeBroker,
                        txnBrokers, type, storeSessionID);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void storeClusterTransaction(TransactionUID tid, TransactionState ts,
        TransactionBroker[] txnBrokers, boolean sync) throws BrokerException {

        if (tid == null || ts == null) {
            throw new NullPointerException();
        }

        if (DEBUG) {
            logger.log(Logger.DEBUG,
                "JDBCStore.storeClusterTransaction() called with txn: " +
                tid.longValue());
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTransactionDAO().insert(null, tid, ts, null,
                        txnBrokers, TransactionInfo.TXN_CLUSTER, getStoreSession());
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void updateClusterTransaction(TransactionUID tid,
        TransactionBroker[] txnBrokers, boolean sync) throws BrokerException {

        if (tid == null) {
            throw new NullPointerException();
        }

        if (DEBUG) {
            logger.log(Logger.DEBUG,
                "JDBCStore.updateClusterTransaction() called with txn: " +
                tid.longValue());
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTransactionDAO().updateTransactionBrokers(
                        null, tid, txnBrokers);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void updateClusterTransactionBrokerState(TransactionUID tid,
        int expectedTxnState, TransactionBroker txnBroker, boolean sync)
        throws BrokerException {

        if (tid == null) {
            throw new NullPointerException();
        }

        if (DEBUG) {
            logger.log(Logger.DEBUG,
                "JDBCStore.updateClusterTransactionBrokerState() called with txn: " +
                tid.longValue());
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTransactionDAO().updateTransactionBrokerState(
                        null, tid, expectedTxnState, txnBroker);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void updateRemoteTransaction(TransactionUID tid,
        TransactionAcknowledgement[] txnAcks,
        BrokerAddress txnHomeBroker, boolean sync) throws BrokerException {

        if (tid == null) {
            throw new NullPointerException();
        }

        if (!Globals.getHAEnabled()) {
            throw new UnsupportedOperationException(
                "Operation not supported by the " + getStoreType() +
                " store in non-HA mode" );
        }

        if (DEBUG) {
            logger.log(Logger.DEBUG,
                "JDBCStore.updateRemoteTransaction() called with txn: " +
                tid.longValue());
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        Connection conn = null;
        Exception myex = null;
        try {
            conn = dbmgr.getConnection(false);
            Util.RetryStrategy retry = null;
            do {
                try {
                    //store the acks if any
                    if (txnAcks != null && txnAcks.length > 0) {
                        for (int i = 0, len = txnAcks.length; i < len; i++) {
                            TransactionAcknowledgement ack = txnAcks[i];
                            if (ack.shouldStore()) {
                            daoFactory.getConsumerStateDAO().updateTransaction(
                                conn, ack.getSysMessageID(),
                                ack.getStoredConsumerUID(),
                                tid);
                            }
                        }
                    }

                    // In HA mode, the txn is owned by another broker
                    // so we'll only update the txn home broker
                    daoFactory.getTransactionDAO().updateTransactionHomeBroker(
                        conn, tid, txnHomeBroker);
                    conn.commit();
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } catch (BrokerException e) {
            myex = e;
            throw e;
        } finally {
            try {
                Util.close(null, null, conn, myex);
            } finally {
                // decrement in progress count
                setInProgress(false);
            }
        }
    }

    public void storeRemoteTransaction(TransactionUID tid, TransactionState ts,
        TransactionAcknowledgement[] txnAcks, BrokerAddress txnHomeBroker,
        boolean sync) throws BrokerException {

        if (tid == null) {
            throw new NullPointerException();
        }

        if (Globals.getHAEnabled()) {
            throw new UnsupportedOperationException(
                "Operation not supported by the " + getStoreType() +
                " store in HA mode" );
        }

        if (DEBUG) {
            logger.log(Logger.DEBUG,
                "JDBCStore.storeRemoteTransaction() called with txn: " +
                tid.longValue());
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        Connection conn = null;
        Exception myex = null;
        try {
            conn = dbmgr.getConnection(false);

            Util.RetryStrategy retry = null;
            do {
                try {
                    // First, store the txn
                    daoFactory.getTransactionDAO().insert(
                        conn, tid, ts, txnHomeBroker, null,
                        TransactionInfo.TXN_REMOTE, getStoreSession());

                    // Now, store the acks if any
                    if (txnAcks != null && txnAcks.length > 0) {
                        for (int i = 0, len = txnAcks.length; i < len; i++) {
                            TransactionAcknowledgement ack = txnAcks[i];
                            if (ack.shouldStore()) {
                            daoFactory.getConsumerStateDAO().updateTransaction(
                                conn, ack.getSysMessageID(),
                                ack.getStoredConsumerUID(),
                                tid);
                            }
                        }
                    }

                    conn.commit();

                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } catch (BrokerException e) {
            myex = e;
            throw e;
        } finally {
            try {
                Util.close(null, null, conn, myex);
            } finally {
                // decrement in progress count
                setInProgress(false);
            }
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
     * @param sync if true, will synchronize data to disk
     * @exception BrokerException if an error occurs while persisting the data
     * @exception NullPointerException if <code>name</code> is
     *			<code>null</code>
     */
    public void updateProperty(String name, Object value, boolean sync)
	throws BrokerException {

        if (name == null) {
            throw new NullPointerException();
        }

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.updateProperty() called with name: " + name);
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getPropertyDAO().update(null, name, value);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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

        if (name == null) {
            throw new NullPointerException();
        }

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.getProperty() called with name: " + name);
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getPropertyDAO().getProperty( null, name);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
	    logger.log(Logger.DEBUG, "JDBCStore.getPropertyNames() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return (String[])
                        daoFactory.getPropertyDAO().getPropertyNames(null).toArray(
                            new String[0]);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
            logger.log(Logger.DEBUG, "JDBCStore.getAllProperties() called");
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getPropertyDAO().getProperties(null);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
     * @param createdTime The time when this record was created.
     * @param recordData The record data.
     * @param sync if true, will synchronize data to disk
     * @exception BrokerException if an error occurs while persisting
     *			the data or if the timestamp is less than 0
     * @exception NullPointerException if <code>recordData</code> is
     *			<code>null</code>
     */
    public void storeConfigChangeRecord(long createdTime, byte[] recordData,
        boolean sync) throws BrokerException {

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.storeConfigChangeRecord() called");
	}

        if (createdTime <= 0) {
            String ts = String.valueOf(createdTime);
            logger.log(Logger.ERROR, BrokerResources.E_INVALID_TIMESTAMP, ts);
            throw new BrokerException(
                br.getKString(BrokerResources.E_INVALID_TIMESTAMP, ts));
        }

        if (recordData == null) {
            throw new NullPointerException();
        }

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getConfigRecordDAO().insert(null, recordData, createdTime);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
    public ArrayList<ChangeRecordInfo> getConfigChangeRecordsSince(long timeStamp)
	throws BrokerException {

	if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.getConfigChangeRecordsSince() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return (ArrayList)daoFactory.getConfigRecordDAO().getRecordsSince(
                        null, timeStamp);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
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
                "JDBCStore.getAllConfigRecords() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getConfigRecordDAO().getAllRecords(null);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    /**
     * Clear all config change records in the store.
     *
     * @param sync if true, will synchronize data to disk
     * @exception BrokerException if an error occurs while clearing the data
     */
    public void clearAllConfigChangeRecords(boolean sync)
	throws BrokerException {

        if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCStore.clearAllConfigChangeRecords() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

	try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getConfigRecordDAO().deleteAll(null);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	} finally {
	    // decrement in progress count
	    setInProgress(false);
	}
    }

    public void clearAll(boolean sync) throws BrokerException {

        if (DEBUG) {
	    logger.log(Logger.DEBUG, "JDBCStore.clearAll() called");
	}

	// make sure store is not closed then increment in progress count
	checkClosedAndSetInProgress();

        Connection conn = null;
        Exception myex = null;
	try {
            conn = dbmgr.getConnection(false);

            Util.RetryStrategy retry = null;
            do {
                try {
                    if (Globals.getHAEnabled()) {
                        // In HA mode, only reset txns, dsts, states, and msgs
                        // in the specified order
                        daoFactory.getTransactionDAO().deleteAll(conn);
                        daoFactory.getDestinationDAO().deleteAll(conn);
                        daoFactory.getConsumerStateDAO().deleteAll(conn);
                        daoFactory.getMessageDAO().deleteAll(conn);
                        daoFactory.getTMLogRecordDAOJMSBG().deleteAll(conn);
                        daoFactory.getJMSBGDAO().deleteAll(conn);
                    } else {
                        List daos = daoFactory.getAllDAOs();
                        Iterator itr = daos.iterator();
                        while (itr.hasNext()) {
                            BaseDAO dao = (BaseDAO)itr.next();
                            if ( !(dao instanceof VersionDAO ||
                                   dao instanceof BrokerDAO ||
                                   dao instanceof StoreSessionDAO) ) {
                                dao.deleteAll(conn);
                            }
                        }
                    }
                    conn.commit();
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } catch ( Exception e ) {
            myex = e;
            throw new BrokerException(
                br.getKString( BrokerResources.X_CLEAR_ALL_FAILED ), e );
	} finally {
            try {
                Util.close(null, null, conn, myex);
            } finally {
                // decrement in progress count
                setInProgress(false);
            }
	}
    }

    public void close(boolean cleanup) {

	// make sure all operations are done before we proceed to close
	setClosedAndWait();

	// true = unlock the tables
	closeDB(true);

    dbmgr.setStoreInited(false);

	if (DEBUG) {
	    logger.log(Logger.DEBUG, "JDBCStore.close("+ cleanup +") done.");
	}
    }

    protected void beforeWaitOnClose() {

        Iterator<Enumeration> itr = null;
        synchronized(dataEnums) {
            itr = dataEnums.iterator();
            Enumeration en = null;
            while (itr.hasNext()) {
                en = itr.next();
                if (en instanceof MessageEnumeration) {
                    ((MessageEnumeration)en).cancel();
                }
            }
        }
        if (dbmgr != null) {
            dbmgr.setIsClosing();
        }
 
    }

    // HA operations

    public long getBrokerHeartbeat(String brokerID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().getHeartbeat(null, brokerID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public HashMap getAllBrokerHeartbeats() throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().getAllHeartbeats(null);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public Long updateBrokerHeartbeat(String brokerID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().updateHeartbeat(null, brokerID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        // Override default so total retry time is 30 secs
                        retry = new Util.RetryStrategy(dbmgr,
                            DBManager.TRANSACTION_RETRY_DELAY_DEFAULT, 4);
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public Long updateBrokerHeartbeat(String brokerID, long lastHeartbeat)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().updateHeartbeat(null,
                        brokerID, lastHeartbeat);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        // Override default so total retry time is 30 secs
                        retry = new Util.RetryStrategy(dbmgr,
                            DBManager.TRANSACTION_RETRY_DELAY_DEFAULT, 4);
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void addBrokerInfo(String brokerID, String URL, BrokerState state,
        int version, long sessionID, long heartbeat) throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getBrokerDAO().insert(null, brokerID, null, URL,
                        version, state.intValue(), sessionID, heartbeat);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }


    public void addBrokerInfo(HABrokerInfo bkrInfo, boolean sync) throws BrokerException {
        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getBrokerDAO().insert(null, bkrInfo.getId(),
                        bkrInfo.getTakeoverBrokerID(), bkrInfo.getUrl(),
                        bkrInfo.getVersion(), bkrInfo.getState(),
                        -1, bkrInfo.getHeartbeat());

                    List sessions = bkrInfo.getAllSessions();
                    if (sessions != null && !sessions.isEmpty()) {
                        StoreSessionDAO dao = daoFactory.getStoreSessionDAO();
                        Iterator itr = sessions.iterator();
                        while (itr.hasNext()) {
                            HABrokerInfo.StoreSession ses =
                                (HABrokerInfo.StoreSession)itr.next();
                            dao.insert(null, ses.getBrokerID(), ses.getID(),
                                ses.getIsCurrent(), ses.getCreatedBy(),
                                ses.getCreatedTS());
                        }
                    }
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }


    public UID updateBrokerInfo( String brokerID, int updateType,
                                 Object oldValue, Object newValue )
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().update(null, brokerID, 
                                                     updateType, oldValue, newValue);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public HABrokerInfo getBrokerInfo(String brokerID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().getBrokerInfo(null, brokerID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public HashMap getAllBrokerInfos(boolean loadSession)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().getAllBrokerInfos(null, loadSession);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public HashMap getAllBrokerInfoByState(BrokerState state)
        throws BrokerException {

        if (state == null) {
            throw new NullPointerException();
        }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().getAllBrokerInfosByState(null, state);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public String getStoreSessionOwner( long sessionID )
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getStoreSessionDAO().getStoreSessionOwner(null, sessionID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public boolean ifOwnStoreSession( long sessionID, String brokerID )
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getStoreSessionDAO().ifOwnStoreSession(null, sessionID, brokerID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public String getStoreSessionCreator( long sessionID )
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getStoreSessionDAO().getStoreSessionCreator(null, sessionID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public boolean updateBrokerState(String brokerID, BrokerState newState,
        BrokerState expectedState, boolean local) throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().updateState( null, brokerID,
                        newState, expectedState, local );
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        // Override default so total retry time is about 1 min
                        retry = new Util.RetryStrategy(dbmgr,
                            DBManager.TRANSACTION_RETRY_DELAY_DEFAULT, 5);
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public BrokerState getBrokerState(String brokerID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().getState(null, brokerID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public Object[] getAllBrokerStates() throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getBrokerDAO().getAllStates(null);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void getTakeOverLock(String brokerID, String targetBrokerID,
        long lastHeartbeat, BrokerState expectedState,
        long newHeartbeat, BrokerState newState, boolean force,
        TakingoverTracker tracker)
        throws TakeoverLockException, BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        tracker.setStage_BEFORE_GET_LOCK();

        Connection conn = null;
        Exception myex = null;
        try {
            synchronized ( takeoverLockMap ) {
                // Verify if a lock has been acquired for the target broker
                TakeoverStoreInfo takeoverInfo =
                    (TakeoverStoreInfo)takeoverLockMap.get( targetBrokerID );
                if (takeoverInfo != null) {
                    logger.logToAll( Logger.WARNING,
                        BrokerResources.W_UNABLE_TO_ACQUIRE_TAKEOVER_LOCK, targetBrokerID );
                    return;
                }

                conn = dbmgr.getConnection( true );

                Util.RetryStrategy retry = null;
                do {
                    try {
                        // Try to obtain the lock by updating the target broker
                        // entry in the broker table; an exception is thrown if we
                        // are unable to get the lock.
                        HABrokerInfo savedInfo =
                            daoFactory.getBrokerDAO().takeover(
                                conn, brokerID, targetBrokerID, lastHeartbeat,
                                expectedState, newHeartbeat, newState );
                        savedInfo.setTakeoverTimestamp(newHeartbeat);

                        tracker.setStage_AFTER_GET_LOCK();

                        long timestamp = System.currentTimeMillis();

                        logger.logToAll( Logger.INFO,
                            BrokerResources.I_TAKEOVER_LOCK_ACQUIRED,
                            targetBrokerID, String.valueOf(timestamp) );

                        takeoverInfo = new TakeoverStoreInfo(
                            targetBrokerID, savedInfo, timestamp );

                        // Save the broker's state
                        takeoverLockMap.put( targetBrokerID, takeoverInfo );

                        // Now, get the all the msgs and corresponding dst IDs
                        // that we'll be taking over to track and ensure that
                        // they can't be accessed while we're taking over!
                        Map msgMap =
                            daoFactory.getMessageDAO().getMsgIDsAndDstIDsByBroker(
                                conn, targetBrokerID );

                        tracker.setMessageMap( msgMap );

                        return;
                    } catch ( Exception e ) {
                        // Exception will be log & re-throw if operation cannot be retry
                        if ( retry == null ) {
                            retry = new Util.RetryStrategy();
                        }
                        retry.assertShouldRetry( e );
                    }
                } while ( true );
            }
        } catch (BrokerException e) {
            myex = e;
            throw e;
        } finally {
            try {
                Util.close( null, null, conn, myex );
            } finally  {
                // decrement in progress count
                setInProgress(false);
            }
        }
    }

    public TakeoverStoreInfo takeOverBrokerStore(String brokerID,
        String targetBrokerID, TakingoverTracker tracker) 
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        tracker.setStage_BEFORE_TAKE_STORE();

        Connection conn = null;
        Exception myex = null;
        try {
            conn = dbmgr.getConnection( false );

            BrokerDAO brokerDAO = daoFactory.getBrokerDAO();
            HABrokerInfo bkrInfo = null;

            Util.RetryStrategy retry = null;
            do {    // JDBC Retry loop
                try {
                    bkrInfo = brokerDAO.getBrokerInfo( conn, targetBrokerID );
                    break;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );

            if ( bkrInfo == null ) {
                String errorMsg = br.getKString(
                    BrokerResources.E_BROKERINFO_NOT_FOUND_IN_STORE, targetBrokerID );
                logger.log( Logger.ERROR, BrokerResources.E_INTERNAL_BROKER_ERROR,
                    errorMsg );
                throw new BrokerException(
                    br.getKString( BrokerResources.E_INTERNAL_BROKER_ERROR, errorMsg ) );
            }

            // Verify a takeover lock has been acquired for the target broker.
            TakeoverStoreInfo takeoverInfo = null;
            synchronized ( takeoverLockMap ) {
                takeoverInfo = (TakeoverStoreInfo)takeoverLockMap.get( targetBrokerID );
            }

            if ( takeoverInfo == null ||
                 !brokerID.equals( bkrInfo.getTakeoverBrokerID() ) ) {
                // Cannot takeover a store without 1st obtaining the lock
                logger.log( Logger.ERROR, BrokerResources.E_TAKEOVER_WITHOUT_LOCK,
                    targetBrokerID );
                throw new BrokerException(
                    br.getKString( BrokerResources.E_TAKEOVER_WITHOUT_LOCK, targetBrokerID ) );
            }
            tracker.setStoreSession(bkrInfo.getSessionID());

            // Start takeover process...

            try {
                retry = null;
                do {    // JDBC Retry loop
                    try {
                        // Get local destinations of target broker
                        DestinationDAO dstDAO = daoFactory.getDestinationDAO();
                        List dstList = dstDAO.getAllLocalDestinations( conn, targetBrokerID );
                        takeoverInfo.setDestinationList( dstList );
                        String args[] = { String.valueOf(dstList.size()),
                            targetBrokerID, dstList.toString() };
                        logger.log( Logger.INFO, br.getString(
                            BrokerResources.I_TAKINGOVER_LOCAL_DSTS, args) );

                        // Get messages of target broker
                        MessageDAO msgDAO = daoFactory.getMessageDAO();
                        Map msgMap = msgDAO.getMsgIDsAndDstIDsByBroker( conn, targetBrokerID );
                        takeoverInfo.setMessageMap( msgMap );
                        logger.log( Logger.INFO, br.getString(
                            BrokerResources.I_TAKINGOVER_MSGS,
                            msgMap.size(), targetBrokerID) );

                        // Get transactions of target broker
                        TransactionDAO txnDAO = daoFactory.getTransactionDAO();
                        List txnList = txnDAO.getTransactionsByBroker( conn, targetBrokerID );
                        takeoverInfo.setTransactionList( txnList );
                        logger.log( Logger.INFO, br.getString(
                            BrokerResources.I_TAKINGOVER_TXNS,
                            txnList.size(), targetBrokerID) );

                        // Get remote transactions of target broker
                        List remoteTxnList = txnDAO.getRemoteTransactionsByBroker( conn, targetBrokerID );
                        takeoverInfo.setRemoteTransactionList( remoteTxnList );
                        logger.log( Logger.INFO, br.getString(
                            BrokerResources.I_TAKINGOVER_REMOTE_TXNS,
                            remoteTxnList.size(), targetBrokerID) );

                        tracker.setStage_BEFORE_DB_SWITCH_OWNER();

                        // Takeover all store sessions of target broker
                        StoreSessionDAO sesDAO = daoFactory.getStoreSessionDAO();
                        List sesList = sesDAO.takeover( conn, brokerID, targetBrokerID );
                        String args2[] = { String.valueOf(sesList.size()),
                            targetBrokerID, sesList.toString() };
                        logger.log( Logger.INFO, br.getString(
                            BrokerResources.I_TAKINGOVER_STORE_SESSIONS, args2) );

                        if (!brokerDAO.updateState( conn, targetBrokerID, 
                                                    BrokerState.FAILOVER_COMPLETE,
                                                    BrokerState.FAILOVER_STARTED, false )) {
                            try {
                                conn.rollback();
                            } catch (SQLException rbe) {
                                logger.logStack( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
                            }
                            throw new BrokerException(
                            "Unable to update state to "+ BrokerState.FAILOVER_COMPLETE+
                            " for broker "+targetBrokerID );
                        }

                        conn.commit();

                        tracker.setStage_AFTER_DB_SWITCH_OWNER();

                        // Removed saved state from cache
                        synchronized( takeoverLockMap ) {
                            takeoverLockMap.remove( targetBrokerID );
                        }

                        tracker.setStage_AFTER_TAKE_STORE();

                        return takeoverInfo;
                    } catch ( Exception e ) {
                        // Exception will be log & re-throw if operation cannot be retry
                        if ( retry == null ) {
                            retry = new Util.RetryStrategy();
                        }
                        retry.assertShouldRetry( e );
                    }
                } while ( true );
            } catch ( Throwable thr ) {
                // We need to remove the takeover lock on the broker table
                // if we're unable to takeover the store due to an error.
                // We do not need to do a transaction rollback here because
                // the DAO layer should done this already.

                logger.logToAll( Logger.INFO,
                    BrokerResources.I_REMOVING_TAKEOVER_LOCK, targetBrokerID );

                HABrokerInfo savedInfo = takeoverInfo.getSavedBrokerInfo();
                try {
                    retry = null;
                    do {    // JDBC Retry loop
                        try {
                            // Restore original heartbeat
                            brokerDAO.update( conn, targetBrokerID,
                                              HABrokerInfo.RESTORE_HEARTBEAT_ON_TAKEOVER_FAIL,
                                              brokerID,
                                              savedInfo );

                            // Removed the lock, i.e. restore entry values
                            brokerDAO.update( conn, targetBrokerID, HABrokerInfo.RESTORE_ON_TAKEOVER_FAIL,
                                              brokerID, savedInfo );

                            logger.log(logger.INFO, br.getKString(br.I_BROKER_STATE_RESTORED_TAKEOVER_FAIL,
                                targetBrokerID, BrokerState.getState(savedInfo.getState()).toString()+
                                                            "[StoreSession:"+savedInfo.getSessionID()+"]"));

                            conn.commit();

                            synchronized( takeoverLockMap ) {
                                takeoverLockMap.remove( targetBrokerID );
                            }

                            break;  // // JDBC Retry loop
                        } catch ( Exception e ) {
                            // Exception will be log & re-throw if operation cannot be retry
                            if ( retry == null ) {
                                retry = new Util.RetryStrategy();
                            }
                            retry.assertShouldRetry( e );
                        }
                    } while ( true );
                } catch ( Exception e ) {
                    logger.logStack( Logger.ERROR,
                        BrokerResources.E_UNABLE_TO_REMOVE_TAKEOVER_LOCK,
                        targetBrokerID, e );
                }

                throw new BrokerException(
                    br.getKString( BrokerResources.E_UNABLE_TO_TAKEOVER_BROKER,
                    targetBrokerID), thr );
            }
        } catch (BrokerException e) {
            myex = e;
            throw e;
        } finally {
            try {
                Util.close( null, null, conn, myex );
            } finally {
                // decrement in progress count
                setInProgress(false);
            }
        }
    }

    public void updateTransactionAccessedTime(TransactionUID txnID,
        long accessedTime) throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTransactionDAO().updateAccessedTime(null,
                        txnID, accessedTime);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public TransactionState getTransactionState(TransactionUID txnID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTransactionDAO().getTransactionState(null, txnID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public BrokerAddress getRemoteTransactionHomeBroker(TransactionUID tid)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTransactionDAO().getTransactionHomeBroker(null, tid);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public TransactionBroker[] getClusterTransactionBrokers(TransactionUID tid)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTransactionDAO().getTransactionBrokers(null, tid);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public long getTransactionAccessedTime(TransactionUID txnID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTransactionDAO().getAccessedTime( null, txnID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public TransactionInfo getTransactionInfo(TransactionUID txnID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTransactionDAO().getTransactionInfo( null, txnID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public Collection getTransactions(String brokerID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTransactionDAO().getTransactionsByBroker(
                        null, brokerID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public int[] getTransactionUsageInfo(TransactionUID txnID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTransactionDAO().getTransactionUsageInfo(
                        null, txnID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public long getDestinationConnectedTime(Destination destination)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getDestinationDAO().getDestinationConnectedTime(
                        null, destination.getUniqueName());
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public boolean hasMessageBeenAcked(DestinationUID dst, SysMessageID mID)
        throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getMessageDAO().hasMessageBeenAcked(
                        null, dst, mID);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    public void updateDestinationConnectedTime(Destination destination,
        long connectedTime) throws BrokerException {

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getDestinationDAO().updateConnectedTime(
                        null, destination, connectedTime);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            setInProgress(false);
        }
    }

    // unlock indicates whether we need to unlock the tables before we close
    private void closeDB(boolean unlock) {

	Connection conn = null;
    Exception myex = null;
	try {
	    if (unlock) {
	    	conn = dbmgr.getConnection( true);

		// unlock the tables
                if ( !Globals.getHAEnabled() &&
                    config.getBooleanProperty( LOCK_STORE_PROP, true ) ) {
                    DBManager.lockTables( conn, false );
                }
	    }
	} catch (Exception e) {
        myex = e;
	    logger.log(Logger.WARNING, BrokerResources.X_CLOSE_DATABASE_FAILED, e);
	} finally {
            try {
                Util.close( null, null, conn, myex );
            } catch ( Exception e ) {}
	}

	dbmgr.close();
    }

    public String getStoreType() {
	return JDBC_STORE_TYPE;
    }

    /**
     * The following methods does not apply to jdbc store.
     * Data synchronization methods are implemented as no-ops
     * after the necessary checks. Others will throw BrokerException.
     */
    public HashMap getStorageInfo(Destination destination)
	throws BrokerException {
	throw new BrokerException(br.getKString(BrokerResources.E_NOT_JDBC_STORE_OPERATION));
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */
    public Hashtable getDebugState() throws BrokerException {

        String url = dbmgr.getOpenDBURL();
	String bid = "(" + dbmgr.getBrokerID() + ")";

	Hashtable t = new Hashtable();
        t.put("JDBC-based store", url + bid);
        t.put("Store version", String.valueOf(STORE_VERSION));

        Connection conn = null;
        Exception myex = null;
        try {
            conn = dbmgr.getConnection( true );

            Iterator itr = daoFactory.getAllDAOs().iterator();
            while ( itr.hasNext() ) {
                // Get debug info for each DAO
                t.putAll( ((BaseDAO)itr.next()).getDebugInfo( conn ) );
            }
        } catch (BrokerException e) {
            myex = e;
            throw e;
        } finally {
            Util.close( null, null, conn, myex );
        }
        t.put(dbmgr.toString(), dbmgr.getDebugState());

        return t;
    }

    public void compactDestination(Destination destination)
	throws BrokerException {
	throw new BrokerException(br.getKString(BrokerResources.E_NOT_JDBC_STORE_OPERATION));
    }

    /**
     * 1. if new store exists (store of current version exists)
     *      print reminder message if old version still exists
     *      check to see if store need to be remove or reset
     * 2. if new store NOT exists
     *	    check if old store exists
     * 3. if old store exists
     *      check to see if store need to be upgrade
     * 4. if old store NOT exists
     *      check if store need to be create
     */
    private boolean checkStore( Connection conn ) throws BrokerException {

        boolean status = true;  // Set to false for caller to close DB

        // Check old store
        int oldStoreVersion = -1;
        if ( checkOldStoreVersion( conn,
            dbmgr.getTableName( VersionDAO.TABLE + SCHEMA_VERSION_40 ),
            VersionDAO.STORE_VERSION_COLUMN, OLD_STORE_VERSION_400 ) ) {
            oldStoreVersion = OLD_STORE_VERSION_400;
        } else if ( checkOldStoreVersion( conn, VERSION_TBL_37 + dbmgr.getBrokerID(),
            TVERSION_CVERSION, OLD_STORE_VERSION_370 ) ) {
            oldStoreVersion = OLD_STORE_VERSION_370;
        } else if ( checkOldStoreVersion( conn, VERSION_TBL_35 + dbmgr.getBrokerID(),
            TVERSION_CVERSION, OLD_STORE_VERSION_350 ) ) {
            oldStoreVersion = OLD_STORE_VERSION_350;
        }

        // Get the store version
        boolean foundNewStore = false;
        int storeVersion = 0;
        try {
            storeVersion = daoFactory.getVersionDAO().getStoreVersion( conn );
        } catch ( BrokerException e ) {
            // Assume new store doesn't exist
            logger.log(Logger.WARNING, e.getMessage(), e.getCause() );
        }

        // Verify the version match
        if ( storeVersion > 0 ) {
            foundNewStore = ( storeVersion == STORE_VERSION );
            if ( foundNewStore ) {
                // Make sure tables exist
                DBTool.updateStoreVersion410IfNecessary( conn );
                if ( dbmgr.checkStoreExists(conn) == -1 ) {
                    logger.log(Logger.ERROR, BrokerResources.E_BAD_STORE_MISSING_TABLES);
                    throw new BrokerException(br.getKString(
                        BrokerResources.E_BAD_STORE_MISSING_TABLES));
                }
            } else {
                // Store doesn't have the version that we are expecting!
                String found = String.valueOf(storeVersion);
                String expected = String.valueOf(STORE_VERSION);
                logger.log(Logger.ERROR, BrokerResources.E_BAD_STORE_VERSION,
                    found, expected);
                throw new BrokerException(br.getKString(
                    BrokerResources.E_BAD_STORE_VERSION,
                    found, expected));
            }

            if ( ( oldStoreVersion > 0 ) && !removeStore ) {
                // old store exists & removeStore is not true so
                // log reminder message to remove old tables, i.e. 3.5
                logger.logToAll(Logger.INFO,
                    BrokerResources.I_REMOVE_OLDTABLES_REMINDER);
            }
        }

        // Process any cmd line options

        if ( foundNewStore ) {
            if ( removeStore ) {
                try {
                    // just drop all tables
                    DBTool.dropTables( conn, null );
                } catch (SQLException e) {
                    throw new BrokerException(
                        br.getKString(BrokerResources.E_REMOVE_JDBC_STORE_FAILED,
                        dbmgr.getOpenDBURL()), e);
                }
                status = false; // Signal calling method to close DB
            } else if ( resetStore ) {
                clearAll(true);
            }
        } else {
            boolean createNew = false;
            if ( createStore ) {
                // Are we supposed to automatically create the store?
                createNew = true;
            }

            boolean dropOld = false;
            String dropMsg = null;  // Reason for dropping the old store
            if ( oldStoreVersion > 0 ) {
                if ( removeStore ) {
                    dropOld = true;
                    dropMsg = BrokerResources.I_REMOVE_OLD_DATABASE_TABLES;
                } else if ( resetStore ) {
                    createNew = true;
                    dropOld = true;
                    dropMsg = BrokerResources.I_RESET_OLD_DATABASE_TABLES;
                } else {
                    // log message to do upgrade
                    logger.logToAll(Logger.INFO, BrokerResources.I_UPGRADE_STORE_MSG,
			new Integer( oldStoreVersion ) );

                    if (upgradeNoBackup && !Broker.getBroker().force) {
                        // will throw BrokerException if the user backs out
                        getConfirmation();
                    }

                    // Upgrade the store to the new version
                    if (!Globals.getHAEnabled()) {
                        new UpgradeStore(this, oldStoreVersion).upgradeStore(conn);
                        return status;
                    }
                }
            }

            if ( !createNew ) {
                // Error - user must create the store manually!
                logger.log(Logger.ERROR, BrokerResources.E_NO_DATABASE_TABLES);
                throw new BrokerException(
                    br.getKString(BrokerResources.E_NO_DATABASE_TABLES));
            }

            // Now, do required actions

            if ( dropOld ) {
                logger.logToAll(Logger.INFO, dropMsg);

                try {
                    // just drop all old tables
                    DBTool.dropTables(conn, dbmgr.getTableNames( oldStoreVersion ));
                } catch (Exception e) {
                    logger.logToAll(Logger.ERROR, BrokerResources.E_REMOVE_OLD_TABLES_FAILED, e);
                    throw new BrokerException(
                        br.getKString(BrokerResources.E_REMOVE_OLD_TABLES_FAILED), e);
                }
            }

            if ( createNew ) {
                logger.logToAll(Logger.INFO, BrokerResources.I_WILL_CREATE_NEW_STORE);

                try {
                    // create the tables
                    DBTool.createTables( conn );
                } catch (Exception e) {
                    String url = dbmgr.getCreateDBURL();
                    if ( url == null || url.length() == 0 ) {
                        url = dbmgr.getOpenDBURL();
                    }
                    String msg = br.getKString(
                        BrokerResources.E_CREATE_DATABASE_TABLE_FAILED, url);
                    logger.logToAll(Logger.ERROR, msg, e);
                    throw new BrokerException(msg, e);
                }
            }
        }

        return status;
    }

    /**
     * Get the current store session ID.
     */
    private long getStoreSession() throws BrokerException {
        if (Globals.getHAEnabled()) {
            return Globals.getStoreSession().longValue();
        } else {
            StoreSessionDAO sessionDAO = daoFactory.getStoreSessionDAO();
            return sessionDAO.getStoreSession( null, dbmgr.getBrokerID() );
        }
    }

    /**
     * Return true if table exists and stored version match what's expected
     * Return false if table does not exist
     */
    public boolean checkOldStoreVersion( Connection conn, String vTable,
        String vColumn, int version ) throws BrokerException {

        try {
            String selectSQL = "SELECT " + vColumn + " FROM " + vTable;

            Statement stmt = null;
            ResultSet rs = null;
            Exception myex = null;
            try {
                stmt = conn.createStatement();
                rs = stmt.executeQuery( selectSQL );
                if ( rs.next() ) {
                    int storeVersion = rs.getInt( 1 );
                    if ( storeVersion == version ) {
                        return true;
                    }

                    // Old store doesn't have the version we are expecting
                    String found = String.valueOf(storeVersion);
                    String expected = String.valueOf(version);
                    logger.log(Logger.ERROR, BrokerResources.E_BAD_OLDSTORE_VERSION,
                        found, expected);
                    throw new BrokerException(br.getKString(
                        BrokerResources.E_BAD_OLDSTORE_VERSION,
                        found, expected));
                } else {
                    // Old store doesn't have any data
                    logger.log(Logger.ERROR,
                        BrokerResources.E_BAD_OLDSTORE_NO_VERSIONDATA, vTable);
                    throw new BrokerException(br.getKString(
                        BrokerResources.E_BAD_OLDSTORE_NO_VERSIONDATA, vTable));
                }
            } catch ( SQLException e ) {
                myex = e;
                // assume that the table does not exist
                logger.log( Logger.DEBUG, "Assume old store does not exist because : " +
                    e.getMessage() );
            } finally {
                Util.close( rs, stmt, null, myex );
            }
        } catch ( Exception e ) {
            logger.log(Logger.ERROR, BrokerResources.X_STORE_VERSION_CHECK_FAILED, e);
            throw new BrokerException(
                br.getKString(BrokerResources.X_STORE_VERSION_CHECK_FAILED), e);
        }

        return false;
    }

    boolean resetMessage() {
	return resetMessage;
    }

    boolean resetInterest() {
	return resetInterest;
    }

    static class StoreSessionReaperTask extends TimerTask
    {
        private boolean canceled = false;
        Logger logger = Globals.getLogger();
        JDBCStore store = null;

        public StoreSessionReaperTask(JDBCStore store) {
            this.store = store;
        }

        public synchronized boolean cancel() {
            canceled = true;
            return super.cancel();
        }

        public void run() {
            synchronized(this) {
                if (canceled) {
                    return;
                }
            }

            try {
                StoreSessionDAO sesDAO = store.daoFactory.getStoreSessionDAO();
                sesDAO.deleteInactiveStoreSession(null);
            } catch (Exception e) {
                logger.logStack( Logger.ERROR,
                    BrokerResources.E_INACTIVE_SESSION_REMOVAL_FAILED, e );
            }
        }
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

        if (xid == null) throw new IllegalArgumentException("null xid");
        if (logRecord == null) throw new IllegalArgumentException("null logRecord");
        if (name == null) throw new IllegalArgumentException("null name");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.storeTMLogRecord("+xid+")");
        Util.logExt(logger_, java.util.logging.Level.FINE, 
                    "JDBCStore.storeTMLogRecord("+ xid+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTMLogRecordDAOJMSBG().insert(null,
                                      xid, logRecord, name, logger_);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
 
    }

    /**
     * Update a log record
     *
     * @param xid the global XID 
     * @param logRecord the new log record data for the xid
     * @param name the jmsbridge name
     * @param addIfNotExist
     * @param sync - not used
     * @param callback to obtain updated data 
     * @param logger_ can be null  
     * @exception KeyNotFoundException if not found and addIfNotExist false
     *            else Exception on erorr
     */
    public void updateTMLogRecord(String xid, byte[] logRecord, 
                                  UpdateOpaqueDataCallback callback,
                                  String name, boolean addIfNotExist, 
                                  boolean sync,
                                  java.util.logging.Logger logger_)
                                  throws Exception {

        if (xid == null) throw new IllegalArgumentException("null xid");
        if (logRecord == null) throw new IllegalArgumentException("null logRecord");
        if (name == null) throw new IllegalArgumentException("null name");
        if (callback == null) throw new IllegalArgumentException("null callback");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.updateTMLogRecord("+xid+")");
        Util.logExt(logger_, java.util.logging.Level.FINE, 
             "JDBCStore.updateTMLogRecord("+ xid+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTMLogRecordDAOJMSBG().updateLogRecord(null,
                                                        xid, logRecord, name,
                                                        callback, addIfNotExist,
                                                        logger_);
                    return;
                } catch (Exception e) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
    }

    /**
     * Remove a log record
     *
     * @param xid the global XID 
     * @param name the jmsbride name
     * @param sync - not used 
     * @param logger_ can be null  
     * @exception KeyNotFoundException if not found 
     *            else Exception on error
     */
    public void removeTMLogRecord(String xid, String name, 
                                  boolean sync, 
                                  java.util.logging.Logger logger_)
                                  throws KeyNotFoundException, Exception {

        if (xid == null) throw new IllegalArgumentException("null xid");
        if (name == null) throw new IllegalArgumentException("null name");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.removeTMLogRecord("+xid+")");
        Util.logExt(logger_, java.util.logging.Level.FINE, 
                    "JDBCStore.removeTMLogRecord("+ xid+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getTMLogRecordDAOJMSBG().delete(null,
                                                        xid, name, logger_);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
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

        if (xid == null) throw new IllegalArgumentException("null xid");
        if (name == null) throw new IllegalArgumentException("null name");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.getTMLogRecord("+xid+")");
        Util.logExt(logger_, java.util.logging.Level.FINE, 
                    "JDBCStore.getTMLogRecord("+ xid+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTMLogRecordDAOJMSBG().getLogRecord(null,
                                                               xid, name, logger_);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
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
    public long getTMLogRecordUpdatedTime(String xid,  String name, 
                                          java.util.logging.Logger logger_) 
                                          throws KeyNotFoundException, Exception {

        if (xid == null) throw new IllegalArgumentException("null xid");
        if (name == null) throw new IllegalArgumentException("null name");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.getTMLogRecordUpdatedTime("+xid+")");
        Util.logExt(logger_, java.util.logging.Level.FINE, 
                    "JDBCStore.getTMLogRecordUpdatedTime("+ xid+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTMLogRecordDAOJMSBG().getUpdatedTime(
                                                     null, xid, name, logger_);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
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

        if (xid == null) throw new IllegalArgumentException("null xid");
        if (name == null) throw new IllegalArgumentException("null name");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.getTMLogRecordCreatedTime("+xid+")");
        Util.logExt(logger_, java.util.logging.Level.FINE, 
                    "JDBCStore.getTMLogRecordcreatedTime("+ xid+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTMLogRecordDAOJMSBG().getCreatedTime(
                                                     null, xid, name, logger_);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
    }

    /**
     * Get all log records for a TM name
     *
     * @param name the jmsbridge name
     * @param logger_ can be null  
     * @return a list of log records
     * @exception Exception if error
     */
    public List getTMLogRecordsByName(String name,
                                      java.util.logging.Logger logger_)
                                      throws Exception {
        return getLogRecordsByNameByBroker(name, dbmgr.getBrokerID(), logger_);
    }

    /**
     * Get all log records for a JMS bridge in a broker 
     *
     * @param name the jmsbridge name
     * @param logger_ can be null  
     * @return a list of log records
     * @exception Exception if error
     */
    public List getLogRecordsByNameByBroker(String name,
                                            String brokerID,
                                            java.util.logging.Logger logger_)
                                            throws Exception {

        if (name == null) throw new IllegalArgumentException("null name");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.getTMLogRecordsByNameByBroker("+name+", "+brokerID+")");
        Util.logExt(logger_, java.util.logging.Level.FINE, 
                    "JDBCStore.getTMLogRecordsByNameByBroker("+name+", "+brokerID+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTMLogRecordDAOJMSBG().getLogRecordsByNameByBroker(
                                                               null, name, brokerID, logger_);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }

    }

    /**
     * Get JMS bridge names in all log records owned by a brokerID
     *
     * @param brokerID 
     * @param logger_ can be null  
     * @return a list of log records
     * @exception Exception if error
     */
    public List getNamesByBroker(String brokerID, 
                                 java.util.logging.Logger logger_)
                                 throws Exception {

        if (brokerID == null) throw new IllegalArgumentException("null brokerID");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.getTMNamesByBroker("+brokerID+")");
        Util.logExt(logger_, java.util.logging.Level.FINE, 
                    "JDBCStore.getTMNamesByBroker("+brokerID+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getTMLogRecordDAOJMSBG().getNamesByBroker(
                                                               null, brokerID, logger_);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }

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

        if (name == null) throw new IllegalArgumentException("null name");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.addJMSBridge("+name+")");
        Util.logExt(logger_, java.util.logging.Level.FINE,
                    "JDBCStore.addJMSBridge("+name+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getJMSBGDAO().insert(null, name, logger_);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
    }

    /**
     * Get JMS bridges owned by this broker 
     *
     * @param logger_ can be null
     * @return a list of names
     * @exception Exception if error
     */
    public List getJMSBridges(java.util.logging.Logger logger_)
                             throws Exception {
        return getJMSBridgesByBroker(dbmgr.getBrokerID(), logger_);
    }

    /**
     * Get JMS bridges owned by a broker
     *
     * @param brokerID 
     * @param logger_ can be null
     * @return a list of names
     * @exception Exception if error
     */
    public List getJMSBridgesByBroker(String brokerID,
                                      java.util.logging.Logger logger_)
                                      throws Exception {
 
        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.getJMSBridgesByBroker("+brokerID+")");
        Util.logExt(logger_, java.util.logging.Level.FINE,
                             "JDBCStore.getJMSBridges("+brokerID+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getJMSBGDAO().getNamesByBroker(null, brokerID, logger_);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
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

        if (name == null) throw new IllegalArgumentException("null name");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.getJMSBridgeUpdatedTime("+name+")");
        Util.logExt(logger_, java.util.logging.Level.FINE,
                             "JDBCStore.getJMSBridgeUpdatedTime("+name+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getJMSBGDAO().getUpdatedTime(null, name, logger_);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
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

        if (name == null) throw new IllegalArgumentException("null name");

        if (DEBUG) {
        logger.log(Logger.DEBUG, "JDBCStore.getJMSbridgeCreatedTime("+name+")");
        Util.logExt(logger_, java.util.logging.Level.FINE,
                    "JDBCStore.getJMSBridgeCreatedTime("+name+")", null);
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getJMSBGDAO().getCreatedTime(null, name, logger_);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy();
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            setInProgress(false);
        }
    }

    public void closeJMSBridgeStore() throws Exception {
        //ignore
    }

}
