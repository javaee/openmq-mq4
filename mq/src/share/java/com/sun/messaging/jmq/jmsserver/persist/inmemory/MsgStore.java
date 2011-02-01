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
 * @(#)MsgStore.java	1.58 06/29/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.inmemory;

import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;
import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MsgStore provides methods to persist/retrieve messages.
 */
class MsgStore {

    // properties used by the message store

    private Logger logger = Globals.getLogger();
    private BrokerResources br = Globals.getBrokerResources();

    // map destination to its messages ; DestinationUID->DstMsgStore
    private ConcurrentHashMap dstMap = new ConcurrentHashMap(32);

    private InMemoryStore store = null;

    static final private Enumeration emptyEnum = new Enumeration() {
	public boolean hasMoreElements() {
	    return false;
	}
	public Object nextElement() {
	    return null;
	}
    };

    /**
     * Messages are loaded on demand.
     */
    MsgStore(InMemoryStore s) {
	this.store = s;
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */  
    Hashtable getDebugState() {
	Hashtable t = new Hashtable();
        Iterator itr = dstMap.values().iterator();
        while (itr.hasNext()) {
            DstMsgStore dstMsgStore = (DstMsgStore)itr.next();
            t.putAll(dstMsgStore.getDebugState());
        }
	return t;
    }

    /**
     * Store a message, which is uniquely identified by it's SysMessageID,
     * and it's list of interests and their states.
     *
     * @param message	the message to be persisted
     * @param iids	an array of interest ids whose states are to be
     *			stored with the message
     * @param states	an array of states
     * @exception java.io.IOException if an error occurs while persisting the data
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if a message with the same id exists
     *			in the store already
     */
    void storeMessage(DestinationUID dst, Packet message, ConsumerUID[] iids,
	int[] states) throws IOException, BrokerException {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH, "storeMessage for " + dst);
	}

	// get from cache; create=true
	DstMsgStore dstMsgStore = getDstMsgStore(dst, true);
	dstMsgStore.storeMessage(message, iids, states);
    }

    /**
     * Return a message with the specified message id.
     */
    Packet getMessage(DestinationUID dst, SysMessageID mid) throws BrokerException {

	try {
            return getDstMsgStore(dst).getMessage(mid);
	} catch (IOException e) {
	    logger.log(Logger.ERROR,
                BrokerResources.X_LOAD_MESSAGE_FAILED, mid.toString(), e);
	    throw new BrokerException(
                br.getString(BrokerResources.X_LOAD_MESSAGE_FAILED,
                mid.toString()), e);
	}
    }

    /**
     * Tests whether the message exists.
     */
    boolean containsMessage(DestinationUID dst, SysMessageID mid)
	throws BrokerException {

        // get from cache; create=true
        DstMsgStore dstMsgStore = getDstMsgStore(dst, true);
        return dstMsgStore.containsMsg(mid);
    }

    /**
     * Remove the message from the persistent store.
     * If the message has an interest list, the interest list will be
     * removed as well.
     *
     * @param id	the system message id of the message to be removed
     * @exception java.io.IOException if an error occurs while removing the message
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the message is not found in the store
     */
    void removeMessage(DestinationUID dst, SysMessageID id)
	throws IOException, BrokerException {

        getDstMsgStore(dst).removeMessage(id);
    }

    void moveMessage(Packet message, DestinationUID from,
	DestinationUID to, ConsumerUID[] ints, int[] states)
	throws IOException, BrokerException {

	SysMessageID mid = message.getSysMessageID();

	// sanity check
	DstMsgStore fromdst = getDstMsgStore(from);
	if (fromdst == null || !fromdst.containsMsg(mid)) {
	    logger.log(Logger.ERROR,
                BrokerResources.E_MSG_NOT_FOUND_IN_STORE, mid, from);
	    throw new BrokerException(
                br.getString(BrokerResources.E_MSG_NOT_FOUND_IN_STORE, mid, from));
	}

	// first save the message and then remove the message
	storeMessage(to, message, ints, states);

	try {
	    fromdst.removeMessage(message.getSysMessageID());
	} catch (BrokerException e) {
	    // if we fails to remove the message; undo store
	    getDstMsgStore(to).removeMessage(message.getSysMessageID());

	    Object[] args = { mid, from, to };
	    logger.log(Logger.ERROR, BrokerResources.X_MOVE_MESSAGE_FAILED, args, e);
	    throw e;
	}
    }

    /**
     * Check if a a message has been acknowledged by all interests.
     *
     * @param dst  the destination the message is associated with
     * @param mid   the system message id of the message to be checked
     * @return true if all interests have acknowledged the message;
     * false if message has not been routed or acknowledge by all interests
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    public boolean hasMessageBeenAcked(DestinationUID dst, SysMessageID mid)
	throws BrokerException {

        return getDstMsgStore(dst).getMessageInfo(mid).hasMessageBeenAck();
    }

    /**
     * Get information about the underlying storage for the specified
     * destination. Only return info about vrfile.
     * @return A HashMap of name value pair of information
     */
    public HashMap getStorageInfo(Destination destination) throws BrokerException {
        // no info to return
        return new HashMap();
    }

    void compactDestination(Destination destination) throws BrokerException {
        // No-op
    }

    /**
     * Remove all messages associated with the specified destination
     * from the persistent store.
     *
     * @param dst	the destination whose messages are to be removed
     * @exception java.io.IOException if an error occurs while removing the messages
     * @exception BrokerException if the destination is not found in the store
     * @exception NullPointerException	if <code>destination</code> is
     *			<code>null</code>
     */
    void removeAllMessages(DestinationUID dst, boolean sync)
	throws IOException, BrokerException {

	// get from cache and instantiate if not found
	DstMsgStore dstMsgStore = getDstMsgStore(dst, true);
	if (dstMsgStore != null) {
	    dstMsgStore.removeAllMessages(sync);
	}
    }

    /**
     * Destination is being removed
     */
    void releaseMessageDir(DestinationUID dst) {
        DstMsgStore dstMsgStore = (DstMsgStore)dstMap.remove(dst);
        dstMsgStore.close();
    }

    /**
     * Return an enumeration of all persisted messages.
     * Use the Enumeration methods on the returned object to fetch
     * and load each message sequentially.
     * Not synchronized.
     *
     * <p>
     * This method is to be used at broker startup to load persisted
     * messages on demand.
     *
     * @return an enumeration of all persisted messages.
     */
    Enumeration messageEnumeration() {
	Enumeration msgEnum = new Enumeration() {
	    // get all destinations
	    Iterator dstitr =
		store.getDstStore().getDestinations().iterator();
	    Enumeration tempenum = null;
	    Object nextToReturn = null;

	    // will enumerate through all messages of all destinations
	    public boolean hasMoreElements() {
		while (true) {
		    if (tempenum != null) {
			if (tempenum.hasMoreElements()) {
			    // got the next message
			    nextToReturn = tempenum.nextElement(); 
			    return true;
			} else {
			    // continue to get the next enumeration
			    tempenum = null;
			}
		    } else {
			// get next enumeration
			while (dstitr.hasNext()) {
			    Destination dst = (Destination)dstitr.next();
			    try {
				// got the next enumeration
			    	tempenum = messageEnumeration(
						dst.getDestinationUID());
				break;
			    } catch (BrokerException e) {
				// log error and try to load messages for
				// the next destionation
				logger.log(Logger.ERROR,
					BrokerResources.X_LOAD_MESSAGES_FOR_DST_FAILED,
					dst.getDestinationUID(), e);
			    }
			}
			if (tempenum == null) {
			    return false; // no more
			}
		    }
		}
	    }

	    public Object nextElement() {
		if (nextToReturn != null) {
		    Object tmp = nextToReturn;
		    nextToReturn = null;
		    return tmp;
		} else {
		    throw new NoSuchElementException();
		}
	    }
	};

	return msgEnum;
    }

    /**
     * Return an enumeration of all persisted messages for the given
     * destination.
     * Use the Enumeration methods on the returned object to fetch
     * and load each message sequentially.
     *
     * This method is to be used at broker startup to load persisted
     * messages on demand.
     *
     * @return an enumeration of all persisted messages.
     */
    Enumeration messageEnumeration(DestinationUID dst) throws BrokerException {

	// get from cache and instantiate if not found
	DstMsgStore dstMsgStore = getDstMsgStore(dst, true);

	if (dstMsgStore != null) {
	    return dstMsgStore.messageEnumeration();
	} else {
	    return emptyEnum;
	}
    }

    /**
     * Return the message count for the given destination.
     */
    int getMessageCount(DestinationUID dst) throws BrokerException {

	// get from cache and instantiate if not found
	DstMsgStore dstMsgStore = getDstMsgStore(dst, true);

	if (dstMsgStore != null) {
	    return dstMsgStore.getMessageCount();
	} else {
	    return 0;
	}
    }

    /**
     * Return the byte count for the given destination.
     */
    long getByteCount(DestinationUID dst) throws BrokerException {

	// get from cache and instantiate if not found
	DstMsgStore dstMsgStore = getDstMsgStore(dst, true);

	if (dstMsgStore != null) {
	    return dstMsgStore.getByteCount();
	} else {
	    return 0;
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
    void storeInterestStates(DestinationUID dst,
	SysMessageID mid, ConsumerUID[] iids, int[] states, boolean sync)
	throws BrokerException {

	try {
            getDstMsgStore(dst).storeInterestStates(mid, iids, states);
	} catch (IOException e) {
	    logger.log(Logger.ERROR,
                BrokerResources.X_PERSIST_INTEREST_LIST_FAILED, mid.toString());
	    throw new BrokerException(
                br.getString(BrokerResources.X_PERSIST_INTEREST_LIST_FAILED,
                mid.toString()), e);
	}
    }

    /**
     * Update the state of the interest associated with the specified
     * message.  The interest should already be in the interest list
     * of the message.
     *
     * @param mid	system message id of the message that the interest
     *			is associated with
     * @param iid	id of the interest whose state is to be updated
     * @param state	state of the interest
     * @param sync	if true, will synchronize data to disk
     * @exception BrokerException if the message is not in the store; if the
     *			interest is not associated with the message; or if
     *			an error occurs while persisting the data
     */
    void updateInterestState(DestinationUID dst, SysMessageID mid,
	ConsumerUID iid, int state, boolean sync) throws BrokerException {

	try {
            getDstMsgStore(dst).updateInterestState(mid, iid, state);
	} catch (IOException e) {
	    // only this state is affected
	    logger.log(Logger.ERROR,
                BrokerResources.X_PERSIST_INTEREST_STATE_FAILED,
                iid.toString(), mid.toString());
	    throw new BrokerException(
                br.getString(BrokerResources.X_PERSIST_INTEREST_STATE_FAILED,
                iid.toString(), mid.toString()), e);
	}
    }

    int getInterestState(DestinationUID dst, SysMessageID mid, ConsumerUID iid)
	throws BrokerException {

        return getDstMsgStore(dst).getMessageInfo(mid).getInterestState(iid);
    }

    HashMap getInterestStates(DestinationUID dst, SysMessageID mid)
        throws BrokerException {

        return getDstMsgStore(dst).getMessageInfo(mid).getInterestStates();
    }

    /**
     * don't return id with state==INTEREST_STATE_ACKNOWLEDGED
     */
    ConsumerUID[] getConsumerUIDs(DestinationUID dst, SysMessageID mid)
	throws BrokerException {

        return getDstMsgStore(dst).getMessageInfo(mid).getConsumerUIDs();
    }

    // clear the store; when this method returns, the store has a state
    // that is the same as an empty store (same as when MsgStore
    // is instantiated with the clear argument set to true
    void clearAll() throws BrokerException {

	// clear all maps and internal data
        if (dstMap != null) {
            // false=no clean up needed
            // since we are going to delete all files afterwards
            closeAllDstMsgStore(false);
            dstMap.clear();
        }
    }

    // synchronized by caller
    void close(boolean cleanup) {

	closeAllDstMsgStore(cleanup);
	dstMap.clear();
    }

    private DstMsgStore getDstMsgStore(DestinationUID dst)
	throws BrokerException {

        DstMsgStore dstMsgStore = (DstMsgStore)dstMap.get(dst);
        if (dstMsgStore == null) {
            logger.log(Logger.ERROR,
                BrokerResources.E_DESTINATION_NOT_FOUND_IN_STORE, dst);
            throw new BrokerException(
                br.getString(BrokerResources.E_DESTINATION_NOT_FOUND_IN_STORE, dst));
        } else {
            return dstMsgStore;
        }
    }

    /**
     * 1. get it from cache
     * 2. if not found and instantiate is true, get file name and try to
     *    instantiate a DstMsgStore object passing in the load argument
     * 3. instantiation is done only if the directory already exists or the
     *    create argument is true.
     *
     * @exception BrokerException if the destination is not found in the store
     */
    private DstMsgStore getDstMsgStore(DestinationUID dst, boolean create)
        throws BrokerException {

	// throw exception if dst is not found
	store.getDstStore().checkDestination(dst);

	DstMsgStore dstMsgStore = (DstMsgStore)dstMap.get(dst);
        if (dstMsgStore == null && create) {
            dstMsgStore = new DstMsgStore(dst);
            Object oldValue = dstMap.putIfAbsent(dst, dstMsgStore);
            if (oldValue != null) {
                dstMsgStore = (DstMsgStore)oldValue;
            }
        }

	return dstMsgStore;
    }

    // close all stores
    private void closeAllDstMsgStore(boolean msgCleanup) {

	Iterator itr = dstMap.values().iterator();
	while (itr.hasNext()) {
	    DstMsgStore dstMsgStore = (DstMsgStore)itr.next();
	    if (dstMsgStore != null)
	    	dstMsgStore.close();
	}
    }
}
