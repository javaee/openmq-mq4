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
 * @(#)DstMsgStore.java	1.31 08/31/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.inmemory;

import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.jmsserver.core.*;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DstMsgStore keeps track of messages for a destination.
 */
class DstMsgStore {

    Logger logger = Globals.getLogger();
    BrokerResources br = Globals.getBrokerResources();

    DestinationUID myDestination = null;

    // cache of all messages of the destination; message id -> MessageInfo
    private ConcurrentHashMap messageMap = new ConcurrentHashMap(4096);

    /**
     * The msgCount and byteCount are
     * - initialized when the DstMsgStore is instantiated
     * - incremented when new messages are stored
     * - decremented when messages are removed
     * - and reset to 0 when the dst is purged or removed
     */
    private int msgCount = 0;
    private long byteCount = 0;
    private Object countLock = new Object();

    DstMsgStore(DestinationUID dst) {
	myDestination = dst;
    }

    MessageInfo storeMessage(Packet message, ConsumerUID[] iids, int[] states)
        throws IOException, BrokerException {

	SysMessageID id = message.getSysMessageID();
        MessageInfo info = new MessageInfo(this, message, iids, states);

        // cache it, make sure to use the cloned SysMessageID
        Object oldmsg = messageMap.putIfAbsent(info.getID(), info);
        if (oldmsg != null) {
            logger.log(Logger.ERROR,
                BrokerResources.E_MSG_EXISTS_IN_STORE, id, myDestination);
            throw new BrokerException(
                br.getString(BrokerResources.E_MSG_EXISTS_IN_STORE, id, myDestination));
        }

        // increate destination message count and byte count
        int msgsize = message.getPacketSize();
        incrMsgCount(msgsize);

        return info;
    }

    /**
     * Remove the message from the persistent store.
     * If the message has an interest list, the interest list will be
     * removed as well.
     *
     * @param id	the system message id of the message to be removed
     * @exception java.io.IOException if an error occurs while removing the message
     * @exception BrokerException if the message is not found in the store
     */
    void removeMessage(SysMessageID id)
	throws IOException, BrokerException {

	MessageInfo oldmsg = (MessageInfo)messageMap.remove(id);
        if (oldmsg == null) {
            logger.log(Logger.ERROR, BrokerResources.E_MSG_NOT_FOUND_IN_STORE,
                id, myDestination);
            throw new BrokerException(
                br.getString(BrokerResources.E_MSG_NOT_FOUND_IN_STORE,
                id, myDestination));
        }

	// decrement destination message count and byte count
	decrMsgCount(oldmsg.getSize());
    }

    /**
     * Remove all messages associated with this destination.
     *
     * @exception java.io.IOException if an error occurs while removing the message
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the message is not found in the store
     */
    void removeAllMessages(boolean sync) throws IOException, BrokerException {
        messageMap.clear();
        clearCounts();
    }

    /**
     * Return an enumeration of all persisted messages.
     * Use the Enumeration methods on the returned object to fetch
     * and load each message sequentially.
     *
     * <p>
     * This method is to be used at broker startup to load persisted
     * messages on demand.
     *
     * @return an enumeration of all persisted messages.
     */
    Enumeration messageEnumeration() {
        return new MsgEnumeration(this, getMessageIterator());
    }

    /**
     * return the number of messages in this file
     */
    int getMessageCount() throws BrokerException {
        if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "DstMsgStore:getMessageCount()");
	}

	return msgCount;
    }

    /**
     * return the number of bytes in this file
     */
    long getByteCount() throws BrokerException {
        if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG, "DstMsgStore:getByteCount()");
	}

	return byteCount;
    }

    protected void close() {
	messageMap.clear();
    }

    MessageInfo getMessageInfo(SysMessageID mid) throws BrokerException {

        MessageInfo info = (MessageInfo)messageMap.get(mid);
        if (info == null) {
            logger.log(Logger.ERROR, BrokerResources.E_MSG_NOT_FOUND_IN_STORE,
                mid, myDestination);
            throw new BrokerException(
                br.getString(BrokerResources.E_MSG_NOT_FOUND_IN_STORE,
                mid, myDestination));
        }

        return info;
    }

    // synchronized access to messageMap.keySet().iterator();
    private Iterator getMessageIterator() {
        return messageMap.keySet().iterator();
    }

    private void incrMsgCount(int msgSize) {
	synchronized (countLock) {
	    msgCount++;
	    byteCount += msgSize;
	}
    }

    private void decrMsgCount(int msgSize) {
	synchronized (countLock) {
	    msgCount--;
	    byteCount -= msgSize;
	}
    }

    private void clearCounts() {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUG,
		"DstMsgStore:clearCounts for " + myDestination);
	}

	synchronized (countLock) {
	    msgCount = 0;
	    byteCount = 0;
	}
    }

    private static class MsgEnumeration implements Enumeration {
	DstMsgStore parent = null;
	Iterator itr = null;

	Object objToReturn = null;

	MsgEnumeration(DstMsgStore p, Iterator i) {
	    parent = p;
	    itr = i;
	}

	public boolean hasMoreElements() {
            if (itr.hasNext()) {
                objToReturn = itr.next();
                return true;	// RETURN TRUE
            } else {
                return false;
            }
	}

	public Object nextElement() {
	    if (objToReturn != null) {
		Object result = null;;
		if (objToReturn instanceof SysMessageID) {
		    try {
			result = parent.getMessage((SysMessageID)objToReturn);
		    } catch (IOException e) {
			// failed to load message
			// log error; and continue
			parent.logger.log(Logger.ERROR,
                            BrokerResources.X_RETRIEVE_MESSAGE_FAILED,
                            objToReturn, parent.myDestination, e);
			throw new NoSuchElementException();
		    } catch (BrokerException e) {
			// msg not found
			// log error; and continue
			parent.logger.log(Logger.ERROR,
                            BrokerResources.X_RETRIEVE_MESSAGE_FAILED,
                            objToReturn, parent.myDestination, e);
			throw new NoSuchElementException();
		    }
		} else {
		    result = objToReturn;
		}
		objToReturn = null;
		return result;
	    } else {
		throw new NoSuchElementException();
	    }
	}
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */  
    Hashtable getDebugState() {
	Hashtable t = new Hashtable();
	t.put((myDestination+":messages in-memory"),
			String.valueOf(msgCount));
	return t;
    }

    // the following 3 methods are added to make sure
    // writing/reading to/from the backing file is synchronized

    void storeInterestStates(SysMessageID mid, ConsumerUID[] iids, int[] states)
	throws IOException, BrokerException {

        getMessageInfo(mid).storeStates(iids, states);
    }

    void updateInterestState(
	SysMessageID mid, ConsumerUID iid, int state)
	throws IOException, BrokerException {

        getMessageInfo(mid).updateState(iid, state);
    }

    boolean containsMsg(SysMessageID mid) {
        return messageMap.containsKey(mid);
    }

    Packet getMessage(SysMessageID mid) throws IOException, BrokerException {

        MessageInfo msginfo = (MessageInfo)messageMap.get(mid);
        if (msginfo == null) {
            logger.log(Logger.ERROR, BrokerResources.E_MSG_NOT_FOUND_IN_STORE,
                mid, myDestination);
            throw new BrokerException(
                br.getString(BrokerResources.E_MSG_NOT_FOUND_IN_STORE,
                    mid, myDestination));
        } else {
            return msginfo.getMessage();
        }
    }
}

