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
 * @(#)MessageInfo.java	1.22 08/31/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.inmemory;

import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;

import java.io.*;
import java.util.*;

/**
 * MessageInfo keeps track of a message and it's ack list.
 * Has methods to parse and persist them.
 */
class MessageInfo {

    private Logger logger = Globals.getLogger();
    private BrokerResources br = Globals.getBrokerResources();

    private Packet msg;
    private SysMessageID mid;
    private int packetSize;
    private DstMsgStore parent = null;

    // interest list info; iid -> position of state in statearray
    private HashMap iidMap = null;

    // states
    private int[] statearray = null;

    /**
     * if this returns successfully, message and it's interest states
     * are persisted; store message in individual files
     */
    MessageInfo(DstMsgStore p, Packet msg, ConsumerUID[] iids, int[] states)
        throws BrokerException {

	this.parent = p;

	mid = (SysMessageID)msg.getSysMessageID().clone();
	packetSize = msg.getPacketSize();

        int size = iids.length;
        iidMap = new HashMap();
        statearray = new int[iids.length];
        storeStates(iids, states);
    }

    /**
     * Return the message.
     * The message is only cached when it is loaded from the backing buffer
     * the first time.
     * It will be set to null after it is retrieved.
     * From then on, we always read it from the backing buffer again.
     */
    synchronized Packet getMessage() throws IOException {
        return msg;
    }

    // no need to synchronized, value set at object creation and wont change
    int getSize() {
	return packetSize; 
    }

    // no need to synchronized, value set at object creation and wont change
    SysMessageID getID() {
	return mid;
    }

    synchronized void storeStates(ConsumerUID[] iids, int[] states)
        throws BrokerException {

	if (iidMap.size() != 0) {
	    // the message has a list already
	    logger.log(Logger.WARNING, BrokerResources.E_MSG_INTEREST_LIST_EXISTS, mid.toString());
	    throw new BrokerException(
                br.getString(BrokerResources.E_MSG_INTEREST_LIST_EXISTS, mid.toString()));
	}

        int size = iids.length;
        iidMap = new HashMap(size);
        statearray = new int[size];
        for (int i = 0; i < size; i++) {
            iidMap.put(iids[i], new Integer(i));
            statearray[i] = states[i];
        }
    }

    synchronized void updateState(ConsumerUID iid, int state)
	throws IOException, BrokerException {

	Integer indexObj = null;
	if (iidMap == null || (indexObj = (Integer)iidMap.get(iid)) == null) {

	    logger.log(Logger.ERROR, BrokerResources.E_INTEREST_STATE_NOT_FOUND_IN_STORE,
			iid.toString(), mid.toString());
	    throw new BrokerException(
			br.getString(BrokerResources.E_INTEREST_STATE_NOT_FOUND_IN_STORE,
			iid.toString(), mid.toString()));
	}

	int index = indexObj.intValue();
	if (statearray[index] != state) {
	    statearray[index] = state;
	}
    }

    synchronized int getInterestState(ConsumerUID iid) throws BrokerException {

	Integer indexobj = null;
	if (iidMap == null || (indexobj = (Integer)iidMap.get(iid)) == null) {
	    logger.log(Logger.ERROR, BrokerResources.E_INTEREST_STATE_NOT_FOUND_IN_STORE,
			iid.toString(), mid.toString());
	    throw new BrokerException(
			br.getString(BrokerResources.E_INTEREST_STATE_NOT_FOUND_IN_STORE,
			iid.toString(), mid.toString()));
	} else {
	    return statearray[indexobj.intValue()];
	}
    }

    synchronized HashMap getInterestStates() {

        HashMap states = new HashMap();
        if (iidMap != null) {
            Set entries = iidMap.entrySet();
            Iterator itor = entries.iterator();
            while (itor.hasNext()) {
                Map.Entry entry = (Map.Entry)itor.next();
                int index = ((Integer)entry.getValue()).intValue();
                states.put(entry.getKey(), Integer.valueOf(statearray[index]));
            }
        }

        return states;
    }

    /**
     * Return ConsumerUIDs whose associated state is not
     * INTEREST_STATE_ACKNOWLEDGED.
     */
    synchronized ConsumerUID[] getConsumerUIDs() {

	ConsumerUID[] ids = new ConsumerUID[0];
	if (iidMap != null) {
	    ArrayList list = new ArrayList();

	    Set entries = iidMap.entrySet();
	    Iterator itor = entries.iterator();
	    while (itor.hasNext()) {
		Map.Entry entry = (Map.Entry)itor.next();
		Integer index = (Integer)entry.getValue();

		if (statearray[index.intValue()] !=
		    Store.INTEREST_STATE_ACKNOWLEDGED) {
			list.add(entry.getKey());
		}
	    }
	    ids = (ConsumerUID[])list.toArray(ids);
	}

	return ids;
    }

    /**
     * Check if a a message has been acknowledged by all interests.
     *
     * @return true if all interests have acknowledged the message;
     * false if message has not been routed or acknowledge by all interests
     */
    synchronized boolean hasMessageBeenAck() {

        // To be safe, message is considered unrouted if interest list is empty
        if (statearray != null && statearray.length > 0) {
            for (int i = 0, len = statearray.length; i < len; i++) {
                if (statearray[i] != Store.INTEREST_STATE_ACKNOWLEDGED) {
                    return false; // Not all interests have acked
                }
            }

            return true; // Msg has been routed and acked
        }

        return false;
    }
}
