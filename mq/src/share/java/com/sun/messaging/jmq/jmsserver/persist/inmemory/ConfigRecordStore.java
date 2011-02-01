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
 * @(#)ConfigRecordStore.java	1.24 06/29/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.inmemory;

import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.persist.ChangeRecordInfo;

import java.io.*;
import java.util.*;


/**
 * Keep track of all configuration change record.
 * File format: 4 byte magic number then each record is appended
 * to the backing file in this order:
 *	timestamp (long)
 *	length of byte array (int)
 *	byte array
 */
class ConfigRecordStore {

    Logger logger = Globals.getLogger();
    BrokerResources br = Globals.getBrokerResources();

    // cache all persisted records
    // list of time stamps
    private ArrayList timeList = new ArrayList(32);

    // list of records
    private ArrayList recordList = new ArrayList(32);

    ConfigRecordStore() {
    }

    /**
     * Append a new record to the config change record store.
     *
     * @param timestamp	The time when this record was created.
     * @param recordData The record data
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if an error occurs while persisting the data
     * @exception NullPointerException	if <code>recordData</code> is
     *			<code>null</code>
     */
    void storeConfigChangeRecord(long timestamp, byte[] recordData,
	boolean sync) throws BrokerException {

	synchronized (timeList) {
	    timeList.add(new Long(timestamp));
	    recordList.add(recordData);
	}
    }

    /**
     * Retrieve all records in the store since timestamp.
     *
     * @return a list of ChangeRecordInfo, empty list of no record
     */
    public ArrayList<ChangeRecordInfo> getConfigChangeRecordsSince(long timestamp) {

	ArrayList records = new ArrayList();

	synchronized (timeList) {
	    int size = timeList.size();

	    int i = 0;
	    for (; i < size; i++) {
		Long stamp = (Long)timeList.get(i);
		if (stamp.longValue() > timestamp)
		    break;
	    }

	    for (; i < size; i++) {
		records.add(new ChangeRecordInfo((byte[])recordList.get(i),
                        ((Long)timeList.get(i)).longValue()));
	    }

	    return records;
	}
    }

    /**
     * Return all config records with their corresponding timestamps.
     *
     * @return a list of ChangeRecordInfo
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if an error occurs while getting the data
     */
    public List<ChangeRecordInfo> getAllConfigRecords() throws BrokerException {
    ArrayList records = new ArrayList();

    for (int i = 0; i < timeList.size(); i++) {
        records.add(new ChangeRecordInfo((byte[])recordList.get(i),
                        ((Long)timeList.get(i)).longValue()));
    }
    return records;
    }

    /**
     * Clear all records
     */
    // clear the store; when this method returns, the store has a state
    // that is the same as an empty store (same as when TxnAckList is
    // instantiated with the clear argument set to true
    void clearAll() throws BrokerException {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH, "ConfigRecordStore.clearAll() called");
	}

	synchronized (timeList) {
	    timeList.clear();
	    recordList.clear();
	}
    }

    void close(boolean cleanup) {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH,
                "ConfigRecordStore: closing, "+timeList.size()+
                " in-memory records");
	}

        synchronized (timeList) {
            timeList.clear();
            recordList.clear();
        }
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */  
    Hashtable getDebugState() {
	Hashtable t = new Hashtable();
	t.put("Config change records", String.valueOf(timeList.size()));
	return t;
    }

    void printInfo(PrintStream out) {
	out.println("\nConfiguration Change Record");
	out.println("---------------------------");
	out.println("number of records: " + timeList.size());
    }
}


