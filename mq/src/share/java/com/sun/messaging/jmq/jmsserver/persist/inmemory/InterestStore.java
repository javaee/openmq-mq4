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
 * @(#)InterestStore.java	1.35 08/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.inmemory;

import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.jmsserver.core.Consumer;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keep track of all persisted Interest objects
 */
class InterestStore {

    Logger logger = Globals.getLogger();
    BrokerResources br = Globals.getBrokerResources();

    // cache of all stored interests; maps interest id -> interest
    private ConcurrentHashMap interestMap = null;

    /**
     * When instantiated, all interests are loaded.
     */
    InterestStore() {
        interestMap = new ConcurrentHashMap(256);
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */  
    Hashtable getDebugState() {
	Hashtable t = new Hashtable();
	t.put("Consumers", String.valueOf(interestMap.size()));
	return t;
    }

    /**
     * Print out usage info in the interest directory.
     */
    public void printInfo(PrintStream out) {
	out.println("\nInterests");
	out.println("---------");
	out.println("number of interests:   " + interestMap.size());
    }

    /**
     * Store an interest which is uniquely identified by it's interest id.
     *
     * @param interest	interest to be persisted
     * @exception java.io.IOException if an error occurs while persisting the interest
     * @exception BrokerException if an interest with the same id exists
     *			in the store already
     */
    void storeInterest(Consumer interest)
	throws IOException, BrokerException {

	ConsumerUID id = interest.getConsumerUID();

        try {
            Object oldValue = interestMap.putIfAbsent(id, interest);
            if (oldValue != null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_INTEREST_EXISTS_IN_STORE, id,
                    interest.getDestinationUID().getLongString());
                throw new BrokerException(
                    br.getString(BrokerResources.E_INTEREST_EXISTS_IN_STORE, id,
                    interest.getDestinationUID().getLongString()));
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR, BrokerResources.X_PERSIST_INTEREST_FAILED, id, e);
            throw e;
        }
    }

    /**
     * Remove the interest from the persistent store.
     *
     * @param interest	the interest to be removed
     * @exception java.io.IOException if an error occurs while removing the interest
     * @exception BrokerException if the interest is not found in the store
     */
    void removeInterest(Consumer interest)
	throws IOException, BrokerException {

	Object oldinterest = null;
        ConsumerUID id = interest.getConsumerUID();

        try {
            oldinterest = interestMap.remove(id);
            if (oldinterest == null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_INTEREST_NOT_FOUND_IN_STORE, id,
            interest.getDestinationUID().getLongString());
                throw new BrokerException(
                    br.getString(BrokerResources.E_INTEREST_NOT_FOUND_IN_STORE,
                    id, interest.getDestinationUID().getLongString()));
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR, BrokerResources.X_REMOVE_INTEREST_FAILED, id);
            throw new BrokerException(
                br.getString(BrokerResources.X_REMOVE_INTEREST_FAILED, id), e);
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
     * @exception java.io.IOException if an error occurs while getting the data
     */
    Consumer[] getAllInterests() throws IOException {

        return (Consumer[])interestMap.values().toArray(new Consumer[0]);
    }

    // clear the store; when this method returns, the store has a state
    // that is the same as an empty store (same as when InterestStore
    // is instantiated with the clear argument set to true
    void clearAll() {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH, "InterestStore.clearAll() called");
	}

        interestMap.clear();
    }

    void close(boolean cleanup) {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH,
                "InterestStore: closing, " + interestMap.size() +
                " in-memory interests");
	}

	interestMap.clear();
    }
}

