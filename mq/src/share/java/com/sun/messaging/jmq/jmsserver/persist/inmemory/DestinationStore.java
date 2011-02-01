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
 * @(#)DestinationStore.java	1.32 08/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.inmemory;

import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Keep track of all persisted destinations by using HashMap.
 */
class DestinationStore {

    Logger logger = Globals.getLogger();
    BrokerResources br = Globals.getBrokerResources();

    // maps destination's ID -> Destination
    private ConcurrentHashMap dstMap = null;
    private InMemoryStore parent = null;

    // when instantiated, all data are loaded
    DestinationStore(InMemoryStore p) {
	this.parent = p;
        dstMap = new ConcurrentHashMap(64);
    }

    /**
     * Store a Destination.
     *
     * @param destination	the destination to be persisted
     * @exception java.io.IOException if an error occurs while persisting
     *		the destination
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the same destination exists
     *			the store already
     * @exception NullPointerException	if <code>destination</code> is
     *			<code>null</code>
     */
    void storeDestination(Destination destination)
	throws IOException, BrokerException {

	DestinationUID did = destination.getDestinationUID();

        try {
            Object oldValue = dstMap.putIfAbsent(did, destination);
            if (oldValue != null) {
                logger.log(Logger.ERROR, BrokerResources.E_DESTINATION_EXISTS_IN_STORE,
                    destination.getName());
                throw new BrokerException(br.getString(
                    BrokerResources.E_DESTINATION_EXISTS_IN_STORE, destination.getName()));
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR, BrokerResources.X_PERSIST_DESTINATION_FAILED,
                destination.getName());
            throw new BrokerException(br.getString(
                BrokerResources.X_PERSIST_DESTINATION_FAILED,
                destination.getName()), e);
        }
    }

    /**
     * Update the destination in the persistent store.
     *
     * @param destination	the destination to be updated
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the destination is not found in the store
     *			or if an error occurs while updating the destination
     */
    void updateDestination(Destination destination)
	throws BrokerException {

        DestinationUID did = destination.getDestinationUID();

        try {
            Object oldValue = dstMap.replace(did, destination);
            if (oldValue == null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_DESTINATION_NOT_FOUND_IN_STORE, destination.getName());
                throw new BrokerException(
                    br.getString(BrokerResources.E_DESTINATION_NOT_FOUND_IN_STORE,
                    destination.getName()));
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR,
                BrokerResources.X_PERSIST_DESTINATION_FAILED, destination.getName());
            throw new BrokerException(
                br.getString(BrokerResources.X_PERSIST_DESTINATION_FAILED,
                destination.getName()), e);
        }
    }

    /**
     * Remove the destination from the persistent store.
     * All messages associated with the destination will be removed as well.
     *
     * @param destination	the destination to be removed
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if the destination is not found in the store
     */
    void removeDestination(Destination destination)
	throws BrokerException {

	DestinationUID did = destination.getDestinationUID();

        try {
            Object oldValue = dstMap.remove(did);
            if (oldValue == null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_DESTINATION_NOT_FOUND_IN_STORE,
                    destination.getName());
                throw new BrokerException(br.getString(
                    BrokerResources.E_DESTINATION_NOT_FOUND_IN_STORE,
                    destination.getName()));
            }

            // remove all messages associated with this destination
            parent.getMsgStore().releaseMessageDir(did);
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR,
                BrokerResources.X_REMOVE_DESTINATION_FAILED,
                destination.getName(), e);
            throw new BrokerException(
                br.getString(BrokerResources.X_REMOVE_DESTINATION_FAILED,
                destination.getName()), e);
        }
    }

    /**
     * Retrieve the destination from the persistent store.
     *
     * @param did the destination to be retrieved
     * @return a Destination object
     */
    Destination getDestination(DestinationUID did) throws IOException {

        return (Destination)dstMap.get(did);
    }

    /**
     * Retrieve all destinations in the store.
     *
     * @return an array of Destination objects; a zero length array is
     * returned if no destinations exist in the store
     * @exception java.io.IOException if an error occurs while getting the data
     */
    Destination[] getAllDestinations() throws IOException {

        return (Destination[])dstMap.values().toArray(new Destination[0]);
    }

    // return the names of all persisted destination
    Collection getDestinations() {

        return dstMap.values();
    }

    /**
     * Clear all destinations
     */
    void clearAll(boolean clearMessages) {

	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH,
		"DestinationStore.clearAll(" + clearMessages + ") called");
	}

        if (clearMessages) {
            Iterator itr = dstMap.values().iterator();
            while (itr.hasNext()) {
                Destination dst = (Destination)itr.next();
                DestinationUID did = dst.getDestinationUID();
                parent.getMsgStore().releaseMessageDir(did);
            }
        }
        dstMap.clear();
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */  
    Hashtable getDebugState() {
	Hashtable t = new Hashtable();
	t.put("Destinations", String.valueOf(dstMap.size()));
	return t;
    }

    void close(boolean cleanup) {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH,
                "DestinationStore: closing, " + dstMap.size()+ " in-memory destinations");
	}
    }

    // check whether the specified destination exists
    // throw BrokerException if it does not exist
    void checkDestination(DestinationUID did) throws BrokerException {
        if (!dstMap.containsKey(did)) {
            logger.log(Logger.ERROR,
                BrokerResources.E_DESTINATION_NOT_FOUND_IN_STORE, did.toString());
            throw new BrokerException(
                br.getString(BrokerResources.E_DESTINATION_NOT_FOUND_IN_STORE,
                did.toString()));
        }
    }
}

