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
 * @(#)PropertiesStore.java	1.22 08/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.inmemory;

import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keep track of persisted properties by using HashMap.
 */
class PropertiesStore {

    Logger logger = Globals.getLogger();
    BrokerResources br = Globals.getBrokerResources();

    // all persisted properties
    // maps property name -> property value
    private ConcurrentHashMap propMap = null;

    private File backingFile = null;

    // when instantiated, all data are loaded
    PropertiesStore() {
        propMap = new ConcurrentHashMap(64);
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
     * @exception BrokerException if an error occurs while persisting the
     *			timestamp
     */
    void updateProperty(String name, Object value)
	throws BrokerException {

        try {
            boolean updated = false;

            if (value == null) {
                // remove it
                Object old = propMap.remove(name);
                updated = (old != null);
            } else {
                // update it
                propMap.put(name, value);
                updated = true;
            }
        } catch (RuntimeException e) {
            logger.log(Logger.ERROR,
                BrokerResources.X_PERSIST_PROPERTY_FAILED, name);
            throw new BrokerException(
                br.getString(BrokerResources.X_PERSIST_PROPERTY_FAILED, name), e);
        }
    }

    /**
     * Retrieve the value for the specified property.
     *
     * @param name name of the property whose value is to be retrieved
     * @return the property value; null is returned if the specified
     *		property does not exist in the store
     * @exception BrokerException if an error occurs while retrieving the data
     * @exception NullPointerException if <code>name</code> is
     *			<code>null</code>
     */
    Object getProperty(String name) throws BrokerException {

        return propMap.get(name);
    }

    /**
     * Return the names of all persisted properties.
     *
     * @return an array of property names; an empty array will be returned
     *		if no property exists in the store.
     */
    String[] getPropertyNames() throws BrokerException {

        Set keys = propMap.keySet();
        return (String[])keys.toArray(new String[keys.size()]);
    }

    Properties getProperties() throws BrokerException {

        Properties props = new Properties();

        Iterator itr = propMap.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry e = (Map.Entry)itr.next();
            props.put(e.getKey(), e.getValue());
        }

        return props;
    }

    /**
     * Clear all records
     */
    // clear the store; when this method returns, the store has a state
    // that is the same as an empty store
    void clearAll() {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH, "PropertiesStore.clearAll() called");
	}

        propMap.clear();
    }

    void close(boolean cleanup) {
	if (Store.getDEBUG()) {
	    logger.log(Logger.DEBUGHIGH,
                "PropertiesStore: closing, " + propMap.size()+
                " in-memory properties");
	}

	propMap.clear();
    }

    /**
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */  
    Hashtable getDebugState() {
	Hashtable t = new Hashtable();
	t.put("Properties", String.valueOf(propMap.size()));
	return t;
    }

    // for internal use; not synchronized
    void printInfo(PrintStream out) {
	out.println("\nProperties");
	out.println("----------");
	out.println("backing file: "+ backingFile);

	// print all property name/value pairs
	Set entries = propMap.entrySet();
	Iterator itor = entries.iterator();
	while (itor.hasNext()) {
	    Map.Entry entry = (Map.Entry)itor.next();
	    out.println((String)entry.getKey() + "="
			+ entry.getValue().toString());
	}
    }
}


