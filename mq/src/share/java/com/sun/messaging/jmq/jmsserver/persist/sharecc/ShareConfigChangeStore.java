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
 */ 

package com.sun.messaging.jmq.jmsserver.persist.sharecc;

import java.util.List;
import java.util.Properties;
import com.sun.messaging.jmq.util.UID;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.BrokerStateHandler;
import com.sun.messaging.jmq.jmsserver.config.*;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.persist.ChangeRecordInfo;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
/**
 * This class contains static methods to obtain a singleton instance
 * for storing and retrieving change records of durable subscriptions
 * and administratively created destinations needed by a conventional
 * cluster that uses a shared store
 */

public abstract class ShareConfigChangeStore {

    private static boolean DEBUG = false;

    public static final String CLUSTER_SHARECC_PROP_PREFIX = Globals.IMQ + ".cluster.sharecc";
    public static final String STORE_TYPE_PROP = Globals.IMQ + ".cluster.sharecc.persist";
    public static final String DEFAULT_STORE_TYPE = Store.JDBC_STORE_TYPE;

    public static final String CREATE_STORE_PROP = STORE_TYPE_PROP+"Create";
    public static final boolean CREATE_STORE_PROP_DEFAULT = Store.CREATE_STORE_PROP_DEFAULT;

    private static final String CLASS_PROP_SUFFIX = ".class";


    static private final String DEFAULT_JDBCSTORE_CLASS =
	"com.sun.messaging.jmq.jmsserver.persist.jdbc.sharecc.JDBCShareConfigChangeStore";

    // Singleton Store instance
    static private ShareConfigChangeStore store = null;

    public static boolean getDEBUG() {
        return DEBUG;
    }

    /**
     * Return a singleton instance of a Store object.
     */
    public static synchronized ShareConfigChangeStore 
    getStore() throws BrokerException {

        if (store != null) {
            return store;
        }

        if (BrokerStateHandler.shuttingDown) {
            throw new BrokerException(
                Globals.getBrokerResources().getKString(
                    BrokerResources.X_SHUTTING_DOWN_BROKER),
                    BrokerResources.X_SHUTTING_DOWN_BROKER);
        }

        BrokerConfig config = Globals.getConfig();

        String type = config.getProperty(STORE_TYPE_PROP, DEFAULT_STORE_TYPE);
        if (!type.toLowerCase().equals(DEFAULT_STORE_TYPE)) {
            throw new BrokerException(
            "Not supported"+STORE_TYPE_PROP+"="+type);
        }

        String classprop = STORE_TYPE_PROP + "."+type + CLASS_PROP_SUFFIX;
        String classname = config.getProperty(classprop);
	    if (classname == null || classname.equals("")) {
            classname = DEFAULT_JDBCSTORE_CLASS;
        } else if (!classname.equals(DEFAULT_JDBCSTORE_CLASS)) {
            throw new BrokerException(
            "Not supported "+classprop+"="+classname);
        }

        try {
            store = (ShareConfigChangeStore)Class.forName(
                        classname).newInstance();
        } catch (Exception e) {
            throw new BrokerException(Globals.getBrokerResources().getKString(
            BrokerResources.E_FAIL_OPEN_SHARECC_STORE, e.getMessage()), e);
        }

        return store;
    }

    /**
     * Release the singleton instance 
     * <p>
     * The next time <code>getStore()</code> is called, a new instance will
     * be instantiated.
     * @param	cleanup	if true, the store will be cleaned up, i.e.
     *			redundant data removed.
     */
    public static synchronized void 
    releaseStore(boolean cleanup) {

	    if (store != null) {
            store.close();
	    }
	    store = null;
    }

    /**
     * @param rec the record to be inserted
     * @param sync true sync to disk
     * @param return the inserted record
     */
    public ChangeRecordInfo storeChangeRecord(ChangeRecordInfo rec, boolean sync)
                                              throws BrokerException {
        throw new UnsupportedOperationException("Unsupported operation");

    }

    
    /**
     * @param rec the reset record to be inserted
     * @param canExist throw exception if the reset record already exists
     * @param sync true sync to disk
     */
    public void storeResetRecord(ChangeRecordInfo rec, boolean canExist, boolean sync)
                                 throws BrokerException {
        throw new UnsupportedOperationException("Unsupported operation");

    }

    /**
     * @param seq get records whose sequence number > seq
     */
    public List<ChangeRecordInfo> getChangeRecordsSince(Long seq, String resetUUID, boolean canReset)
    throws BrokerException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public List<ChangeRecordInfo> getAllChangeRecords() throws BrokerException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void clearAllChangeRecords(boolean sync) throws BrokerException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public abstract Properties getStoreShareProperties();

    public abstract String getVendorPropertySetting();

    public abstract void close();

}