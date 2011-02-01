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

package com.sun.messaging.jmq.jmsserver.persist.jdbc.sharecc;

import com.sun.messaging.jmq.util.UID;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.*;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.config.BrokerConfig;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.persist.ChangeRecordInfo;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.Util;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.DBTool;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.comm.BaseDAO;
import com.sun.messaging.jmq.jmsserver.persist.sharecc.ShareConfigChangeStore;
import com.sun.messaging.jmq.util.synchronizer.CloseInProgressSynchronizer;
import com.sun.messaging.jmq.util.synchronizer.CloseInProgressCallback;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 */
public class JDBCShareConfigChangeStore extends ShareConfigChangeStore
implements CloseInProgressCallback {

    private static boolean DEBUG = getDEBUG();

    public static final String SCHEMA_VERSION = "45";

    private static final String CLOSEWAIT_TIMEOUT_PROP = 
        ShareConfigChangeStore.STORE_TYPE_PROP+"CloseWaitTimeoutInSeconds";
    private static final int CLOSEWAIT_TIMEOUT_PROP_DEFAULT = 30; 

    private boolean createStore = false;
	private final Logger logger = Globals.getLogger();
    private final BrokerResources br = Globals.getBrokerResources();
    private final BrokerConfig config = Globals.getConfig();

    private ShareConfigChangeDBManager dbmgr = null;
    private ShareConfigRecordDAOFactory daoFactory = null;

    private final CloseInProgressSynchronizer inprogresser =
                         new CloseInProgressSynchronizer(logger);

    private int closeWaitTimeout = CLOSEWAIT_TIMEOUT_PROP_DEFAULT;

    /**
     */
    public JDBCShareConfigChangeStore() throws BrokerException {

        inprogresser.reset();
        closeWaitTimeout = config.getIntProperty(CLOSEWAIT_TIMEOUT_PROP, 
                                                 CLOSEWAIT_TIMEOUT_PROP_DEFAULT);

        dbmgr = ShareConfigChangeDBManager.getDBManager();
        daoFactory = dbmgr.getDAOFactory();

        String url = dbmgr.getOpenDBURL();
        if (url == null) {
            url = "not specified";
        }

        String user = dbmgr.getUser();
        if (user == null) {
            user = "not specified";
        }

        createStore = config.getBooleanProperty(CREATE_STORE_PROP, 
                                                CREATE_STORE_PROP_DEFAULT);
        if (createStore) {
            String args[] = { br.getString(BrokerResources.I_AUTOCREATE_ON),
                              String.valueOf(SCHEMA_VERSION), 
                              dbmgr.getClusterID(), url, user };
            logger.logToAll(Logger.INFO, br.getKString(
                            BrokerResources.I_SHARECC_JDBCSTORE_INFO, args));
        } else {
            String args[] = { br.getString(BrokerResources.I_AUTOCREATE_OFF), 
                              String.valueOf(SCHEMA_VERSION), 
                              dbmgr.getClusterID(), url, user };
            logger.logToAll(Logger.INFO, br.getKString(
                            BrokerResources.I_SHARECC_JDBCSTORE_INFO, args));
        }

        Connection conn = null;
        Exception myex = null;
        try {
            conn = dbmgr.getConnection(true);
            checkStore(conn);
        } catch (BrokerException e) {
            myex = e;
            throw e;
        } finally {
             dbmgr.closeSQLObjects(null, null, conn, myex);
        }
        dbmgr.setStoreInited(true);

        if (DEBUG) {
	        logger.log(Logger.DEBUG, "JDBCShareConfigChangeStore instantiated.");
        }
    }

    private void checkStore( Connection conn ) throws BrokerException {
        int ret=dbmgr.checkStoreExists(conn);
        if (ret == -1) {
            logger.log(Logger.ERROR, br.E_SHARECC_JDBCSTORE_MISSING_TABLE);
            throw new BrokerException(br.getKString(
                br.E_SHARECC_JDBCSTORE_MISSING_TABLE));
        }
        if (ret == 0) {
            if (!createStore) { 
                logger.log(Logger.ERROR, br.E_NO_SHARECC_JDBCSTORE_TABLE);
                throw new BrokerException(br.getKString(br.E_NO_DATABASE_TABLES));
            }

            logger.logToAll(Logger.INFO, br.getKString(
                br.I_SHARECC_JDBCSTORE_CREATE_NEW,
                br.getString(br.I_DATABASE_TABLE))); //only 1 table

            try {
                 DBTool.createTables( conn, false, dbmgr );
            } catch (Exception e) {
                String url = dbmgr.getCreateDBURL();
                if ( url == null || url.length() == 0 ) {
                     url = dbmgr.getOpenDBURL();
                }
                String emsg = br.getKString(br.E_FAIL_TO_CREATE,
                                           br.getString(br.I_DATABASE_TABLE));
                logger.logToAll(Logger.ERROR, emsg+"-"+url, e);
                throw new BrokerException(emsg, e);
            }
        }
    }

    public Properties getStoreShareProperties() {
        if (dbmgr == null) {
            throw new RuntimeException("JDBShareConfigChangeStore not initialized");
        }
        Properties p = new Properties();
        p.setProperty(dbmgr.getOpenDBUrlProp(), dbmgr.getOpenDBUrl());
        return p;
    }

    public String getVendorPropertySetting() {
        return dbmgr.getVendorProp()+"="+dbmgr.getVendor();
    }

    /**
     */
    public final String getStoreVersion() {
        return SCHEMA_VERSION;
    }

    private void checkClosedAndSetInProgress() throws BrokerException {

        try {
            inprogresser.checkClosedAndSetInProgressWithWait(closeWaitTimeout, 
                br.getKString(br.I_WAIT_ACCESS_SHARECC_JDBCSTORE));

        } catch (IllegalStateException e) {
            logger.log(Logger.ERROR, BrokerResources.E_STORE_ACCESSED_AFTER_CLOSED);
            throw new BrokerException(
                br.getKString(BrokerResources.E_STORE_ACCESSED_AFTER_CLOSED));

        } catch (java.util.concurrent.TimeoutException e) { 
            String msg = br.getKString(
                br.W_TIMEOUT_WAIT_ACCESS_SHARECC_JDBCSTORE,
                closeWaitTimeout);
            logger.log(Logger.ERROR, msg);
            throw new BrokerException(msg);

        } catch (Exception e) {
            String msg = br.getKString(
                br.E_FAIL_ACCESS_SHARECC_JDBCSTORE, e.toString());
            logger.log(Logger.ERROR, msg);
            throw new BrokerException(msg);
        }
    }

    public ChangeRecordInfo storeChangeRecord(ChangeRecordInfo rec, boolean sync)
                                              throws BrokerException {
	    if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCShareCCStore.storeChangeRecord called");
	    }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getShareConfigRecordDAO().insert(null, rec);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy(dbmgr);
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            inprogresser.setInProgress(false);
        }
    }

    public void storeResetRecord(ChangeRecordInfo rec, boolean canExist, boolean sync)
                                  throws BrokerException {
	    if (DEBUG) {
	    logger.log(Logger.DEBUG,
                "JDBCShareCCStore.storeResetRecord called");
	    }

        // make sure store is not closed then increment in progress count
        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getShareConfigRecordDAO().insertResetRecord(null, rec, canExist);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy(dbmgr);
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
            // decrement in progress count
            inprogresser.setInProgress(false);
        }
    }

    /**
     * Retrieves all the records whose sequence number greater than
     * the specified seq 
     * 
     * @return a List 
     */
    public List<ChangeRecordInfo> getChangeRecordsSince(Long seq, String resetUUID, boolean canReset)
    throws BrokerException {

        if (DEBUG) {
            logger.log(Logger.DEBUG,
                       "JDBCShareCCStore.getChangeRecordsSince("+seq+", "+resetUUID+")");
        }

        checkClosedAndSetInProgress();

	    try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getShareConfigRecordDAO().getRecords(
                                      null, seq, resetUUID, canReset);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy(dbmgr);
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
	    } finally {
	        inprogresser.setInProgress(false);
	    }
    }

    /**
     * Return all config records
     *
     * @return 
     * @exception BrokerException if an error occurs while getting the data
     */
    public List<ChangeRecordInfo> getAllChangeRecords() throws BrokerException {

        if (Store.getDEBUG()) {
            logger.log(Logger.DEBUG,
                "JDBCShareCCStore.getAllChangeRecords() called");
	    }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    return daoFactory.getShareConfigRecordDAO().getRecords(null, null, null, true);
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if ( retry == null ) {
                        retry = new Util.RetryStrategy(dbmgr);
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
	        inprogresser.setInProgress(false);
        }
    }

    /**
     * Clear all config change records 
     *
     * @param sync ignored
     * @exception BrokerException 
     */
    public void clearAllChangeRecords(boolean sync) throws BrokerException {

        if (DEBUG) {
	        logger.log(Logger.DEBUG,
                "JDBCShareCCStore.clearAllChangeRecords() called");
        }

        checkClosedAndSetInProgress();

        try {
            Util.RetryStrategy retry = null;
            do {
                try {
                    daoFactory.getShareConfigRecordDAO().deleteAll(null);
                    return;
                } catch ( Exception e ) {
                    // Exception will be log & re-throw if operation cannot be retry
                    if (retry == null) {
                        retry = new Util.RetryStrategy(dbmgr);
                    }
                    retry.assertShouldRetry( e );
                }
            } while ( true );
        } finally {
	        inprogresser.setInProgress(false);
        }
    }

    public void beforeWaitAfterSetClosed() {
        if (dbmgr != null) {
            dbmgr.setIsClosing();
        }
    }

    public void close() {

        try {
            inprogresser.setClosedAndWaitWithTimeout(this, closeWaitTimeout,
                br.getKString(br.I_WAIT_ON_CLOSED_SHARECC_JDBCSTORE));
        } catch (Exception e) {
            logger.log(logger.WARNING, br.getKString(
                br.W_CLOSE_SHARECC_JDBCSTORE_EXCEPTION, e.toString()));
        }

	    dbmgr.close();
        dbmgr.setStoreInited(false);

	    if (DEBUG) {
	        logger.log(Logger.DEBUG, "JDBCShareConfigChangeStore.close done.");
	    }
    }

    public String getStoreType() {
        return Store.JDBC_STORE_TYPE;
    }

    /*
     * Get debug information about the store.
     * @return A Hashtable of name value pair of information
     */
    public Hashtable getDebugState() throws BrokerException {

        Hashtable t = new Hashtable();
        t.put("JDBCSharedConfigChangeStore",
              "version:"+String.valueOf(SCHEMA_VERSION));

        Connection conn = null;
        Exception myex = null;
        try {
            conn = dbmgr.getConnection( true );

            Iterator itr = daoFactory.getAllDAOs().iterator();
            while ( itr.hasNext() ) {
                t.putAll( ((BaseDAO)itr.next()).getDebugInfo( conn ) );
            }
        } catch (BrokerException e) {
            myex = e;
            throw e;
        } finally {
            dbmgr.closeSQLObjects( null, null, conn, myex );
        }
        t.put("DBManager", dbmgr.getDebugState());

        return t;
    }

}
