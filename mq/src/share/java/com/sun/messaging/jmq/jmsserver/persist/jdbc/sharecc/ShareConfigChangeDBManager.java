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

import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.config.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.Util;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.comm.BaseDAO;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.comm.CommDBManager;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.comm.DBConnectionPool;
import com.sun.messaging.jmq.jmsserver.persist.sharecc.ShareConfigChangeStore;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.net.*;
import javax.sql.*;

/**
 */
public final class ShareConfigChangeDBManager extends CommDBManager {

    static final String JDBC_PROP_PREFIX = ShareConfigChangeStore.STORE_TYPE_PROP+
                                           "."+Store.JDBC_STORE_TYPE;

    static final int LONGEST_TABLENAME_LEN = 11;

    private BrokerResources br = Globals.getBrokerResources();

    // cluster id to make table name unique per cluster
    private String clusterID = null;

    private ShareConfigRecordDAOFactory daoFactory = null;
    private DBConnectionPool dbpool = null;
    private boolean storeInited = false;

    private static final Object classLock = ShareConfigChangeDBManager.class;
    private static ShareConfigChangeDBManager dbMgr = null;

    protected boolean getDEBUG() {
        return ShareConfigChangeStore.getDEBUG();
    }


    /**
     * Get DBManager method for singleton pattern.
     * @return DBManager
     * @throws BrokerException
     */
    public static
    ShareConfigChangeDBManager getDBManager() throws BrokerException {
        synchronized (classLock) {
            if (dbMgr == null) {
                dbMgr = new ShareConfigChangeDBManager();
                dbMgr.loadTableSchema();
                dbMgr.dbpool = new DBConnectionPool(dbMgr, "ccshare", true);
                dbMgr.initDBMetaData();
            }
        }
        return dbMgr;
    }

    protected String getJDBCPropPrefix() {
        return JDBC_PROP_PREFIX;
    }

    protected String getStoreTypeProp() {
        return ShareConfigChangeStore.STORE_TYPE_PROP;
    }

    protected String getCreateStoreProp() {
        return ShareConfigChangeStore.CREATE_STORE_PROP;
    }

    protected boolean getCreateStorePropDefault() {
        return ShareConfigChangeStore.CREATE_STORE_PROP_DEFAULT;
    }

    protected String getLogStringTag() {
        return "["+JDBC_PROP_PREFIX+"]";
    }

    public String toString() {
        return "CCShareDBManger";
    }

    protected Connection getConnection() throws BrokerException {
        return dbpool.getConnection();
    }

    public void freeConnection(Connection c, Throwable thr)
    throws BrokerException {

        dbpool.freeConnection(c, thr);
    }


    protected void
    checkMaxTableNameLength(int maxTableNameLength) throws BrokerException {
        if (maxTableNameLength > 0) {
            // We do know the max number of chars allowed for a table
            // name so verify brokerID or clusterID is within limit.
            int longest = LONGEST_TABLENAME_LEN;
            if (isOracle()) {
                longest += 4;
            }
            if ((clusterID.length()+longest+1) > maxTableNameLength) {
                Object[] args = { clusterID, Integer.valueOf(maxTableNameLength),
                                  Integer.valueOf(longest+1) };
                throw new BrokerException(br.getKString(
                    BrokerResources.E_CLUSTER_ID_TOO_LONG, args));
            }
        }
    }

    protected boolean isStoreInited() {
        return storeInited;
    }

    protected void setStoreInited(boolean b) {
        storeInited = b;
    }


    /**
     * When instantiated, the object configures itself by reading the
     * properties specified in BrokerConfig.
     */
    private ShareConfigChangeDBManager() throws BrokerException {

        initDBManagerProps();
        initDBDriver();
    }

    protected void initTableSuffix() throws BrokerException {
        clusterID = Globals.getClusterID();
        if (clusterID == null || clusterID.length() == 0 ||
            !Util.isAlphanumericString(clusterID)) {
             throw new BrokerException(br.getKString(
                 BrokerResources.E_BAD_CLUSTER_ID, clusterID));
        }

        // Use cluster ID as the suffix
        tableSuffix = "C" + clusterID;
    }

    public Hashtable getDebugState() {
        Hashtable ht = super.getDebugState();
        ht.put("clusterID", ""+clusterID);
        ht.put(dbpool.toString(), dbpool.getDebugState());
        return ht;
    }

    public ShareConfigRecordDAOFactory getDAOFactory() {
        synchronized(classLock) {
            if (daoFactory == null) {
                daoFactory = new ShareConfigRecordDAOFactory();
            }
        }
        return daoFactory;
    }

    protected BaseDAO getFirstDAO() throws BrokerException {
        return (BaseDAO)((List)getDAOFactory().getAllDAOs()).get(0);
    }

    public Iterator allDAOIterator() throws BrokerException {
        return getDAOFactory().getAllDAOs().iterator();
    }

    String getClusterID() {
        return clusterID;
    }

    protected void close() {
        synchronized (classLock) {
            dbpool.close();
            super.close();
            dbMgr = null;
        }
    }

    // Get all names of tables used in a specific store version
    public String[] getTableNames(int version) {
        return (String[]) tableSchemas.keySet().toArray(new String[0]);
    }

    public int checkStoreExists(Connection conn) throws BrokerException {
        return super.checkStoreExists(conn, 
            JDBCShareConfigChangeStore.SCHEMA_VERSION);
    }

    public boolean hasSupplementForCreateDrop(String tableName) {
        return true;
    }
  
    public void lockTable(Connection conn, boolean doLock) throws BrokerException {
    }

    public void
    closeSQLObjects(ResultSet rset, Statement stmt, Connection conn, Throwable ex)
    throws BrokerException {
        try {
            if ( rset != null ) {
                rset.close();
            }
            if ( stmt != null ) {
                stmt.close();
            }
        } catch ( SQLException e ) {
            throw new BrokerException(
                br.getKString(br.E_UNEXPECTED_EXCEPTION, e.toString()), e );
        } finally {
            if ( conn != null ) {
                freeConnection( conn, ex );
            }
        }
    }
}
