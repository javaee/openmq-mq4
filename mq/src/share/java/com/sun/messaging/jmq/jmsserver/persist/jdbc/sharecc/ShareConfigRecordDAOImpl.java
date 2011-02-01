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
import com.sun.messaging.jmq.io.Status;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.persist.ChangeRecordInfo;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.Util;
import com.sun.messaging.jmq.jmsserver.persist.jdbc.comm.CommDBManager;

import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.sql.*;
import java.io.IOException;

/**
 * This class implements ShareConfigRecordDAO
 */
public class ShareConfigRecordDAOImpl extends ShareConfigRecordBaseDAOImpl
implements ShareConfigRecordDAO {

    protected String tableName;

    // SQLs

    protected String insertSQLOracle;
    protected String selectSeqSQLOracle;

    protected String insertSQL;
    protected String insertResetRecordSQL;
    protected String insertResetRecordWithLockSQL;
    protected String selectRecordsSinceSQL;
    protected String selectSinceWithResetRecordSQL;
    protected String selectAllSQL;
    protected String selectSeqByUUIDSQL;
    protected String updateResetRecordUUIDSQL;
    protected String setResetRecordFLAGNULLSQL;
    protected String selectResetRecordUUIDSQL;

    /**
     * Constructor
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    ShareConfigRecordDAOImpl() throws BrokerException {

        tableName = getDBManager().getTableName( TABLE_NAME_PREFIX );

        insertSQL = new StringBuffer(128)
            .append( "INSERT INTO " ).append( tableName )
            .append( " ( " )
            .append( UUID_COLUMN ).append( ", " )
            .append( RECORD_COLUMN ).append( ", " )
            .append( TYPE_COLUMN ).append( ", " )
            .append( CREATED_TS_COLUMN )
            .append( ") VALUES ( ?, ?, ?, ? )" )
            .toString();

        insertSQLOracle = new StringBuffer(128)
            .append( "INSERT INTO " ).append( tableName )
            .append( " ( " )
            .append( SEQ_COLUMN ).append( ", " )
            .append( UUID_COLUMN ).append( ", " )
            .append( RECORD_COLUMN ).append( ", " )
            .append( TYPE_COLUMN ).append( ", " )
            .append( CREATED_TS_COLUMN )
            .append( ") VALUES ("+tableName+"_seq.NEXTVAL, ?, ?, ?, ? )" )
            .toString();

        selectSeqSQLOracle = new StringBuffer(128)
            .append( "SELECT " ).append( tableName+"_seq.CURRVAL " )
            .append( "FROM DUAL" )
            .toString();

        insertResetRecordSQL = new StringBuffer(128)
            .append( "INSERT INTO " ).append( tableName )
            .append( " ( " )
            .append( SEQ_COLUMN ).append( ", " )
            .append( UUID_COLUMN ).append( ", " )
            .append( RECORD_COLUMN ).append( ", " )
            .append( TYPE_COLUMN ).append( ", " )
            .append( CREATED_TS_COLUMN )
            .append( ") VALUES ( 1, ?, ?, ?, ? )" )
            .toString();

        insertResetRecordWithLockSQL = new StringBuffer(128)
            .append( "INSERT INTO " ).append( tableName )
            .append( " ( " )
            .append( SEQ_COLUMN ).append( ", " )
            .append( UUID_COLUMN ).append( ", " )
            .append( RECORD_COLUMN ).append( ", " )
            .append( TYPE_COLUMN ).append( ", " )
            .append( CREATED_TS_COLUMN ).append(", ")
            .append( FLAG_COLUMN )
            .append( ") VALUES ( 1, ?, ?, ?, ?, ? )" )
            .toString();

        selectSinceWithResetRecordSQL = new StringBuffer(128)
            .append( "SELECT " )
            .append( SEQ_COLUMN ).append( ", " )
            .append( UUID_COLUMN ).append( ", " )
            .append( RECORD_COLUMN ).append( ", " )
            .append( TYPE_COLUMN ).append( ", " )
            .append( CREATED_TS_COLUMN )
            .append( " FROM " ).append( tableName )
            .append( " WHERE " )
            .append( SEQ_COLUMN ).append( " > ?" )
            .append( " OR " )
            .append( TYPE_COLUMN ).append(  " = " )
            .append( ChangeRecordInfo.TYPE_RESET_PERSISTENCE )
            .append( " ORDER BY " ).append( SEQ_COLUMN )
            .toString();

        selectAllSQL = new StringBuffer(128)
            .append( "SELECT " )
            .append( SEQ_COLUMN ).append( ", " )
            .append( UUID_COLUMN ).append( ", " )
            .append( RECORD_COLUMN ).append( ", " )
            .append( TYPE_COLUMN ).append( ", " )
            .append( CREATED_TS_COLUMN )
            .append( " FROM " ).append( tableName )
            .append( " ORDER BY " ).append( SEQ_COLUMN )
            .toString();

        selectSeqByUUIDSQL = new StringBuffer(128)
            .append( "SELECT " )
            .append( SEQ_COLUMN )
            .append( " FROM " ).append( tableName )
            .append( " WHERE " )
            .append( UUID_COLUMN ).append( " = ?" )
            .toString();

        updateResetRecordUUIDSQL = new StringBuffer(128)
            .append( "UPDATE " ).append( tableName )
            .append( " SET " )
            .append( UUID_COLUMN ).append( " = ?" )
            .append( " WHERE " )
            .append( TYPE_COLUMN ).append( " = " )
            .append( ChangeRecordInfo.TYPE_RESET_PERSISTENCE )
            .toString();

        setResetRecordFLAGNULLSQL = new StringBuffer(128)
            .append( "UPDATE " ).append( tableName )
            .append( " SET " )
            .append( FLAG_COLUMN ).append( " = NULL" )
            .append( " WHERE " )
            .append( TYPE_COLUMN ).append( " = " )
            .append( ChangeRecordInfo.TYPE_RESET_PERSISTENCE )
            .toString();

        selectResetRecordUUIDSQL = new StringBuffer(128)
            .append( "SELECT " )
            .append( UUID_COLUMN )
            .append( " FROM " ).append( tableName )
            .append( " WHERE " )
            .append( TYPE_COLUMN ).append( " = " )
            .append( ChangeRecordInfo.TYPE_RESET_PERSISTENCE )
            .toString();
    }

    /**
     * Get the prefix name of the table.
     * @return table name
     */
    public final String getTableNamePrefix() {
        return TABLE_NAME_PREFIX;
    }

    /**
     * Get the name of the table.
     * @return table name
     */
    public final String getTableName() {
        return tableName;
    }

    public ChangeRecordInfo insert( Connection conn, 
                                    ChangeRecordInfo rec )
                                    throws BrokerException {
        return insert(  conn, rec, true );
    }

    /**
     * Insert a new entry.
     *
     * @param conn database connection
     * @param rec the record to be inserted   
     * @return the record inserted     
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    public ChangeRecordInfo insert( Connection conn, 
                                    ChangeRecordInfo rec,
                                    boolean clearFlag )
                                    throws BrokerException {

        String sql = null;
        boolean myConn = false;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Exception myex = null;
        try {
            CommDBManager mgr = getDBManager();
            if ( conn == null ) {
                conn = mgr.getConnection( (mgr.isDerby() ? true:false) );
                myConn = true;
            }
            String resetUUID = rec.getResetUUID(); 

            if (clearFlag) {
                setResetRecordFLAGNULL( conn );
            }

            if (mgr.isOracle()) {
                sql = insertSQLOracle;
            } else {
                sql = insertSQL;
            }
            if (mgr.supportsGetGeneratedKey()) {
                pstmt = conn.prepareStatement( sql, new String[]{SEQ_COLUMN} );
            } else {
                pstmt = conn.prepareStatement( sql );
            }
            pstmt.setString( 1, rec.getUUID() );
            Util.setBytes( pstmt, 2, rec.getRecord() );
            pstmt.setInt( 3, rec.getType() );
            pstmt.setLong( 4, rec.getTimestamp() );
            pstmt.executeUpdate();
            Long seq = null;
            if (mgr.supportsGetGeneratedKey()) {
                rs = pstmt.getGeneratedKeys();
                if (rs.next()) {
                    seq = new Long(rs.getLong( 1 ));
                }
            } else if (mgr.isOracle()) {
                Statement st = conn.createStatement();  
                rs = st.executeQuery( selectSeqSQLOracle );
                if (rs.next()) {
                    seq = new Long(rs.getLong( 1 ));
                }
                rs.close();
                rs = null;
                st.close();
            } else {
                seq = getSequenceByUUID( conn, rec.getUUID() );
            }
            if (seq == null) {
                throw new BrokerException(
                    br.getKString(br.X_SHARECC_FAIL_GET_SEQ_ON_INSERT, rec));
            }
            String currResetUUID = getResetRecordUUID( conn );
            if (resetUUID != null && !currResetUUID.equals(resetUUID)) {
                throw new BrokerException(br.getKString(br.X_SHARECC_TABLE_RESET,
                   "["+resetUUID+", "+currResetUUID+"]"), Status.PRECONDITION_FAILED);
            }

            if ( myConn  && !conn.getAutoCommit() ) {
                conn.commit();
            }

            if (resetUUID == null) {
                rec.setResetUUID( currResetUUID ); 
            }
            rec.setSeq(seq);

            return rec;

        } catch ( Exception e ) {
            myex = e;
            try {
                if ( (conn != null) && !conn.getAutoCommit() ) {
                    conn.rollback();
                }
            } catch ( SQLException rbe ) {
                logger.log( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
            }

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = getDBManager().wrapSQLException("[" + sql + "]", (SQLException)e);
            } else {
                ex = e;
            }

            throw new BrokerException(
                br.getKString( br.X_SHARECC_INSERT_RECORD_FAIL, 
                    rec.toString(), ex.getMessage() ), ex );
        } finally {
            if ( myConn ) {
                closeSQLObjects( rs, pstmt, conn, myex );
            } else {
                closeSQLObjects( rs, pstmt, null, myex );
            }
        }
    }

    /**
     * Insert reset record.
     *
     * @param conn database connection
     * @param rec the reset record to be inserted   
     * @param canExist Exception if the reset record already exist
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    public void insertResetRecord( Connection conn, 
                                   ChangeRecordInfo rec,
                                   boolean canExist)
                                   throws BrokerException {

        String sql = null;
        boolean myConn = false;
        PreparedStatement pstmt = null;
        Exception myex = null;
        try {
            // Get a connection
            if ( conn == null ) {
                conn = getDBManager().getConnection( false );
                myConn = true;
            }

            if (!hasResetRecord( conn )) {
                sql = insertResetRecordSQL;
                if (!canExist) {
                    sql = insertResetRecordWithLockSQL;
                }
                pstmt = conn.prepareStatement( sql );
                pstmt.setString( 1, rec.getUUID() );
                Util.setBytes( pstmt, 2, rec.getRecord() );
                pstmt.setInt( 3, ChangeRecordInfo.TYPE_RESET_PERSISTENCE );
                pstmt.setLong( 4, rec.getTimestamp() );
                if (!canExist) {
                    pstmt.setInt( 5,  ChangeRecordInfo.FLAG_LOCK);
                }
                int count = pstmt.executeUpdate();
                if ( count == 1) {
                    setResetRecordUUID( conn, rec.getUUID() );
                 } else if (!hasResetRecord(conn) || !canExist ) {
                     throw new BrokerException("Unexpected affected row count "+count+" on  "+sql);
                 }
            } else if (!canExist) {
                 throw new BrokerException(br.getKString(br.E_SHARECC_TABLE_NOT_EMPTY, 
                     ((ShareConfigChangeDBManager)getDBManager()).getClusterID()));
            }
            if ( myConn ) {
                conn.commit();
            }
        } catch ( Exception e ) {
            myex = e;
            try {
                if ( (conn != null) && !conn.getAutoCommit() ) {
                    conn.rollback();
                }
            } catch ( SQLException rbe ) {
                logger.log( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
            }

            try {
                boolean exist = hasResetRecord( conn );
                if (exist) {
                    if (!canExist) {
                        throw new BrokerException(br.getKString(br.E_SHARECC_TABLE_NOT_EMPTY, 
                            ((ShareConfigChangeDBManager)getDBManager()).getClusterID()));
                    }
                    myex = null;
                    return;
                }
            } catch ( Exception e2 ) {
                 logger.log( Logger.ERROR, br.getKString(
                     br.E_SHARECC_CHECK_EXIST_EXCEPTION, e2.getMessage()));
            }

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = getDBManager().wrapSQLException("[" + insertResetRecordSQL + "]", (SQLException)e);
            } else {
                ex = e;
            }

            throw new BrokerException(
                br.getKString(br.X_SHARECC_INSERT_RESET_RECORD_FAIL, ex.getMessage()), ex);
        } finally {
            if ( myConn ) {
                closeSQLObjects( null, pstmt, conn, myex );
            } else {
                closeSQLObjects( null, pstmt, null, myex );
            }
        }
    }

    /**
     *
     * @param conn database connection
     * @param uuid  
     * @param record 
     * @param timestamp
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    public boolean hasResetRecord( Connection conn )
    throws BrokerException {

        boolean myConn = false;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Exception myex = null;
        try {
            // Get a connection
            if ( conn == null ) {
                conn = getDBManager().getConnection( true );
                myConn = true;
            }

            pstmt = conn.prepareStatement( selectAllSQL );
            rs = pstmt.executeQuery();
            if ( rs.next() ) {
                int typ  = rs.getInt( 4 );
                if (typ == ChangeRecordInfo.TYPE_RESET_PERSISTENCE) {
                    return true;
                } 
                throw new BrokerException(
                "Unexpected 1st record type "+typ+" for first record in database table "+getTableName());
            } else {
                return false;
            }
        } catch ( Exception e ) {
            myex = e;
            try {
                if ( (conn != null) && !conn.getAutoCommit() ) {
                    conn.rollback();
                }
            } catch ( SQLException rbe ) {
                logger.log( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
            }

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = getDBManager().wrapSQLException("[" + selectAllSQL + "]", (SQLException)e);
            } else {
                ex = e;
            }

            throw new BrokerException(br.getKString(
                br.X_SHARECC_QUERY_RESET_RECORD_FAIL, ex.getMessage()), ex);
        } finally {
            if ( myConn ) {
                closeSQLObjects( null, pstmt, conn, myex );
            } else {
                closeSQLObjects( null, pstmt, null, myex );
            }
        }
    }

    /**
     *
     * @param conn database connection
     * @param uuid  
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    public void setResetRecordUUID( Connection conn, String uuid )
                                    throws BrokerException {

        boolean myConn = false;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Exception myex = null;
        try {
            // Get a connection
            if ( conn == null ) {
                conn = getDBManager().getConnection( true );
                myConn = true;
            }

            pstmt = conn.prepareStatement( updateResetRecordUUIDSQL );
            pstmt.setString( 1, uuid );
            if ( pstmt.executeUpdate() < 1 ) {
                throw new BrokerException(
                "Unexpected affected row count for "+updateResetRecordUUIDSQL);
            } 
        } catch ( Exception e ) {
            myex = e;
            try {
                if ( (conn != null) && !conn.getAutoCommit() ) {
                    conn.rollback();
                }
            } catch ( SQLException rbe ) {
                logger.log( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
            }

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = getDBManager().wrapSQLException("[" + updateResetRecordUUIDSQL + "]", (SQLException)e);
            } else {
                ex = e;
            }

            throw new BrokerException(br.getKString(
            br.X_SHARECC_SET_RESET_RECORD_UUID_FAIL, uuid, ex.getMessage()), ex);
        } finally {
            if ( myConn ) {
                closeSQLObjects( rs, pstmt, conn, myex );
            } else {
                closeSQLObjects( rs, pstmt, null, myex );
            }
        }
    }

    /**
     *
     * @param conn database connection
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    public void setResetRecordFLAGNULL( Connection conn )
    throws BrokerException {

        boolean myConn = false;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Exception myex = null;
        try {
            // Get a connection
            if ( conn == null ) {
                conn = getDBManager().getConnection( true );
                myConn = true;
            }

            pstmt = conn.prepareStatement( setResetRecordFLAGNULLSQL );
            if ( pstmt.executeUpdate() < 1 ) {
                throw new BrokerException(
                "Unexpected affected row count for "+setResetRecordFLAGNULLSQL);
            } 
        } catch ( Exception e ) {
            myex = e;
            try {
                if ( (conn != null) && !conn.getAutoCommit() ) {
                    conn.rollback();
                }
            } catch ( SQLException rbe ) {
                logger.log( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
            }

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = getDBManager().wrapSQLException("[" +setResetRecordFLAGNULLSQL+ "]", (SQLException)e);
            } else {
                ex = e;
            }

            throw new BrokerException(br.getKString(
            br.X_SHARECC_CLEAR_RESET_RECORD_FLAG_FAIL, FLAG_COLUMN, ex.getMessage()), ex);
        } finally {
            if ( myConn ) {
                closeSQLObjects( rs, pstmt, conn, myex );
            } else {
                closeSQLObjects( rs, pstmt, null, myex );
            }
        }
    }


    /**
     *
     * @param conn database connection
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    public String getResetRecordUUID( Connection conn )
                                throws BrokerException {

        boolean myConn = false;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Exception myex = null;
        try {
            // Get a connection
            if ( conn == null ) {
                conn = getDBManager().getConnection( true );
                myConn = true;
            }

            pstmt = conn.prepareStatement( selectResetRecordUUIDSQL );
            rs = pstmt.executeQuery();
            String uuid = null;
            if ( rs.next() ) {
                uuid = rs.getString( 1 );
            }
            if (uuid == null) {
                throw new BrokerException(
                "No reset record found in database table "+getTableName());
            }
            return uuid;
        } catch ( Exception e ) {
            myex = e;
            try {
                if ( (conn != null) && !conn.getAutoCommit() ) {
                    conn.rollback();
                }
            } catch ( SQLException rbe ) {
                logger.log( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
            }

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = getDBManager().wrapSQLException("[" + selectResetRecordUUIDSQL + "]", (SQLException)e);
            } else {
                ex = e;
            }

            throw new BrokerException(br.getKString(
            br.X_SHARECC_QUERY_RESET_RECORD_UUID_FAIL, ex.getMessage()), ex);
        } finally {
            if ( myConn ) {
                closeSQLObjects( rs, pstmt, conn, myex );
            } else {
                closeSQLObjects( rs, pstmt, null, myex );
            }
        }
    }

    /**
     * Get records since sequence seq (exclusive) or get all records 
     *
     * @param conn database connection
     * @param seq sequence number, null if get all records
     * @param resetUUID last reset UUID this broker has processed 
     * @return an array of ShareConfigRecord 
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    public List<ChangeRecordInfo> getRecords( Connection conn, Long seq,
                                              String resetUUID, boolean canReset )
                                              throws BrokerException {

        ArrayList<ChangeRecordInfo> records =  new ArrayList<ChangeRecordInfo>();

        boolean myConn = false;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String selectSQL = null;
        Exception myex = null;
        try {
            // Get a connection
            if ( conn == null ) {
                conn = getDBManager().getConnection( false );
                myConn = true;
            }

            if (seq == null || resetUUID == null) {
                records = getAllRecords( conn );

            } else {

			    selectSQL  = selectSinceWithResetRecordSQL;
                pstmt = conn.prepareStatement( selectSQL );
                pstmt.setLong( 1, seq.longValue() );
         
                rs = pstmt.executeQuery();
                long seqv = -1;
                String uuidv = null;
                byte[] buf =  null;
                int typv = 0;
                long tsv = -1;
                ChangeRecordInfo cri = null;
                boolean loadfail = false;
                boolean reseted = false, foundreset = false;
                String newResetUUID = null;
                while ( rs.next() ) {
                    try {
                        seqv = rs.getLong( 1 );
                        uuidv  = rs.getString( 2 );
                        buf = Util.readBytes( rs, 3 );
                        typv  = rs.getInt( 4 );
                        tsv  = rs.getLong( 5 );
                        if (typv == ChangeRecordInfo.TYPE_RESET_PERSISTENCE) {
                            foundreset = true;
                            if (uuidv.equals(resetUUID)) {
                                continue;
                            }
                            newResetUUID = uuidv;
                            reseted = true;
                            break;
                        }
                        cri = new ChangeRecordInfo(Long.valueOf(seqv), uuidv, buf, typv, tsv);
                        cri.setResetUUID(resetUUID);
                        cri.setIsSelectAll(false);
                        records.add(cri);
                    } catch (IOException e) {
                        loadfail = true;
                        IOException ex = getDBManager().wrapIOException(
                                         "[" + selectAllSQL + "]", e );
                        logger.logStack( Logger.ERROR,
                            BrokerResources.X_PARSE_CONFIGRECORD_FAILED,
                            String.valueOf( seq ), ex);
                    }
                }
                if (!foundreset) {
                    throw new BrokerException(
                    "Unexpected: shared database table "+getTableName()+" has no reset record",
                    Status.PRECONDITION_FAILED);
                }
                if (reseted) {
                    if (!canReset) {
                        throw new BrokerException(br.getKString(br.X_SHARECC_TABLE_RESET,
                        "["+resetUUID+", "+newResetUUID+"]"), Status.PRECONDITION_FAILED);
                    }
                    logger.log(logger.INFO, br.getKString(br.I_SHARECC_USE_RESETED_TABLE,
                                            "["+resetUUID+", "+newResetUUID+"]"));
                    records = getAllRecords( conn );
                }
            }
            if (myConn) {
                conn.commit();
            }
        } catch ( Exception e ) {
            myex = e;
            try {
                if ( (conn != null) && !conn.getAutoCommit() ) {
                    conn.rollback();
                }
            } catch ( SQLException rbe ) {
                logger.log( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
            }

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = getDBManager().wrapSQLException("[" + selectSQL + "]", (SQLException)e);
            } else {
                ex = e;
            }

            throw new BrokerException( br.getKString(
                br.X_SHARECC_QUERY_RECORDS_FAIL, ex.getMessage()), ex );
        } finally {
            if ( myConn ) {
                closeSQLObjects( rs, pstmt, conn, myex );
            } else {
                closeSQLObjects( rs, pstmt, null, myex );
            }
        }

        return records;
    }

    public ArrayList<ChangeRecordInfo> getAllRecords( Connection conn )
    throws BrokerException {

        ArrayList<ChangeRecordInfo> records =  new ArrayList<ChangeRecordInfo>();

        boolean myConn = false;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Exception myex = null;
        try {
            // Get a connection
            if ( conn == null ) {
                conn = getDBManager().getConnection( true );
                myConn = true;
            }

            pstmt = conn.prepareStatement( selectAllSQL );
            rs = pstmt.executeQuery();
            long seqv = -1;
            String uuidv = null;
            byte[] buf =  null;
            int typv = 0;
            long tsv = -1;
            ChangeRecordInfo cri = null;
            boolean foundreset = false;
            while ( rs.next() ) {
                try {
                    seqv = rs.getLong( 1 );
                    uuidv  = rs.getString( 2 );
                    buf = Util.readBytes( rs, 3 );
                    typv  = rs.getInt( 4 );
                    if (typv == ChangeRecordInfo.TYPE_RESET_PERSISTENCE) {
                        foundreset = true;
                    }
                    tsv  = rs.getLong( 5 );
                    cri = new ChangeRecordInfo(Long.valueOf(seqv), uuidv, buf, typv, tsv);
                    cri.setIsSelectAll(true);
                    records.add(cri);
                } catch (IOException e) {
                    IOException ex = getDBManager().wrapIOException(
                        "[" + selectAllSQL + "]", e );
                    logger.logStack( Logger.ERROR,
                        BrokerResources.X_PARSE_CONFIGRECORD_FAILED,
                        String.valueOf( seqv ), ex);
                    throw new BrokerException(ex.getMessage(), Status.PRECONDITION_FAILED);
                }
            }
            if (!foundreset) {
                throw new BrokerException(
                "Unexpected: shared database table "+getTableName()+" has no reset record",
                Status.PRECONDITION_FAILED);
            }
        } catch ( Exception e ) {
            myex = e;
            try {
                if ( (conn != null) && !conn.getAutoCommit() ) {
                    conn.rollback();
                }
            } catch ( SQLException rbe ) {
                logger.log( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
            }

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = getDBManager().wrapSQLException("[" + selectAllSQL + "]", (SQLException)e);
            } else {
                ex = e;
            }

            throw new BrokerException( br.getKString(
                br.X_SHARECC_QUERY_ALL_RECORDS_FAIL, ex.getMessage()), ex );
        } finally {
            if ( myConn ) {
                closeSQLObjects( rs, pstmt, conn, myex );
            } else {
                closeSQLObjects( rs, pstmt, null, myex );
            }
        }

        return records;
    }


    /**
     * Get the sequence number of the specified record
     *
     * @param conn database connection
     * @param uuid uid of the record 
     * @return null if no such record
     */
    private Long getSequenceByUUID( Connection conn, String uuid )
        throws BrokerException {

        Long seq = null;

        boolean myConn = false;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Exception myex = null;
        try {
            // Get a connection
            if ( conn == null ) {
                conn = getDBManager().getConnection( true );
                myConn = true;
            }

            pstmt = conn.prepareStatement( selectSeqByUUIDSQL );
            pstmt.setString( 1, uuid );
            rs = pstmt.executeQuery();
            if ( rs.next() ) {
                long seqv = rs.getLong( 1 );
                seq = new Long(seqv);
            }
        } catch ( Exception e ) {
            myex = e;
            try {
                if ( (conn != null) && !conn.getAutoCommit() ) {
                    conn.rollback();
                }
            } catch ( SQLException rbe ) {
                logger.log( Logger.ERROR, BrokerResources.X_DB_ROLLBACK_FAILED, rbe );
            }

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = getDBManager().wrapSQLException("[" + selectSeqByUUIDSQL + "]", (SQLException)e);
            } else {
                ex = e;
            }

            String[] args = new String[] { SEQ_COLUMN, String.valueOf(seq),
                                           ex.getMessage() };
            throw new BrokerException( br.getKString(
                br.X_SHARECC_QUERY_SEQ_BY_UUID_FAIL, args), ex );
        } finally {
            if ( myConn ) {
                closeSQLObjects( rs, pstmt, conn, myex );
            } else {
                closeSQLObjects( rs, pstmt, null, myex );
            }
        }

        return seq;
    }

    /**
     * Get debug information about the store.
     * @param conn database connection
     * @return a HashMap of name value pair of information
     */
    public HashMap getDebugInfo( Connection conn ) {

        HashMap map = new LinkedHashMap();
        StringBuffer buf = new StringBuffer();
        ArrayList<ChangeRecordInfo> records = null;
        try {
            records = getAllRecords( conn );
            Iterator<ChangeRecordInfo>  itr = records.iterator();
            ChangeRecordInfo rec = null;
            while (itr.hasNext()) {
                rec = itr.next();
                buf.append(rec.toString()).append("\n");
            }
        } catch ( Exception e ) {
            logger.log( Logger.ERROR, e.getMessage(), e.getCause() );
        }

        map.put("Cluster Config Change Records:\n", buf.toString());
        if (records != null) {
            map.put( "Count", records.size());
        }
        return map;
    }
}
