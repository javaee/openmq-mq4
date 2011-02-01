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
 * @(#)OracleMessageDAOImpl.java	1.16 06/29/07
 */ 

package com.sun.messaging.jmq.jmsserver.persist.jdbc;

import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.Status;

import java.sql.*;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 * This class implement Oracle MessageDAO.
 */
class OracleMessageDAOImpl extends MessageDAOImpl {

    /**
     * Constructor
     * @throws com.sun.messaging.jmq.jmsserver.util.BrokerException
     */
    OracleMessageDAOImpl() throws BrokerException {

        super();

        // Initialize message column with an "empty" BLOB
        insertSQL = new StringBuffer(128)
            .append( "INSERT INTO " ).append( tableName )
            .append( " ( " )
            .append( ID_COLUMN ).append( ", " )
            .append( MESSAGE_SIZE_COLUMN ).append( ", " )
            .append( STORE_SESSION_ID_COLUMN ).append( ", " )
            .append( DESTINATION_ID_COLUMN ).append( ", " )
            .append( TRANSACTION_ID_COLUMN ).append( ", " )
            .append( CREATED_TS_COLUMN ).append( ", " )
            .append( MESSAGE_COLUMN )
            .append( ") VALUES ( ?, ?, ?, ?, ?, ?, EMPTY_BLOB() )" )
            .toString();

        // Blob column need to be update separately
        updateDestinationSQL = new StringBuffer(128)
            .append( "UPDATE " ).append( tableName )
            .append( " SET " )
            .append( DESTINATION_ID_COLUMN ).append( " = ?, " )
            .append( MESSAGE_SIZE_COLUMN ).append( " = ? " )
            .append( " WHERE " )
            .append( ID_COLUMN ).append( " = ?" )
            .toString();
    }

    /**
     * Insert a new entry.
     * @param conn database connection
     * @param message the message to be persisted
     * @param dstID the destination
     * @param conUIDs an array of interest ids whose states are to be
     *      stored with the message
     * @param states an array of states
     * @param storeSessionID the store session ID that owns the msg
     * @param createdTime timestamp
     * @param checkMsgExist check if message & destination exist in the store
     * @exception com.sun.messaging.jmq.jmsserver.util.BrokerException if a message with the same id exists
     *  in the store already
     */
    public void insert( Connection conn, String dstID, Packet message,
        ConsumerUID[] conUIDs, int[] states, long storeSessionID, long createdTime,
        boolean checkMsgExist ) throws BrokerException {

        boolean myConn = false;
        PreparedStatement pstmt = null;
        Exception myex = null;
        try {
            // Get a connection
            DBManager dbMgr = DBManager.getDBManager();

            // Verify that we're using an Oracle driver because we're using
            // Oracle Extensions for LOBs.
            if ( ! dbMgr.isOracleDriver() ) {
                // Try generic implementation
                super.insert( conn, dstID, message, conUIDs, states, storeSessionID, createdTime, checkMsgExist );
                return;
            }

            if ( conn == null ) {
                conn = dbMgr.getConnection( false );
                myConn = true;
            }

            SysMessageID sysMsgID = (SysMessageID)message.getSysMessageID();
            String id = sysMsgID.getUniqueName();
            int size = message.getPacketSize();
            long txnID = message.getTransactionID();

            if ( dstID == null ) {
                dstID = DestinationUID.getUniqueString(
                    message.getDestination(), message.getIsQueue() );
            }

            if ( checkMsgExist ) {
                if ( hasMessage( conn, id ) ) {
                    throw new BrokerException(
                        br.getKString(BrokerResources.E_MSG_EXISTS_IN_STORE, id, dstID ) );
                }

                dbMgr.getDAOFactory().getDestinationDAO().checkDestination( conn, dstID );
            }

            String sql = insertSQL;
            try {
                pstmt = conn.prepareStatement( sql );
                pstmt.setString( 1, id );
                pstmt.setInt( 2, size );
                pstmt.setLong( 3, storeSessionID );
                pstmt.setString( 4, dstID );
                Util.setLong( pstmt, 5, (( txnID == 0 ) ? -1 : txnID) );
                pstmt.setLong( 6, createdTime );
                pstmt.executeUpdate();
                pstmt.close();

                // Obtain a "handle" to the BLOB for the message column
                sql = selectForUpdateSQL;
                pstmt = conn.prepareStatement( sql );
                pstmt.setString( 1, id );
                ResultSet rs = pstmt.executeQuery();
                rs.next();
                Blob blob = rs.getBlob(1);
                rs.close();
                pstmt.close();

                // Write out the message using the BLOB locator
                OutputStream bos = Util.OracleBLOB_getBinaryOutputStream( blob );
                message.writePacket( bos );
                bos.close();

                // Store the consumer's states if any
                if ( conUIDs != null ) {
                    dbMgr.getDAOFactory().getConsumerStateDAO().insert(
                        conn, dstID, sysMsgID, conUIDs, states, false );
                }

                // Commit all changes
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

                Exception ex;
                if ( e instanceof BrokerException ) {
                    throw (BrokerException)e;
                } else if ( e instanceof IOException ) {
                    ex = DBManager.wrapIOException("[" + insertSQL + "]", (IOException)e);
                } else if ( e instanceof SQLException ) {
                    ex = DBManager.wrapSQLException("[" + insertSQL + "]", (SQLException)e);
                } else {
                    ex = e;
                }

                throw new BrokerException(
                    br.getKString( BrokerResources.X_PERSIST_MESSAGE_FAILED,
                    id ), ex );
            }
        } catch (BrokerException e) {
            myex = e;
            throw e;
        } finally {
            if ( myConn ) {
                Util.close( null, pstmt, conn, myex );
            } else {
                Util.close( null, pstmt, null, myex );
            }
        }
    }

    /**
     * Move a message to another destination.
     * @param conn database connection
     * @param message the message
     * @param fromDst the destination
     * @param toDst the destination to move to
     * @param conUIDs an array of interest ids whose states are to be
     *      stored with the message
     * @param states an array of states
     * @throws IOException
     * @throws BrokerException
     */
    public void moveMessage( Connection conn, Packet message,
        DestinationUID fromDst, DestinationUID toDst, ConsumerUID[] conUIDs,
        int[] states ) throws IOException, BrokerException {

	SysMessageID sysMsgID = (SysMessageID)message.getSysMessageID().clone();
        String id = sysMsgID.getUniqueName();
        int size = message.getPacketSize();

        boolean myConn = false;
        PreparedStatement pstmt = null;
        String sql = selectForUpdateSQL;
        Exception myex = null;
        try {
            // Get a connection
            DBManager dbMgr = DBManager.getDBManager();

            // Verify that we're using an Oracle driver because we're using
            // Oracle Extensions for LOBs.
            if ( ! dbMgr.isOracleDriver() ) {
                // Try generic implementation
                super.moveMessage( conn, message, fromDst, toDst, conUIDs, states );
                return;
            }

            if ( conn == null ) {
                conn = dbMgr.getConnection( false );
                myConn = true;
            }

            // Obtain a "handle" to the BLOB for the message column
            pstmt = conn.prepareStatement( sql );
            pstmt.setString( 1, id );
            ResultSet rs = pstmt.executeQuery();

            if ( !rs.next() ) {
                // We're assuming the entry does not exist
                throw new BrokerException(
                    br.getKString( BrokerResources.E_MSG_NOT_FOUND_IN_STORE,
                        id, fromDst ), Status.NOT_FOUND );
            }

            Blob blob = rs.getBlob(1);
            rs.close();
            pstmt.close();

            // Write out the message using the BLOB locator
            OutputStream bos = Util.OracleBLOB_getBinaryOutputStream( blob );
            message.writePacket( bos );
            bos.close();

            sql = updateDestinationSQL;
            pstmt = conn.prepareStatement( sql );
            pstmt.setString( 1, toDst.toString() );
            pstmt.setInt( 2, size );
            pstmt.setString( 3, id );
            pstmt.executeUpdate();

            /**
             * Update consumer states:
             * 1. remove the old states
             * 2. re-insert the states
             */
            ConsumerStateDAO conStateDAO = dbMgr.getDAOFactory().getConsumerStateDAO();
            conStateDAO.deleteByMessageID( conn, sysMsgID );
            if ( conUIDs != null || states != null ) {
                conStateDAO.insert(
                    conn, toDst.toString(), sysMsgID, conUIDs, states, false );
            }

            // Commit all changes
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

            Exception ex;
            if ( e instanceof BrokerException ) {
                throw (BrokerException)e;
            } else if ( e instanceof SQLException ) {
                ex = DBManager.wrapSQLException("[" + updateDestinationSQL + "]", (SQLException)e);
            } else {
                ex = e;
            }

            Object[] args = { id, fromDst, toDst };
            throw new BrokerException(
                br.getKString( BrokerResources.X_MOVE_MESSAGE_FAILED,
                args ), ex );
        } finally {
            if ( myConn ) {
                Util.close( null, pstmt, conn, myex );
            } else {
                Util.close( null, pstmt, null, myex );
            }
        }
    }
}
