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
 * @(#)TransactionProcessing.java	1.4 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.data;

import com.sun.messaging.jmq.io.Status;
import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.util.JMQXid;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;
import com.sun.messaging.jmq.util.UID;
import com.sun.messaging.jmq.util.CacheHashMap;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;
import com.sun.messaging.jmq.util.log.Logger;

/**
 * This class handled transaction processing and requeing
 */

//
// NOTE: 
// ultimately, all txn processing should be moved to this file
// however it is too close to the beta freeze for me to do that
// it will be looked after beta or in the next release
//
public class TransactionProcessing
{
    TransactionList translist = Globals.getTransactionList();
    Logger logger = Globals.getLogger();


    public void checkState(BrokerAddress bkr)
    {
        // checks all indoubt transactions
    }

    public void addRemoteTxn(BrokerAddress bkr, TransactionUID txnid)
    {
    }

    public void removeRemoteTxn(BrokerAddress bkr, TransactionUID txnid)
    {
    }

    public void checkLocalTransaction(TransactionUID txnid)
    {
        // ok if its not prepared, rollback
        // if its rolledback ... clean up all local acks
        // if its committed .. clean up all local messages
        // see if we can remote the txn -> must have no
        // txnids and no messages
        // if not start to watch it
    }

    public void handleLocalRollback(TransactionUID txnid)
    {
        // call doCommit for now
    }

    public void handleLocalCommit(TransactionUID txnid)
    {
        // call doRollback for now
    }

    public void handleRemoteRollback(TransactionUID txnid)
    {
        // logic from MultibrokerRouter
    }

    public void handleRemoteCommit(TransactionUID txnid)
    {
        // logic from MultibrokerRouter
    }

    public boolean watchLocalTransaction(TransactionUID txnid)
    {
        // wake up and see if the txn is complete
        // return true if it is, false otherwise
        return false;
    }

    public void checkRemoteTransaction(TransactionUID txnid)
        throws BrokerException
    {
        // if not in list, throw GONE
        TransactionState ts = translist.retrieveState(txnid);
        if (ts == null) {
            throw new BrokerException(
                 BrokerResources.E_INTERNAL_BROKER_ERROR,
		"Unknown TXN", null,
                 Status.GONE);
        }

        // ok if rolled back, handle it differently
        // depending whether its local or remote

    }



}
