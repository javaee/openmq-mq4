/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2010 Oracle and/or its affiliates. All rights reserved.
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

package com.sun.messaging.jmq.jmsserver.persist.file;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;
import com.sun.messaging.jmq.jmsserver.data.TransactionWorkMessage;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.util.WaitTimeoutException;
import com.sun.messaging.jmq.util.log.Logger;

/**
 * @author gsivewright
 * 
 * This class is used to keep track of which messages have been logged to the
 * transaction log since the last checkpoint.
 * 
 * This info is required to check if a message removal event also needs to be
 * logged.
 * 
 * An unlogged message removal will not be replayed and can result in a
 * duplicate.
 * 
 */
public class LoggedMessageHelper {

	public static final Logger logger = Globals.getLogger();

	Map<SysMessageID, SysMessageID> loggedSendsSinceLastCheckpoint = 
		new ConcurrentHashMap<SysMessageID, SysMessageID>();
	
	Set<SysMessageID> pendingRemove = Collections
			.synchronizedSet(new HashSet<SysMessageID>());
	
	TransactionLogManager txnLogManager;

	public LoggedMessageHelper(TransactionLogManager txnLogManager) {
		this.txnLogManager = txnLogManager;
	}

	public void preMessageRemoved(DestinationUID dstID, SysMessageID mid)
			throws BrokerException {
		if (Store.getDEBUG()) {
			String msg = getPrefix() + " preMessageRemoved() dest=" + dstID
					+ " mid=" + mid;
			logger.log(Logger.DEBUG, msg);
		}
		// we may need to log this event if:
		// (a) message send for this message was logged after previous
		// checkpoint
		// (b) last ack of this message has not already been logged

		// so we don't need to log if log Globals.logNonTransactedMsgAck()
		// ==true
		if (!TransactionLogManager.logNonTransactedMsgAck) {

			if (loggedSendsSinceLastCheckpoint.containsKey(mid)) {
				txnLogManager.logMsgRemoval(dstID, mid);
			}
		}
		
		// we have logged th eremoval of this message so no need 
		// to keep track of logged send  for it
		loggedSendsSinceLastCheckpoint.remove(mid);

	}
	
	public void postMessageRemoved(DestinationUID dstID, SysMessageID mid)
			throws BrokerException {
		synchronized (pendingRemove) {
			Object found = pendingRemove.remove(mid);
			
			// this method is called on ALL message removes
			// so may not find a match
			
			if (found != null) {
				if (Store.getDEBUG()) {
					String msg = getPrefix() + " postMessageRemoved() dest="
							+ dstID + " id=" + mid + " pendingRemoves="
							+ pendingRemove.size();
					logger.log(Logger.DEBUG, msg);
				}

				if (pendingRemove.size() == 0)
					pendingRemove.notify();
			}
		}
	}

	public void lastAckLogged(DestinationUID dst, SysMessageID id) {
		pendingRemove.add(id);
		if (Store.getDEBUG()) {
			String msg = getPrefix() + " lastAckLogged() dest=" + dst + " id="
					+ id + " pendingRemoves="+pendingRemove.size();
			logger.log(Logger.DEBUG, msg);
		}
		loggedSendsSinceLastCheckpoint.remove(id);
	}
	

	public void waitForPendingRemoveCompletion(boolean nowait) 
        throws WaitTimeoutException {
		synchronized (pendingRemove) {
			if (Store.getDEBUG()) {
				String msg = getPrefix() + " num pendingRemove ="
						+ pendingRemove.size();
				logger.log(Logger.DEBUG, msg);
			}
			try {
				while (pendingRemove.size() > 0) {
					if (Store.getDEBUG()) {
						String msg = getPrefix() + " waiting for "
								+ pendingRemove.size()
								+ " pendingRemove";
						logger.log(Logger.DEBUG, msg);
					}
                                        if (nowait) {
                                            throw new WaitTimeoutException(this.getClass().getSimpleName());
                                        }
					pendingRemove.wait(1000);
				}
                                txnLogManager.notifyPlayToStoreCompletion();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	void messageListLogged(List<TransactionWorkMessage> twms) {
		Iterator<TransactionWorkMessage> iter = twms.iterator();
		while (iter.hasNext()) {
			TransactionWorkMessage twm = iter.next();
			messageLogged(twm);
		}

	}

	public void messageLogged(TransactionWorkMessage twm) {
		if (Store.getDEBUG()) {
			String msg = getPrefix() + " messageLogged()" + twm;
			logger.log(Logger.DEBUG, msg);
		}
		SysMessageID id = twm.getMessage().getSysMessageID();
		loggedSendsSinceLastCheckpoint.put(id, id);
	}

	/**
	 * on checkpoint we can clear out list of logged messages as they will not
	 * be replayed.
	 */
	public void onCheckpoint() {
		loggedSendsSinceLastCheckpoint.clear();
	}

	String getPrefix() {
		return "LoggedMessageHelper: " + Thread.currentThread().getName();
	}

}
