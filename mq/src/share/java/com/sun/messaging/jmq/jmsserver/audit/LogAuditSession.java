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

package com.sun.messaging.jmq.jmsserver.audit;

import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.Globals;

/**
 * The LogAuditSession logs audit records in the broker log file.
 *
 * @version    1.3
 */
public class LogAuditSession extends MQAuditSession {

    LogAuditSession() {
	auditOn = MQAuditService.AUDIT_LOGGING_LICENSED &&
			MQAuditService.logAuditEnabled;

	if (auditOn) {
	    // force log level to at least INFO
	    // if not already set lower than that
	    String value = Globals.getConfig().getProperty(
				Globals.IMQ + ".log.level", "INFO");
	    int setAt = Logger.INFO;
	    try {
		setAt = Logger.levelStrToInt(value);
	    } catch (IllegalArgumentException e) {
		setAt = Logger.INFO;
	    }
	    if (setAt > Logger.INFO) {
		logger.updateProperty("imq.log.level", "INFO");
	    }
	}
	logger.log(logger.DEBUG, this + ":auditOn="+auditOn);
    }

    /**
     * Invoked post authentication.
     * @param user	user who is being authenticated
     * @param remoteHost host the user connects from
     * @param success	status of authentication
     */
    public void authentication(String user, String host, boolean success) {
	if (!auditOn)
	    return;

	String status = success ? SUCCESS_STR : FAILURE_STR;
	logger.log(logger.INFO, AUDIT_PREFIX + ACTION + AUTHENTICATION
			+ COMMA + USER + user + COMMA + HOST + host
			+ COMMA + status);
    }

    public void brokerOperation(String user, String host, String op) {
	if (!auditOn)
	    return;

	logger.log(logger.INFO, AUDIT_PREFIX + ACTION + op
			+ COMMA + USER + user + COMMA + HOST
			+ (host == null ? brokerHost : host)
			+ COMMA + SUCCESS_STR);
    }

    public void connectionAuth(
	String user, String host, String type, String name, boolean success) {

	if (!auditOn)
	    return;

	String status = success ? SUCCESS_STR : FAILURE_STR;
	logger.log(logger.INFO, AUDIT_PREFIX + ACTION + AUTHORIZATION
			+ COMMA + USER + user + COMMA + HOST + host
			+ COMMA + TYPE + type + COMMA + NAME + name
			+ COMMA + status);
    }

    public void destinationAuth(String user, String host, String type,
	String name, String op, boolean success) {

	if (!auditOn)
	    return;

	String status = success ? SUCCESS_STR : FAILURE_STR;
	logger.log(logger.INFO, AUDIT_PREFIX + ACTION + AUTHORIZATION
			+ COMMA + USER + user + COMMA + HOST + host
			+ COMMA + TYPE + type + COMMA + NAME + name
			+ COMMA + OPERATION + op + COMMA + status);
    }

    public void storeOperation(String user, String host, String op) {

	if (!auditOn)
	    return;

	logger.log(logger.INFO, AUDIT_PREFIX + ACTION + op
			+ COMMA + USER + user + COMMA + HOST
			+ (host == null ? brokerHost : host)
			+ COMMA + SUCCESS_STR);
    }

    public void destinationOperation(
	String user, String host, String op, String type, String name) {

	if (!auditOn)
	    return;

	logger.log(logger.INFO, AUDIT_PREFIX + ACTION + op
			+ COMMA + USER + user + COMMA + HOST + host
			+ COMMA + TYPE + type + COMMA + NAME + name
			+ COMMA + SUCCESS_STR);
    }

    public void durableSubscriberOperation(
	String user, String host, String op, String name, String clientID) {

	if (!auditOn)
	    return;

	logger.log(logger.INFO, AUDIT_PREFIX + ACTION + op
			+ COMMA + USER + user + COMMA + HOST + host
			+ COMMA + NAME + name + COMMA + CLIENT_ID + clientID
			+ COMMA + SUCCESS_STR);
    }
}
