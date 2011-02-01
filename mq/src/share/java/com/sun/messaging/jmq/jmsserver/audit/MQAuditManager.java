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

import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;

/**
 * The MQAuditManager takes care of writing audit logs for the broker.
 *
 * @version    1.2
 */
public class MQAuditManager extends MQAuditSession {

    private MQAuditSession logAuditSession = null;
    private MQAuditSession bsmAuditSession = null;

    MQAuditManager() {
	// if auditing is not licensed, do nothing
	if (!MQAuditService.AUDIT_LOGGING_LICENSED)
	    return;

	// if audit logging through broker log is enabled
	if (MQAuditService.logAuditEnabled) {
	    try {
		logAuditSession = MQAuditService.getAuditSession(
						MQAuditService.LOG_TYPE);
		auditOn = logAuditSession.isAuditOn();
	    } catch (BrokerException e) {
		logger.log(logger.ERROR,
			"Failed to get audit session to log audit records");
	    }
	}

	// if audit logging through BSM is enabled
	if (MQAuditService.bsmAudit) {
	    try {
		bsmAuditSession = MQAuditService.getAuditSession(
						MQAuditService.BSM_TYPE);
		auditOn = auditOn || bsmAuditSession.isAuditOn();
	    } catch (BrokerException e) {
		logger.log(logger.ERROR,
			"Failed to get audit session to log BSM audit records");
	    }
	}
    }

    public void setInstance(String name, String host, int port) {
	super.setInstance(name, host, port);

	if (logAuditSession != null) {
	    logAuditSession.setInstance(name, host, port);
	}
	if (bsmAuditSession != null) {
	    bsmAuditSession.setInstance(name, host, port);
	}
    }

    /**
     * Invoked post authentication.
     * @param user	user who is being authenticated
     * @param remoteHost host the user connects from
     * @param success	status of authentication
     */
    public void authentication(String user, String host, boolean success) {
	if (logAuditSession != null) {
	    logAuditSession.authentication(user, host, success);
	}
	if (bsmAuditSession != null) {
	    bsmAuditSession.authentication(user, host, success);
	}
    }

    public void brokerOperation(String user, String host, String op) {
	if (logAuditSession != null) {
	    logAuditSession.brokerOperation(user, host, op);
	}
	if (bsmAuditSession != null) {
	    bsmAuditSession.brokerOperation(user, host, op);
	}
    }

    public void connectionAuth(
	String user, String host, String type, String name, boolean success) {
	if (logAuditSession != null) {
	    logAuditSession.connectionAuth(user, host, type, name, success);
	}
	if (bsmAuditSession != null) {
	    bsmAuditSession.connectionAuth(user, host, type, name, success);
	}
    }

    public void destinationAuth(String user, String host, String type,
	String name, String op, boolean success) {
	if (logAuditSession != null) {
	    logAuditSession.destinationAuth(user, host, type, name, op, success);
	}
	if (bsmAuditSession != null) {
	    bsmAuditSession.destinationAuth(user, host, type, name, op, success);
	}
    }

    public void storeOperation(String user, String host, String op) {
	if (logAuditSession != null) {
	    logAuditSession.storeOperation(user, host, op);
	}
	if (bsmAuditSession != null) {
	    bsmAuditSession.storeOperation(user, host, op);
	}
    }

    public void destinationOperation(
	String user, String host, String op, String type, String name) {
	if (logAuditSession != null) {
	    logAuditSession.destinationOperation(user, host, op, type, name);
	}
	if (bsmAuditSession != null) {
	    bsmAuditSession.destinationOperation(user, host, op, type, name);
	}
    }

    public void durableSubscriberOperation(
	String user, String host, String op, String name, String clientID) {
	if (logAuditSession != null) {
	    logAuditSession.durableSubscriberOperation(user, host, op, name,
	    						clientID);
	}
	if (bsmAuditSession != null) {
	    bsmAuditSession.durableSubscriberOperation(user, host, op, name,
	    						clientID);
	}
    }
}
