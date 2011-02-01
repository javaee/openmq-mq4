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
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.config.*;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.util.log.Logger;

/**
 * The MQAuditSession interface defines a generic interface for
 * generating audit records for important management events.
 * Underlying audit session implementation objects exporting this
 * interface to perform specific types of auditing.
 * <p>
 * An audit session is created by calling a get method in the
 * audit service factory, a static factory class which
 * creates platform specific implementation class instances of
 * the audit session based upon installation time configuration
 * parameters.
 *
 * @version    1.2
 */
public abstract class MQAuditSession {

    public static final int SUCCESS = 0;
    public static final int FAILURE = -1;
    public static final String LOCALHOSTIP = "127.0.0.1";
    public static final String LOCALHOST = "localhost";

    public static final String AUDIT_PREFIX = "AUDIT ";
    public static final String COMMA = ";";
    public static final String SUCCESS_STR = "success";
    public static final String FAILURE_STR = "failure";
    public static final String QUEUE = "queue";
    public static final String TOPIC = "topic";

    // prefix string literals
    public static final String MQ_INSTANCE = "MQ instance=";
    public static final String ACTION = "action=";
    public static final String MQ_ACTION = "MQ action=";
    public static final String USER = "user=";
    public static final String MQ_USER = "MQ user=";
    public static final String HOST = "host=";
    public static final String TYPE = "type=";
    public static final String NAME = "name=";
    public static final String OPERATION = "operation=";
    public static final String CLIENT_ID = "clientID=";

    // actions
    public static final String BROKER_STARTUP = "broker startup";
    public static final String BROKER_SHUTDOWN = "broker shutdown";
    public static final String BROKER_RESTART = "broker restart";
    public static final String AUTHENTICATION = "authentication";
    public static final String AUTHORIZATION = "authorization";
    public static final String REMOVE_INSTANCE = "remove instance";
    public static final String RESET_STORE = "reset store";
    public static final String CREATE_DESTINATION = "create destination";
    public static final String PURGE_DESTINATION = "purge destination";
    public static final String DESTROY_DESTINATION = "destroy destination";
    public static final String DESTROY_DURABLE = "destroy durable subscriber";

    // destination operations
    public static final String CREATE = "create";
    public static final String PRODUCE = "produce";
    public static final String CONSUME = "consume";
    public static final String BROWSE = "browse";

    protected BrokerConfig config = Globals.getConfig();
    protected Logger logger = Globals.getLogger();
    protected BrokerResources br = Globals.getBrokerResources();

    public boolean auditOn = false;

    protected String brokerHost = LOCALHOST;
    protected String instance = "???";

    /**
     * The isAuditOn method returns true if auditing has been enabled for
     * this session.
     * Checking if auditing has been enabled allows
     * applications to conditionally skip code that sets up and writes
     * audit events.  Some platforms require that the administrator
     * explicitly enable auditing.
     *
     * @return true if auditing is enabled for this session; false otherwise.
     */
    public boolean isAuditOn() {
	return auditOn;
    }

    public void setInstance(String name, String host, int port) {
	this.brokerHost = host;
	instance = name + "@" + host + ":" + port;
    }

    /**
     * Invoked post authentication.
     * @param user	user who is being authenticated
     * @param remoteHost host the user connects from
     * @param success	status of authentication
     */
    public abstract void authentication(String user, String host,
				boolean success);

    /**
     * Invoked for the following events:
     *   broker startup
     *   broker shutdown
     *   broker restart
     *   remove instance
     */
    public abstract void brokerOperation(String user, String host, String op);

    public abstract void connectionAuth(String user, String host, String type,
				String name, boolean success);

    public abstract void destinationAuth(String user, String host, String type,
				String name, String op, boolean success);

    public abstract void storeOperation(String user, String host, String op);

    public abstract void destinationOperation(
	String user, String host, String op, String type, String name);

    public abstract void durableSubscriberOperation(
	String user, String host, String op, String name, String clientID);
}
