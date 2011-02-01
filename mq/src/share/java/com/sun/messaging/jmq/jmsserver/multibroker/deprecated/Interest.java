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
 * @(#)Interest.java	1.8 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.multibroker;

import java.io.*;
import java.util.*;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.Subscription;

/**
 * This class represents a client interest.
 * @deprecated since 3.5
 */
public class Interest implements  Serializable
{
    /*
     * Added serialVersionUID for compatibility with iMQ 2.0.
     *
     * Note - The JDK 1.3 compiler generates a different
     * serialVersionUID for this class than prior JDK versions.
     * This can lead to nasty interoperability / compatibility
     * problems. Until this issue is fully resolved by JDK folks,
     * we should stick to JDK 1.2 compiler.
     */
    public static final long serialVersionUID = 99353142765567461L;

    /**
     * Interest identifier that is unique across the cluster.
     */
    // auto converted
    protected com.sun.messaging.jmq.jmsserver.core.ConsumerUID id = null;

    /**
     * JMS durable name.
     */
    protected String durableName = null;

    /**
     * JMS client id.
     */
    protected String clientID = null;

    /**
     * Name of the destination.
     */
    protected String destName = null;

    /**
     * Optional selector string specified by the client application.
     */
    protected String selstr = null;

    /**
     * This flag is <code> true </code> if a durable subscription is
     * currently in 'attached' stated. It tells the broker whether
     * to forward a message to this interest right away or hold it
     * until somebody attaches to this durable subscription.
     */
    protected boolean consumerReady = true;

    /**
     * flag which determines if messages should be sent to interests
     * on the same connection.
     */
    protected boolean noLocalDelivery = false;

    /**
     * Is this a queue interest or topic interest.
     */
    protected boolean isQueue = false;


    /**
     * Address of the broker directly connected to the client.
     */
    private BrokerAddress brokerAddr;


    public Object readResolve() throws ObjectStreamException {

        try {
            DestinationUID duid = DestinationUID.getUID(destName,
                   isQueue);
            com.sun.messaging.jmq.jmsserver.core.Subscription 
                 obj = Subscription.subscribe(durableName,
                     clientID, selstr, duid,
                     noLocalDelivery, false, false);
            obj.setConsumerUID(id);
            Destination d = Destination.getDestination(duid);
            d.addConsumer(obj, true);
            Subscription.clearSubscriptions();            
            return obj;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }

    } 

}

/*
 * EOF
 */
