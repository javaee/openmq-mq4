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
 * @(#)DestinationLogHandler.java	1.9 06/29/07
 */ 

package com.sun.messaging.jmq.jmsserver.util;

import com.sun.messaging.jmq.util.log.*;
import com.sun.messaging.jmq.util.*;
import com.sun.messaging.jmq.io.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.Broker;
import com.sun.messaging.jmq.jmsserver.BrokerStateHandler;
import com.sun.messaging.jmq.jmsserver.service.*;
import com.sun.messaging.jmq.jmsserver.config.ConfigListener;
import com.sun.messaging.jmq.jmsserver.config.PropertyUpdateException;
import com.sun.messaging.jmq.jmsserver.config.BrokerConfig;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import java.util.*;
import java.io.*;
import java.net.*;


/**
 * A LogHandler that logs to a JMS destination
 */
public class DestinationLogHandler extends LogHandler {

    static boolean open = false;

    // XXX REVISIT 6/20/2003 dipol. Should be configureable
    private static int DESTINATION_BEHAVIOR = DestLimitBehavior.REMOVE_OLDEST;
    private String topic = "";
    private Destination destination = null;
    private boolean persist = false;
    private int     ttl = 300; // in seconds
    private int     capacity = 100; // in seconds

    public DestinationLogHandler() {
    }

    /**
     * Configure DestinationLogHandler with the values contained in
     * the passed Properties object. 
     * <P>
     * An example of valid properties are:
     * <PRE>
     * imq.log.destination.topic=mq.log.broker
     * imq.log.destination.output=ERROR|WARNING
     * imq.log.destination.timetolive=300
     * imq.log.destination.persist=false
     * </PRE>
     * In this case prefix would be "imq.log.destination"
     *
     * @param props	Properties to get configuration information from
     * @param prefix	String that this handler's properties are prefixed with
     *
     * @throws IllegalArgumentException if one or more property values are
     *                                  invalid. All valid properties will
     *					still be set.
     */
    public synchronized void configure(Properties props, String prefix)
	throws IllegalArgumentException {

	String value = null;
	String property = null;
	String error_msg = null;

	prefix = prefix + ".";

	property = prefix + "capacity";
	if ((value = props.getProperty(property)) != null) {
            try {
            	capacity = Integer.parseInt(value);
	    } catch (NumberFormatException e) {
		error_msg = rb.getString(rb.W_BAD_NFORMAT, property, value);
            }
	}

	property = prefix + "topic";
	if ((value = props.getProperty(property)) != null) {
            topic = value;
	}

	property = prefix + "timetolive";
	if ((value = props.getProperty(property)) != null) {
            try {
            	ttl = Integer.parseInt(value);
	    } catch (NumberFormatException e) {
		error_msg = rb.getString(rb.W_BAD_NFORMAT, property, value);
            }
	}

	property = prefix + "persist";
	if ((value = props.getProperty(property)) != null) {
            persist = value.equals("true");
	}

	property = prefix + "output"; 
	if ((value = props.getProperty(property)) != null) {
	    try {
	        setLevels(value);
	    } catch (IllegalArgumentException e) {
	        error_msg = (error_msg != null ? error_msg + "\n" : "") +
			property + ": " + e.getMessage();
	    }
        } 

        if (error_msg != null) {
            throw new IllegalArgumentException(error_msg);
        }


        if (open) {
            this.close();
        }

        // Causes prop changes to take effect
        this.open();
    }

    /**
     * Publish string to log
     *
     * @param level	Log level to use
     * @param message	Message to write to log file
     *
     */
    public void publish(int level, String message) throws IOException {
    	
		// ignore FORCE messages if we have explicitly been asked to ignore them
		if (level == Logger.FORCE && !isAllowForceMessage()) {
			return;
		}

        if (!open) {
            return;
        }

        // Can't publish messages if we are shutting down
        if (BrokerStateHandler.shuttingDown) {
            return;
        }

//System.out.println("*****publish(" + topic + "): " + message );
        // We only publish messages if there are consumers. The loggin
        // destination is typically autocreated. If there are no consumers
        // it will go away.
        try {
            // Get the destination we are going to publish to
            destination = Destination.getLoadedDestination(topic, false);
            if (destination == null) {
                // No destination means no consumers
//System.out.println("******No destination");
                return;
            }

            if (destination.getCapacity() != capacity) {
                destination.setCapacity(capacity);
            }

            if (destination.getLimitBehavior() != DESTINATION_BEHAVIOR) {
                destination.setLimitBehavior(DESTINATION_BEHAVIOR);
            }
        } catch (Exception e) {
            IOException e2 = new IOException(
                    "Could not get or configure logging destination \"" +
                        topic + "\". Closing destination logger: " + e);
            e2.initCause(e);
            this.close();
            throw e2;
        }

        // Only send message if there are consumers
        if (destination.getActiveConsumerCount() <= 0) {
//System.out.println("******No consumers");
            return;
        }

        Hashtable props = new Hashtable();
        Hashtable body = new Hashtable();

        long curTime = System.currentTimeMillis();

        PortMapper pm = Globals.getPortMapper();

        int port = 0;

        if (pm != null) {
            port = pm.getPort();
        }

        props.put("broker",  Globals.getMQAddress().getHostName() + ":" + port);
        props.put("brokerInstance", Globals.getConfigName() );
        props.put("type", topic );
        props.put("timestamp", new Long(curTime) );

        body.put("level", Logger.levelIntToStr(level));
        body.put("text", message);

        try {

        Packet pkt = new Packet(false);
        pkt.setProperties(props);
        pkt.setPacketType(PacketType.MAP_MESSAGE);
	pkt.setDestination(topic);
	pkt.setPriority(5);
	pkt.setIP(Globals.getBrokerInetAddress().getAddress());
	pkt.setPort(port);
	pkt.updateSequenceNumber();
	pkt.updateTimestamp();
	pkt.generateSequenceNumber(false);
	pkt.generateTimestamp(false);

	pkt.setIsQueue(false);
	pkt.setTransactionID(0);
	pkt.setSendAcknowledge(false);
	pkt.setPersistent(persist);
	pkt.setExpiration(ttl == 0 ? (long)0
			   : (curTime + ttl));

        ByteArrayOutputStream byteArrayOutputStream = 
                    new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = 
                    new ObjectOutputStream(byteArrayOutputStream);

        objectOutputStream.writeObject(body);
        objectOutputStream.flush();
        byteArrayOutputStream.flush();

        byte[] messageBody = byteArrayOutputStream.toByteArray();

        objectOutputStream.close();
        byteArrayOutputStream.close();
        pkt.setMessageBody(messageBody);

        PacketReference ref = PacketReference.createReference(pkt, null);

        destination.queueMessage(ref, false);
        Set s =destination.routeNewMessage(ref);
        destination.forwardMessage(s, ref);

        } catch (Exception e) {
            // Make sure we close so we don't recurse!
            this.close();
            // XXX L10N 6/20/2003 dipol
            Globals.getLogger().log(Logger.ERROR,
                "Destination logger: Can't log to destination: " + topic,
                e);
            Globals.getLogger().log(Logger.ERROR,
                "Closing destination logger.");
        }
    }


    /**
     * Open handler
     */
    public void open() {
       
        synchronized(getClass()) {
            if (!open) {
                open = true;
            }
        }
    }

    /**
     * Close handler
     */
    public void close() {
        synchronized(getClass()) {
            if (open) {
                open = false;
            }
        }
    }

    /**
     * Return a string description of this Handler. The descirption
     * is the class name followed by the destination we are logging to
     */
    public String toString() {
	return this.getClass().getName() + ":" + topic;
    }
}
