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
 * @(#)GetDestinationsHandler.java	1.39 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.data.handlers.admin;

import java.util.Hashtable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Vector;
import java.util.Iterator;
import java.util.List;

import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.jmsserver.data.PacketRouter;
import com.sun.messaging.jmq.util.DestType;
import com.sun.messaging.jmq.util.SizeString;
import com.sun.messaging.jmq.io.*;
import com.sun.messaging.jmq.util.admin.MessageType;
import com.sun.messaging.jmq.util.admin.ConnectionInfo;
import com.sun.messaging.jmq.util.admin.DestinationInfo;
import com.sun.messaging.jmq.util.admin.ConsumerInfo;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.Consumer;
import com.sun.messaging.jmq.jmsserver.core.Producer;
import com.sun.messaging.jmq.jmsserver.core.DestinationUID;

public class GetDestinationsHandler extends AdminCmdHandler
{
    private static boolean DEBUG = getDEBUG();

    public GetDestinationsHandler(AdminDataHandler parent) {
    super(parent);
    }

    /**
     * Handle the incomming administration message.
     *
     * @param con    The Connection the message came in on.
     * @param cmd_msg    The administration message
     * @param cmd_props The properties from the administration message
     */
    public boolean handle(IMQConnection con, Packet cmd_msg,
                       Hashtable cmd_props) {

        if ( DEBUG ) {
            logger.log(Logger.DEBUG, this.getClass().getName() + ": " +
                            "Getting destinations: " + cmd_props);
        }


        Vector v = new Vector();
        int status = Status.OK;
        String errMsg = null;

        String destination = (String)cmd_props.get(MessageType.JMQ_DESTINATION);
        Integer destType = (Integer)cmd_props.get(MessageType.JMQ_DEST_TYPE);

        assert destination == null || destType != null;

        if (destination != null) {
            try {
                Destination d= Destination.getDestination(destination,
                          DestType.isQueue(destType.intValue()));

                if (d != null) {
                    if (DEBUG) {
                        d.debug();
                    }

                    v.add(getDestinationInfo(d));
                } else {
                    errMsg= rb.getString( rb.X_DESTINATION_NOT_FOUND, 
                               destination);
                    status = Status.NOT_FOUND;
                }
            } catch (Exception ex) {
                logger.log(Logger.ERROR,"Internal Error ", ex);
                assert false;
            }
        } else {
                // Get info on ALL destinations
    
           Iterator itr = Destination.getAllDestinations();
     
           while (itr.hasNext()) {
               Destination d = (Destination)itr.next();
               DestinationInfo dd= getDestinationInfo(d);
               v.add(dd);
           } 
       }

       // Send reply
       Packet reply = new Packet(con.useDirectBuffers());
       reply.setPacketType(PacketType.OBJECT_MESSAGE);
   
       setProperties(reply, MessageType.GET_DESTINATIONS_REPLY,
           status, errMsg);
   
       setBodyObject(reply, v);
       parent.sendReply(con, cmd_msg, reply);

       return true;
   }


    public static DestinationInfo getDestinationInfo(Destination d) {
        DestinationInfo di = new DestinationInfo();
        di.nMessages=d.size();
        di.nMessageBytes=d.byteSize();
        di.nConsumers=d.getConsumerCount();
        di.nfConsumers=d.getFailoverConsumerCount();
        di.naConsumers=d.getActiveConsumerCount();
        di.nProducers= d.getProducerCount();
        di.autocreated=(d.isAutoCreated() || d.isInternal() || d.isDMQ()
                        || d.isAdmin());
        di.destState=d.getState();
        di.name=d.getDestinationName();
        di.type=d.getType() &
                ~(DestType.DEST_INTERNAL | DestType.DEST_AUTO | DestType.DEST_ADMIN);

        di.fulltype=d.getType();
        di.maxMessages=d.getCapacity();
        if (di.maxMessages < 0)
            di.maxMessages = 0;
        SizeString bc = d.getByteCapacity();
        di.maxMessageBytes=(bc == null ? 0 : bc.getBytes());
        if (di.maxMessageBytes < 0)
            di.maxMessageBytes = 0;
        bc = d.getMaxByteSize();
        di.maxMessageSize=(bc == null ? 0 : bc.getBytes());
        if (di.maxMessageSize < 0)
            di.maxMessageSize = 0;
        di.destScope=d.getScope();
        di.destLimitBehavior=d.getLimitBehavior();
        di.maxPrefetch=d.getMaxPrefetch();
        di.destCDP=d.getClusterDeliveryPolicy();
        di.maxActiveConsumers=d.getMaxActiveConsumers();
        di.maxFailoverConsumers=d.getMaxFailoverConsumers();
        di.maxProducers = d.getMaxProducers();
        di.maxNumSharedConsumers = d.getMaxNumSharedConsumers();
        di.sharedConsumerFlowLimit = d.getSharedConsumerFlowLimit();
        di.useDMQ = d.getUseDMQ();
        di.nUnackMessages = d.getUnackSize();
        di.nTxnMessages = d.txnSize();
        di.nTxnMessageBytes = d.txnByteSize();
        di.validateXMLSchemaEnabled = d.validateXMLSchemaEnabled();
        di.XMLSchemaUriList = d.getXMLSchemaUriList();
        di.reloadXMLSchemaOnFailure = d.reloadXMLSchemaOnFailure();
        di.nRemoteMessages = d.getRemoteSize();
        di.nRemoteMessageBytes = d.getRemoteBytes();

	if (!d.isQueue())  {
	    Hashtable<String, Integer> h = new Hashtable<String, Integer>();

	    if (di.nConsumers > 0)  {
		Iterator consumers = d.getConsumers();

		while (consumers.hasNext())  {
		    Consumer oneCon = (Consumer)consumers.next();

		    if (oneCon.isWildcard())  {
			DestinationUID id = oneCon.getDestinationUID();
			String wildcard = id.getName();

			Integer count = h.get(wildcard), newCount;

			if (count == null)  {
			    newCount = new Integer(1);
			} else  {
			    newCount = new Integer(count.intValue() + 1);
			}
			h.put(wildcard, newCount);
		    }
		}
	    }
	    if (h.size() > 0)  {
	        di.consumerWildcards = h;
	    }

	    h = new Hashtable<String, Integer>();
	    if (di.nProducers > 0)  {
		Iterator producers = d.getProducers();

		while (producers.hasNext())  {
		    Producer oneProd = (Producer)producers.next();

		    if (oneProd.isWildcard())  {
			DestinationUID id = oneProd.getDestinationUID();
			String wildcard = id.getName();

			Integer count = h.get(wildcard), newCount;

			if (count == null)  {
			    newCount = new Integer(1);
			} else  {
			    newCount = new Integer(count.intValue() + 1);
			}
			h.put(wildcard, newCount);
		    }
		}
	    }

	    if (h.size() > 0)  {
	        di.producerWildcards = h;
	    }
	}


 
        return di;
        
    }


}
