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
 * @(#)MessageManagerConfig.java	1.3 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.management.mbeans;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.HashMap;

import javax.management.ObjectName;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanException;

import com.sun.messaging.jmq.io.SysMessageID;
import com.sun.messaging.jmq.io.PacketType;
import com.sun.messaging.jms.management.server.*;
import com.sun.messaging.jmq.jmsserver.core.Consumer;
import com.sun.messaging.jmq.jmsserver.core.Subscription;
import com.sun.messaging.jmq.jmsserver.core.ConsumerUID;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;
import com.sun.messaging.jmq.jmsserver.management.util.ConsumerUtil;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.util.lists.RemoveReason;
import com.sun.messaging.jmq.util.log.Logger;

public class MessageManagerConfig extends MQMBeanReadWrite  {
    private static MBeanParameterInfo[] deleteMessageSignature = {
	            new MBeanParameterInfo("destinationType", String.class.getName(), 
		                        mbr.getString(mbr.I_DST_MGR_OP_PARAM_DEST_TYPE)),
	            new MBeanParameterInfo("destinationName", String.class.getName(), 
		                        mbr.getString(mbr.I_DST_MGR_OP_PARAM_DEST_NAME)),
		    new MBeanParameterInfo("messageID", String.class.getName(),
			                "Message ID")
			    };

    private static MBeanParameterInfo[] replaceMessageSignature = {
	            new MBeanParameterInfo("destinationType", String.class.getName(), 
		                            mbr.getString(mbr.I_DST_MGR_OP_PARAM_DEST_TYPE)),
	            new MBeanParameterInfo("destinationName", String.class.getName(), 
		                        mbr.getString(mbr.I_DST_MGR_OP_PARAM_DEST_NAME)),
		    new MBeanParameterInfo("messageID", String.class.getName(),
			                "Message ID"),
		    new MBeanParameterInfo("messageBody", HashMap.class.getName(),
			                "Message Body")
			    };

    private static MBeanOperationInfo[] ops = {
	    new MBeanOperationInfo("deleteMessage",
		"Delete a message in a destination",
		    deleteMessageSignature, 
		    Void.TYPE.getName(),
		    MBeanOperationInfo.ACTION),

	    new MBeanOperationInfo("replaceMessage",
		"Replace a message in a destination",
		    replaceMessageSignature, 
		    String.class.getName(),
		    MBeanOperationInfo.ACTION)
		};


    public MessageManagerConfig()  {
	super();
    }

    public void deleteMessage(String destinationType, String destinationName,
				String messageID) throws MBeanException {
	try  {
	    if ((destinationName == null) || (destinationType == null))  {
		throw new BrokerException("Destination name and type not specified");
	    }

	    Destination d = Destination.getDestination(destinationName,
			(destinationType.equals(DestinationType.QUEUE)));

	    if (d == null)  {
		throw new BrokerException(rb.getString(rb.X_DESTINATION_NOT_FOUND,
					       destinationName));
	    }

	    if (messageID == null)  {
		throw new BrokerException("Message ID not specified");
	    }

	    SysMessageID sysMsgID = SysMessageID.get(messageID);
	    PacketReference pr = Destination.get(sysMsgID);

	    if (pr != null)  {
                d.removeMessage(sysMsgID, RemoveReason.REMOVED_OTHER);
	        logger.log(Logger.INFO, "Deleted from destination "
			+ destinationName
			+ " message with ID: " 
			+ messageID);
            } else  {
		throw new BrokerException("Could not locate message "
                + messageID
                + " in destination "
                + destinationName);
            }
	} catch (Exception e)  {
            handleOperationException("deleteMessage", e);
	}
    }

    public String replaceMessage(String destinationType, String destinationName,
				String messageID,
				HashMap messageBody) throws MBeanException {
	String newMsgID = null;

	try  {
	    if ((destinationName == null) || (destinationType == null))  {
		throw new BrokerException("Destination name and type not specified");
	    }

	    Destination d = Destination.getDestination(destinationName,
			(destinationType.equals(DestinationType.QUEUE)));

	    if (d == null)  {
		throw new BrokerException(rb.getString(rb.X_DESTINATION_NOT_FOUND,
					       destinationName));
	    }

	    if (messageID == null)  {
		throw new BrokerException("Message ID not specified");
	    }

	    if (messageBody == null)  {
		throw new BrokerException("New message body not specified");
	    }

	    d.load();

	    SysMessageID sysMsgID = SysMessageID.get(messageID);
	    PacketReference pr = Destination.get(sysMsgID);

	    if (pr != null)  {
		/*
		 * Need to check if message types match
		 */
                int oldPacketType = pr.getPacket().getPacketType();
		Integer newPacketType = (Integer)messageBody.get("MessageBodyType");

		if (oldPacketType != newPacketType.intValue())  {
		    throw new BrokerException(
			"Existing message and new message types do not match.");
		}

		byte[] bytes = getBytesFromMessage(messageBody);

		newMsgID = d.replaceMessageString(sysMsgID, null,
					bytes);

		if (newMsgID == null)  {
		    throw new BrokerException(
			"New message ID not returned as expected.");
		}

	        logger.log(Logger.INFO, "Replaced from destination "
			+ destinationName
			+ " old message with ID: " 
			+ sysMsgID
			+ " with new message with ID: "
			+ newMsgID);
            } else  {
		throw new BrokerException("Could not locate message "
                + messageID
                + " in destination "
                + destinationName);
            }

	} catch (Exception e)  {
            handleOperationException("replaceMessage", e);
	}

	return (newMsgID);
    }

    public String getMBeanName()  {
	return ("MessageManagerConfig");
    }

    public String getMBeanDescription()  {
	return ("Configuration MBean for Message Manager");
	/*
	return (mbr.getString(mbr.I_MSG_MGR_CFG_DESC));
	*/
    }

    public MBeanAttributeInfo[] getMBeanAttributeInfo()  {
	return (null);
    }

    public MBeanOperationInfo[] getMBeanOperationInfo()  {
	return (ops);
    }

    public MBeanNotificationInfo[] getMBeanNotificationInfo()  {
	return (null);
    }

    private byte[] getBytesFromMessage(HashMap h)  {
        String errMsg = null;
        byte ba[] = null;

	Object msgBody = h.get("MessageBody");
	Integer msgType = (Integer)h.get("MessageBodyType");

        switch (msgType.intValue())  {
        case PacketType.TEXT_MESSAGE:
	    try  {
                String textMsg = (String)msgBody;
	        ba = textMsg.getBytes("UTF8");
	    } catch(Exception e)  {
                errMsg = "Caught exception while creating text message body";
                logger.log(Logger.ERROR, errMsg, e);
	    }
        break;

        case PacketType.BYTES_MESSAGE:
	    ba = (byte[])msgBody;
        break;

        case PacketType.MAP_MESSAGE:
            try  {
                ByteArrayOutputStream byteArrayOutputStream = 
	                            new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = 
	                            new ObjectOutputStream(byteArrayOutputStream);

	        HashMap map = (HashMap)msgBody;
	        objectOutputStream.writeObject(map);
	        objectOutputStream.flush();

	        ba = byteArrayOutputStream.toByteArray();
            } catch(Exception e)  {
                errMsg = "Caught exception while creating map message body";
                logger.log(Logger.ERROR, errMsg, e);
            }
        break;

        case PacketType.STREAM_MESSAGE:
	    ba = (byte[])msgBody;
        break;

        case PacketType.OBJECT_MESSAGE:
            try  {
                ByteArrayOutputStream byteArrayOutputStream = 
	                            new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = 
	                            new ObjectOutputStream(byteArrayOutputStream);

	        objectOutputStream.writeObject(msgBody);
	        objectOutputStream.flush();

	        ba = byteArrayOutputStream.toByteArray();
	    } catch(Exception e)  {
                errMsg = "Caught exception while creating object message body";
                logger.log(Logger.ERROR, errMsg, e);
            }
        break;

	default:
            errMsg = "Unsupported message type for REPLACE_MESSAGE handler: " 
				+ msgType.intValue();
            logger.log(Logger.ERROR, errMsg);
	break;
        }

        return (ba);
    }

}
