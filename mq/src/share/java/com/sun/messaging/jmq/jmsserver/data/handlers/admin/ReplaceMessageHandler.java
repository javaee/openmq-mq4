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
 * @(#)ReplaceMessageHandler.java	1.5 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.data.handlers.admin;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;
import java.util.Hashtable;
import java.util.HashMap;
import java.nio.ByteBuffer;

import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQConnection;
import com.sun.messaging.jmq.util.DestType;
import com.sun.messaging.jmq.io.*;
import com.sun.messaging.jmq.util.admin.MessageType;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.core.Destination;
import com.sun.messaging.jmq.jmsserver.core.PacketReference;

public class ReplaceMessageHandler extends AdminCmdHandler  {
    private static boolean DEBUG = getDEBUG();

    public ReplaceMessageHandler(AdminDataHandler parent) {
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
                            "Replace message: " + cmd_props);
        }

        int status = Status.OK;
        String errMsg = null;

        String destination = (String)cmd_props.get(MessageType.JMQ_DESTINATION);
        Integer destType = (Integer)cmd_props.get(MessageType.JMQ_DEST_TYPE);
        String msgID = (String)cmd_props.get(MessageType.JMQ_MESSAGE_ID);
	Hashtable newMsgIDHash = null;
        String newMsgID = null;
        Object body = null;

	if ((destination == null) || (destType == null))  {
            errMsg = "REPLACE_MESSAGE: Destination name and type not specified";
            logger.log(Logger.ERROR, errMsg);

            status = Status.ERROR;
	}

	if (msgID == null)  {
            errMsg = "REPLACE_MESSAGE: Message ID not specified";
            logger.log(Logger.ERROR, errMsg);

            status = Status.ERROR;
	}

        body = getBodyObject(cmd_msg);

	if ((body == null) || !(body instanceof HashMap))  {
            errMsg = 
		"REPLACE_MESSAGE: New message body specified or is of incorrect type";
            logger.log(Logger.ERROR, errMsg);

            status = Status.ERROR;
	}

        if (status == Status.OK) {
            try {

                Destination d= Destination.getDestination(destination,
                          DestType.isQueue(destType.intValue()));

                if (d != null) {
                    if (DEBUG) {
                        d.debug();
                    }

		    d.load();

		    SysMessageID sysMsgID = SysMessageID.get(msgID);
                    PacketReference	pr = getPacketReference(sysMsgID);

		    if (pr != null)  {
			HashMap hashMapBody = (HashMap)body;

			/*
			 * Need to check if message types match
			 */
			int oldPacketType = pr.getPacket().getPacketType();
			Integer newPacketType = 
				(Integer)hashMapBody.get("MessageBodyType");

			if (oldPacketType != newPacketType.intValue())  {
                            errMsg = "REPLACE_MESSAGE: Existing message and new message types do not match.";
                            logger.log(Logger.ERROR, errMsg);
    
                            status = Status.ERROR;
			} else  {
		            byte[] bytes = getBytesFromMessage(hashMapBody);

		            newMsgID = d.replaceMessageString(sysMsgID, null,
						bytes);

		           if (newMsgID == null)  {
                               errMsg = "REPLACE_MESSAGE: New message ID not returned as expected.";
                               logger.log(Logger.ERROR, errMsg);

                               status = Status.ERROR;
		           } else  {
		               /*
		                * Need to set new msg ID on msg props
		                */
		               newMsgIDHash = new Hashtable();
		               newMsgIDHash.put(MessageType.JMQ_MESSAGE_ID,
		                                    newMsgID.toString());
		            }
			}
		    } else  {
		        /*
                        errMsg= rb.getString(rb.X_MSG_NOT_FOUND, msgID);
		        */
                        errMsg= "REPLACE_MESSAGE: Could not locate message " 
		                    + msgID 
		                    + " in destination " 
		                    + destination;
                        status = Status.NOT_FOUND;
		    }
                } else {
                    errMsg= rb.getString( rb.X_DESTINATION_NOT_FOUND, 
                               destination);
                    status = Status.NOT_FOUND;
                }
            } catch (Exception ex) {
                logger.log(Logger.ERROR,"Internal Error ", ex);
                status = Status.ERROR;
		errMsg = ex.getMessage();
                assert false;
            }
        }

       // Send reply
       Packet reply = new Packet(con.useDirectBuffers());
       reply.setPacketType(PacketType.OBJECT_MESSAGE);
   
       setProperties(reply, MessageType.REPLACE_MESSAGE_REPLY,
           status, errMsg, newMsgIDHash);
   
       /*
       setBodyObject(reply, v);
       */
       parent.sendReply(con, cmd_msg, reply);

       return true;
   }

    public byte[] getBytesFromMessage(HashMap h)  {
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

    public static PacketReference getPacketReference(SysMessageID sysMsgID)  {
        return (Destination.get(sysMsgID));
    }
}
