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

package com.sun.messaging.ums.service;

import com.sun.messaging.ums.common.Constants;
import com.sun.messaging.xml.MessageTransformer;
import com.sun.messaging.ums.common.MessageUtil;
import com.sun.messaging.ums.simple.SimpleMessage;
import java.util.Map;
import java.util.Properties;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import javax.jms.Session;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPMessage;

public class SendServiceImpl implements SendService {

    //private Lock lock = null;
    private Properties props = null;
    private ClientPool cache = null;
    //cache sweeper
    private CacheSweeper sweeper = null;
    private Logger logger = UMSServiceImpl.logger;
    
    private static final String SERVICE_NAME = "SEND_SERVICE";
    
    private String myName = null;
    
    private String provider = null;

    public SendServiceImpl(String provider, ClientPool cache, CacheSweeper sweeper, Properties p) throws JMSException {
        
        this.provider = provider;
        
        this.sweeper = sweeper;
        
        this.myName = provider + "_" + SERVICE_NAME;
        
        this.props = p;

        //lock = new Lock();

        //cache = new JMSCache(MY_NAME, props, lock, logger);
        //cache = new ClientPool(provider, SERVICE_NAME, props, lock);
       
        //add my cache to the sweeper
        //sweeper.addClientPool(cache);
        this.cache = cache;
    }
    
    //public String authenticate (String user, String password) throws JMSException {
    //    return cache.authenticate(user, password);
    //}
    
    //public void authenticateUUID (String clientId) throws JMSException {
    //    cache.authenticateUUID (clientId);
    //}
       
    public void send(SOAPMessage sm) throws JMSException {

        Client client = null;
        Message message = null;
        
        String user = null;
        String pass = null;

        try {
            
            /**
            String clientId = MessageUtil.getServiceClientId(sm);

            if (clientId == null) {
                user = MessageUtil.getServiceAttribute(sm, Constants.USER);
                pass = MessageUtil.getServiceAttribute(sm, Constants.PASSWORD);
            }
            
            client = cache.getClient(clientId, user, pass);
            **/
            
            client = cache.getClient(sm);
            
            //Object syncObj = lock.getLock(client.getId());
            Object syncObj = client.getLock();
            
            if (UMSServiceImpl.debug) {
                logger.info("*** SendServiceImpl sending message: " + sm);
            }
            
            synchronized (syncObj) {

                //client = cache.getClient(clientId);

                Session session = client.getSession();

                String destName = MessageUtil.getServiceDestinationName(sm);
                boolean isTopic = MessageUtil.isServiceTopicDomain(sm);

                Destination dest = cache.getJMSDestination(destName, isTopic);
                
                //XXX remove message header element
                MessageUtil.removeMessageHeaderElement(sm);
                
                if (UMSServiceImpl.debug) {
                    logger.info("*** SendServiceImpl sending message: " + sm);
                }
                //sm.writeTo(System.out);
                
                message = MessageTransformer.SOAPMessageIntoJMSMessage(sm, session);

                MessageProducer producer = client.getProducer();

                producer.send(dest, message);
            }

            if (UMSServiceImpl.debug) {
                logger.info ("*** SendServiceImpl sent message: " + message);
            }
            
        } catch (Exception ex) {

            if (ex instanceof JMSException) {
                throw (JMSException) ex;
            } else {
                JMSException jmse = new JMSException(ex.getMessage());
                jmse.setLinkedException(ex);
                throw jmse;
            }

        } finally {
            cache.returnClient(client);
        }
    }

    /**
     * Send a Text message to MQ.
     * 
     * @param clientId
     * @param isTopic
     * @param destName
     * @param text
     * @throws javax.jms.JMSException
     */
    public void sendText(String sid, boolean isTopic, String destName, String text, Map map) throws JMSException {

        Client client = null;
        Message message = null;

        String user = null;
        String pass = null;

        try {

            client = cache.getClient(sid, map); 
            
            //Object syncObj = lock.getLock(client.getId());
            Object syncObj = client.getLock();
            
            if (UMSServiceImpl.debug) {
                logger.info("*** SendServiceImpl sending simple message: sid = " + sid + ", text=" + text);
            }
            
            synchronized (syncObj) {

                //client = cache.getClient(clientId);

                Session session = client.getSession();

                Destination dest = cache.getJMSDestination(destName, isTopic);

                message = session.createTextMessage(text);

                MessageProducer producer = client.getProducer();

                producer.send(dest, message);
            }

            if (UMSServiceImpl.debug) {
                logger.info("*** SendServiceImpl sent text message... ");
            }
            
        } catch (Exception ex) {
            
            logger.log(Level.WARNING, ex.getMessage(), ex);

            if (ex instanceof JMSException) {
                throw (JMSException) ex;
            } else {
                JMSException jmse = new JMSException(ex.getMessage());
                jmse.setLinkedException(ex);
                throw jmse;
            }

        } finally {
            cache.returnClient(client);
        }
    }
    
    public void commit (SimpleMessage sm) throws JMSException {
        
        String sid = sm.getMessageProperty(Constants.CLIENT_ID);
        Map map = sm.getMessageProperties();
        
        if (sid == null) {
            throw new JMSException ("Cannot commit a transaction because sid is null.");
        }
        
        Client client = cache.getClient(sid, map);
        
        this.commit(client);
    }
    
     public void rollback (SimpleMessage sm) throws JMSException {
        
        String sid = sm.getMessageProperty(Constants.CLIENT_ID);
        Map map = sm.getMessageProperties();
        
         if (sid == null) {
             throw new JMSException("Cannot rollback a transaction because sid is null.");
         }
        
        Client client = cache.getClient(sid, map);
        
        this.rollback (client);
    }
    
    public void commit (SOAPMessage sm) throws JMSException {
        
        try {
            
            String sid = MessageUtil.getServiceClientId(sm);
            
            if (sid == null) {
                throw new JMSException("Cannot commit a transaction because sid is null.");
            }
        
            Client client = cache.getClient(sm);
        
            this.commit(client);
            
        } catch (SOAPException soape) {
            JMSException jmse = new JMSException (soape.getMessage());
            
            jmse.setLinkedException (soape);
            throw jmse;
        }
    }
    
    public void rollback(SOAPMessage sm) throws JMSException {

        try {


            String sid = MessageUtil.getServiceClientId(sm);

            if (sid == null) {
                throw new JMSException("Cannot rollback a transaction because sid is null.");
            }

            Client client = cache.getClient(sm);

            this.rollback(client);
        } catch (SOAPException soape) {
            JMSException jmse = new JMSException(soape.getMessage());

            jmse.setLinkedException(soape);
            throw jmse;
        }
    }
    
     private void commit (Client client) throws JMSException {

        try {
           
            //client = cache.getClient(sm);
            
            //Object syncObj = lock.getLock(client.getId());
            Object syncObj = client.getLock();
            
            if (UMSServiceImpl.debug) {
                logger.info("*** Commiting transaction, sid = " + client.getId());
            }
            
            synchronized (syncObj) {

                Session session = client.getSession();
                session.commit();
            }
               
            if (UMSServiceImpl.debug) {
                logger.info ("*** Transaction committed. sid=" + client.getId());
            }
            
        } catch (Exception ex) {

            if (ex instanceof JMSException) {
                throw (JMSException) ex;
            } else {
                JMSException jmse = new JMSException(ex.getMessage());
                jmse.setLinkedException(ex);
                throw jmse;
            }

        } finally {
            cache.returnClient(client);
        }
    }
     
    private void rollback (Client client) throws JMSException {

        try {
           
            //client = cache.getClient(sm);
            
            //Object syncObj = lock.getLock(client.getId());
            Object syncObj = client.getLock();
            
            if (UMSServiceImpl.debug) {
                logger.info("*** rolling back transaction, sid = " + client.getId());
            }
            
            synchronized (syncObj) {

                Session session = client.getSession();
                session.rollback();
            }
               
            if (UMSServiceImpl.debug) {
                logger.info ("*** Transaction rolled back. sid=" + client.getId());
            }
            
        } catch (Exception ex) {

            if (ex instanceof JMSException) {
                throw (JMSException) ex;
            } else {
                JMSException jmse = new JMSException(ex.getMessage());
                jmse.setLinkedException(ex);
                throw jmse;
            }

        } finally {
            cache.returnClient(client);
        }
    }

    public void close() {
        
        try {
            //sweeper.removeClientPool(cache);
            cache.close();
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        
    }

}
