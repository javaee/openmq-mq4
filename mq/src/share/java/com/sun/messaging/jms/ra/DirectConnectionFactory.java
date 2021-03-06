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

package com.sun.messaging.jms.ra;

import javax.jms.*;
import javax.resource.ResourceException;

import javax.transaction.xa.XAResource;
import javax.naming.Reference;
import javax.naming.NamingException;

import java.util.Vector;
import java.util.Properties;
import java.util.logging.Logger;

import com.sun.messaging.jmq.jmsservice.*;

/**
 *  DirectConnectionFactory encapsulates JMS ConnectionFactory behavior for MQ
 *  DIRECT mode operation.
 */
public class DirectConnectionFactory extends ConnectionCreator
        implements
        javax.jms.ConnectionFactory,
        javax.jms.QueueConnectionFactory,
        javax.jms.TopicConnectionFactory,
        javax.resource.Referenceable,
        java.io.Serializable {

    /**
     *  Configuration properties of the Direct ConnectionFactory
     */
    private Properties configuration = null;

    /**
     *  ResourceAdapter for this Direct ConnectionFactory
     */
    private ResourceAdapter ra = null;

    /**
     *  ManagedConnectionFactory for this DirectConnectionFactory
     */
    private ManagedConnectionFactory mcf = null;

    /**
     *  Flags whether this DirectConnectionFactory is being used in the 
     *  Application Client Container
     */
    private boolean inACC = false;

    /**
     *  Flags whether this DirectConnectionFactory is being used in a clustered
     *  instance or not
     */
    private boolean inClusteredContainer = false;

    /**
     *  The RA Namespace to use for this DirectConnectionFactory in a clustered
     *  environment
     */
    private String raNameSpace = null;

    /**
     *  JMSService for the Direct ConnectionFactory
     */
    private transient JMSService jmsservice = null;

    /**
     *  Reference for the Direct ConnectionFactory
     */
    private Reference reference = null;

    /** The ConnectionManager instance */
    private javax.resource.spi.ConnectionManager cm = null;

    /**
     *  DirectConnection made by this Direct ConnectionFactory
     */
    private transient Vector <DirectConnection> connections = null;

    /**
     *  Logging
     */
    private static transient final String _className =
            "com.sun.messaging.jms.ra.DirectConnectionFactory";
    private static transient final String _lgrNameOutboundConnection =
            "javax.resourceadapter.mqjmsra.outbound.connection";
    private static transient final String _lgrNameJMSConnectionFactory =
            "javax.jms.ConnectionFactory.mqjmsra";
    private static transient final Logger _loggerOC =
            Logger.getLogger(_lgrNameOutboundConnection);
    private static transient final Logger _loggerJF =
            Logger.getLogger(_lgrNameJMSConnectionFactory);
    private static transient final String _lgrMIDPrefix = "MQJMSRA_DCF";
    private static transient final String _lgrMID_EET = _lgrMIDPrefix+"1001: ";
    private static transient final String _lgrMID_INF = _lgrMIDPrefix+"1101: ";
    private static transient final String _lgrMID_WRN = _lgrMIDPrefix+"2001: ";
    private static transient final String _lgrMID_ERR = _lgrMIDPrefix+"3001: ";
    private static transient final String _lgrMID_EXC = _lgrMIDPrefix+"4001: ";

    private static boolean _disableConnectionManagement;
    //private static String dcm;

    static {
        //dcm = System.getProperty("imq.jmsra.direct.disableCM", "false");
        _disableConnectionManagement = Boolean.getBoolean("imq.jmsra.direct.disableCM");
    }


    /**
     *  Construct a new instance of DirectConnectionFactory with a JMSService
     *  object and configuration properties
     *
     *  @param  jmsservice The JMSService to use to create JMS objects
     *  @param  props The Properties to use to configure this ConnectionFactory
     *
     */
    public DirectConnectionFactory(JMSService jmsservice, Properties props) {
        Object params[] = new Object[2];
        params[0] = jmsservice;
        params[1] = props;
        _loggerOC.entering(_className, "constructor(jmsservice, props)", params);
        this.jmsservice = jmsservice;
        this.configuration= props;
        connections = new Vector <DirectConnection> ();
        _loggerOC.exiting(_className, "constructor(jmsservice, props):config="+toString());
    }

    /**
     *  Construct a new instance of DirectConnectionFactory with a JMSService
     *  object, configuration properties and a ConnectionManager
     * @throws ResourceException 
     */
    public DirectConnectionFactory(ManagedConnectionFactory mcf,
            javax.resource.spi.ConnectionManager cm) {
        Object params[] = new Object[2];
        params[0] = mcf;
        params[1] = cm;
        _loggerOC.entering(_className, "constructor(mcf, cm)", params);
        this.mcf = mcf;
        this.cm = cm;
        this.ra = (ResourceAdapter)mcf.getResourceAdapter();
        this.jmsservice = ra._getJMSService();
        this.inACC = ra.getInAppClientContainer();
        this.inClusteredContainer = ra.getInClusteredContainer();
        this.raNameSpace = ra._getRAUID();
        connections = new Vector <DirectConnection> ();
        _loggerOC.exiting(_className, "constructor(mcf, cm):config="+toString());

    }

    /////////////////////////////////////////////////////////////////////////
    //  Methods implementing javax.resource.Referenceable
    /////////////////////////////////////////////////////////////////////////
    /**
     *  Set the Reference for this ConnectionFactory
     *
     *  @param  ref The Reference to set
     */
    public void setReference(Reference ref) {
        _loggerJF.fine(_lgrMID_INF+"setReference():" + "Reference="+ref);
        this.reference = ref;
    }

    /**
     *  Returns the Reference for this ConnectionFactory
     *
     *  @return The Reference
     *
     *  @throws NamingException If the Reference is unavailable
     */
    public Reference getReference()
    throws NamingException {
        _loggerJF.fine(_lgrMID_INF+"getReference():");
        //MQJMSRA doesn't create Reference objects;
        //throw the correct Exception
        throw new NamingException("MQRA:DCF:getReference:NOT Supported");
    }
    /////////////////////////////////////////////////////////////////////////
    //  end javax.resource.Referenceable
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    //  Methods that implement javax.jms.ConnectionFactory
    /////////////////////////////////////////////////////////////////////////
    /**
     *  Creates a DIRECT Connection with the default user identity.
     *
     *  @return The JMS Connection object to use
     *
     *  @throws JMSSecurityException if the default user identity does not
     *          authenthicate successfully with the JMS server
     *  @throws JMSException if any JMS server error occurred
     */  
    public Connection createConnection()
    throws JMSException {
        _loggerJF.fine(_lgrMID_INF+"createConnection():");
        String username, password;
        if (this.mcf == null){
            username = "guest";
            password = "guest";
        } else {
            username = mcf.getUserName();
            password = mcf.getPassword();
        }
        return this.createConnection(username, password);
    }

    /**
     *  Creates a DIRECT Connection with a specified user identity.
     * 
     *  @param username The username that should be used to authenticate the
     *                  creation of this JMS Connection
     *  @param password The password that should be used to authenticate the
     *                  creation of this JMS Connection
     *
     *  @return The JMS Connection object to use
     *
     *  @throws JMSSecurityException if the specified user identity does not
     *          authenticate successfully with the JMS server
     *  @throws JMSException if any JMS server error occurred
     */  
    public Connection createConnection(String username, String password)
    throws JMSException {
        _loggerJF.fine(_lgrMID_INF+"createConnection(u,p):username="+username);
        if (!this._disableConnectionManagement){
            return (Connection)this._allocateConnection(username, password);
        }
        long connectionId = _createConnectionId(username, password);
        DirectConnection dc = new DirectConnection(this, this.jmsservice,
                connectionId, this.inACC);
        return (Connection) dc; 
    }
    /////////////////////////////////////////////////////////////////////////
    //  end javax.jms.ConnectionFactory
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    //  Methods that implement javax.jms.QueueConnectionFactory
    /////////////////////////////////////////////////////////////////////////
    /**
     *  Creates a DIRECT QueueConnection with the default user identity.
     *
     *  @return The JMS QueueConnection object to use
     *
     *  @throws JMSSecurityException if the default user identity does not
     *          authenthicate successfully with the JMS server
     *  @throws JMSException if any JMS server error occurred
     */  
    public QueueConnection createQueueConnection()
    throws JMSException {
        _loggerJF.fine(_lgrMID_INF+"createQueueConnection():");
        String username, password;
        if (this.mcf == null){
            username = "guest";
            password = "guest";
        } else {
            username = mcf.getUserName();
            password = mcf.getPassword();
        }
        return this.createQueueConnection(username, password);
    }

    /**
     *  Creates a DIRECT QueueConnection with a specified user identity.
     * 
     *  @param username The username that should be used to authenticate the
     *                  creation of this JMS QueueConnection
     *  @param password The password that should be used to authenticate the
     *                  creation of this JMS QueueConnection
     *
     *  @return The JMS QueueConnection object to use
     *
     *  @throws JMSSecurityException if the specified user identity does not
     *          authenticate successfully with the JMS server
     *  @throws JMSException if any JMS server error occurred
     */  
    public QueueConnection createQueueConnection(
            String username, String password)
    throws JMSException {
        _loggerJF.fine(_lgrMID_INF+
                "createQueueConnection(u,p):username="+username);
        if (!this._disableConnectionManagement){
            return (QueueConnection)this._allocateConnection(username, password);
        }
        long connectionId = _createConnectionId(username, password);
        DirectConnection dc = new DirectConnection(this, this.jmsservice,
                connectionId, this.inACC);
        return (QueueConnection) dc; 
    }
    /////////////////////////////////////////////////////////////////////////
    //  end javax.jms.QueueConnectionFactory
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    //  Methods that implement javax.jms.TopicConnectionFactory
    /////////////////////////////////////////////////////////////////////////
    /**
     *  Creates a DIRECT TopicConnection with the default user identity.
     *
     *  @return The JMS TopicConnection object to use
     *
     *  @throws JMSSecurityException if the default user identity does not
     *          authenthicate successfully with the JMS server
     *  @throws JMSException if any JMS server error occurred
     */  
    public TopicConnection createTopicConnection()
    throws JMSException {
        _loggerJF.fine(_lgrMID_INF+"createTopicConnection():");
        String username, password;
        if (this.mcf == null){
            username = "guest";
            password = "guest";
        } else {
            username = mcf.getUserName();
            password = mcf.getPassword();
        }
        return this.createTopicConnection(username, password);
    }

    /**
     *  Creates a DIRECT TopicConnection with a specified user identity.
     * 
     *  @param username The username that should be used to authenticate the
     *                  creation of this JMS TopicConnection
     *  @param password The password that should be used to authenticate the
     *                  creation of this JMS TopicConnection
     *
     *  @return The JMS TopicConnection object to use
     *
     *  @throws JMSSecurityException if the specified user identity does not
     *          authenticate successfully with the JMS server
     *  @throws JMSException if any JMS server error occurred
     */  
    public TopicConnection createTopicConnection(
            String username, String password)
    throws JMSException {
        _loggerJF.fine(_lgrMID_INF+"createTopicConnection(u,p):username="+username);
        if (!this._disableConnectionManagement){
            return (TopicConnection)this._allocateConnection(username, password);
        }
        long connectionId = _createConnectionId(username, password);
        DirectConnection dc = new DirectConnection(this, this.jmsservice,
                connectionId, this.inACC);
        return (TopicConnection) dc; 
    }
    /////////////////////////////////////////////////////////////////////////
    //  end javax.jms.TopicConnectionFactory
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    //  MQ methods
    /////////////////////////////////////////////////////////////////////////
    /**
     *  Create a connection with the jmsservice and return a connectionId.
     *  Used by the methods implementing javax.jms.Connection,
     *  javax,jms.QueueConnection, and javax.jms.TopicConnection
     *
     *  @param username The username that should be used to authenticate the
     *                  creation of the connection with the jmsservice
     *  @param password The password that should be used to authenticate the
     *                  creation of the connection with the jmsservice
     *
     *  @return The connectionId to be used by the DirectConnection object
     *          that is returned by the JMS API method
     *
     *  @throws JMSSecurityException if the specified user identity does not
     *          authenticate successfully with the JMS server
     *  @throws JMSException if any JMS server error occurred
     */
    private long _createConnectionId(String username, String password)
    throws JMSException {
        JMSServiceReply reply;
        long connectionId = 0L;
        if (this.jmsservice == null) {
            if (this.ra != null) {
                this.jmsservice = this.ra._getJMSService();
            }
            if (this.jmsservice == null) {
                //Ultimate fallback :)
                this.jmsservice = ResourceAdapter._getRAJMSService();
            }
        }
        assert (this.jmsservice != null);
        try {
            reply = jmsservice.createConnection(username, password, null);
            try {
                connectionId = reply.getJMQConnectionID();                
            } catch (NoSuchFieldException nsfe){
                String exerrmsg = _lgrMID_EXC +
                        "JMSServiceException:Missing JMQConnectionID";
                JMSException jmse = new JMSException(exerrmsg);
                jmse.initCause(nsfe);
                _loggerJF.severe(exerrmsg);
                throw jmse;
            }

        } catch (JMSServiceException jse) {
            JMSServiceReply.Status status = jse.getJMSServiceReply().getStatus();
            JMSException jmsse;
            String failure_cause;
            boolean security_exception = true;
            switch (status) {
                case INVALID_LOGIN:
                    failure_cause = "authentication failure.";
                    break;
                case FORBIDDEN:
                    failure_cause = "authorization failure.";
                    break;
                default:
                    failure_cause = "unkown JMSService server error.";
                    security_exception = false;
            }
            String exerrmsg = 
                    "createConnection on JMSService:" +
                    jmsservice.getJMSServiceID() +
                    " failed for username:"+ username +
                    " due to " + failure_cause;
            _loggerJF.severe(exerrmsg);
            jmsse = (security_exception
                    ? new JMSSecurityException(exerrmsg)
                    : new JMSException(exerrmsg));
            jmsse.initCause(jse);
            throw jmsse;
        }
        return connectionId;
    }

    /**
     *  Set the ResourceAdapter object for this DirectConnectionFactory
     */
    public void setResourceAdapter(ResourceAdapter ra) {
        this.ra = ra;
    }

    /**
     *  Returns the configuration of this DirectConnectionFactory
     *
     *  @return The configuration properties of this DirectConnectionFactory
     */
    public Properties _getConfiguration() {
        return configuration;
    }

    /**
     *  Set a configuration property value for this DirectConnectionFactory
     *
     *  @param  name    The name of the property to set
     *  @param  value   The value of the property to set
     *
     *  @throws IllegalArgumentException If the property name is invalid
     *
     */
    public void setConfigurationProperty(String name, String value)
    throws IllegalArgumentException {
        
    }

    /**
     *  Returns whether this DirectConnectionFactory is active in an
     *  Application Server cluster or not.
     *
     *  @return {@code true} if it is active in a cluster;
     *          {@code false} otherwise
     */
    protected boolean isRAClustered() {
        return this.inClusteredContainer;
    }

    /**
     *  Returns the name space assigned by the RA.
     *  Each instance of the RA sets a 'namespace within which JMS client
     *  identifiers are required to be unique. The 'namespace' is acquired by
     *  the RA from the MQ broker.
     *
     *  @return The RA name space
     */
    protected String getRANamespace() {
        return this.raNameSpace;
    }

    protected Connection _createConnection(String username, String password)
    throws JMSException {
        long connectionId = _createConnectionId(username, password);
        DirectConnection dc = new DirectConnection(this, this.jmsservice,
                connectionId, this.inACC);
        return dc;
    }

    protected XAResource _createXAResource(ManagedConnection mc, Object conn)
    throws JMSException {
        return (XAResource)null;
    }

    private Connection _allocateConnection(String username, String password)
    throws JMSException
    {
        javax.resource.spi.ConnectionRequestInfo crinfo =
            new com.sun.messaging.jms.ra.ConnectionRequestInfo(mcf,
                username, password);

        //System.out.println("MQRA:CFA:createConnection:allocating connection");
        DirectConnection dc;
        try {
            dc = (DirectConnection)this.cm.allocateConnection(mcf, crinfo);
            return dc;
        } catch (ResourceException re) {
            //XXX:Fix codes
            String code = "1";
            throw new com.sun.messaging.jms.JMSException(
                "MQRA:DCF:allocation failure:createConnection:"+re.getMessage(), code, re);
        }
    }
}
