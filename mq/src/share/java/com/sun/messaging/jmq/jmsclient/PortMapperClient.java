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
 * @(#)PortMapperClient.java	1.30 06/27/07
 */ 

package com.sun.messaging.jmq.jmsclient;

import javax.jms.*;
import com.sun.messaging.jmq.Version;
import com.sun.messaging.jmq.io.PortMapperTable;
import com.sun.messaging.jmq.io.PortMapperEntry;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.ReadOnlyPacket;
import com.sun.messaging.jmq.jmsclient.protocol.tcp.TCPConnectionHandler;
import com.sun.messaging.AdministeredObject;
import com.sun.messaging.ConnectionConfiguration;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.Map;

/**
 *
 * This class provide a way for the client to recover itself if the
 * connection goes away.
 *
 * <p>The API user is transparent to the recovering attempt.  All
 * consumers are registered to the broker with their original IDs.
 * All producers are added with the same parameters.
 *
 */


public class PortMapperClient {

    protected ConnectionImpl connection = null;
    protected PortMapperTable portMapperTable = null;

    protected boolean useMQAddress = false;
    protected MQAddress addr = null;

    private static final Version version = com.sun.messaging.jmq.jmsclient.ConnectionImpl.version;

    private boolean debug = Debug.debug;

    public PortMapperClient (ConnectionImpl connection) throws JMSException{
        this.connection = connection;
        init();
    }

    public PortMapperClient (MQAddress addr, ConnectionImpl connection)
        throws JMSException {
        this.addr = addr;
        this.useMQAddress = true;
        this.connection = connection;
        init();
    }

    public int getPortForProtocol(String protocol){
        String type = connection.getConnectionType();
        return getPort(protocol, type, null);
    }

    //bug 4959114.
    public int
    getPortForService(String protocol, String service) throws JMSException {
        String type = connection.getConnectionType();
        int port = getPort(protocol, type, service);

        if ( port == -1 ) {
            String errorString =
            AdministeredObject.cr.getKString(AdministeredObject.cr.X_UNKNOWN_BROKER_SERVICE, service);
            JMSException jmse =new com.sun.messaging.jms.JMSException
            (errorString, AdministeredObject.cr.X_UNKNOWN_BROKER_SERVICE);

            ExceptionHandler.throwJMSException(jmse);

        }

        return port;
    }

    //bug 4959114.
    private int getPort(String protocol, String type, String servicename) {
        //int port = 25374;
        int port = -1;
        Map table = portMapperTable.getServices();
        PortMapperEntry pme = null;

        Iterator it = table.values().iterator();

        while (it.hasNext()){
            pme = (PortMapperEntry) it.next();
            if (pme.getProtocol().equals(protocol)){
                if (pme.getType().equals(type)){
                    if (servicename == null){
                        port = pme.getPort();
                        break;
                    } else {
                        if (pme.getName().equals(servicename)){
                            port = pme.getPort();
                            break;
                        }
                    }
                }
            }
        }

        return port;
    }

    protected void init() throws JMSException {
        try {
            readBrokerPorts();

            checkBrokerVersion();
        } catch (JMSException jmse) {

            String str = this.getHostName() + ":" + this.getHostPort();
            connection.setLastContactedBrokerAddress(str);

            ExceptionHandler.throwJMSException(jmse);
        }
    }

    protected void checkBrokerVersion() throws JMSException {
        String bkrversion = portMapperTable.getBrokerVersion();
        String clientMVersion = version.getImplementationVersion();

        // Raptor (3.5) clients can talk to a Falcon (3.0) broker.
        if (Version.compareVersions(bkrversion, "3.0") < 0) {
            String errorString = AdministeredObject.cr.getKString(
                AdministeredObject.cr.X_VERSION_MISMATCH,
                 clientMVersion, bkrversion);

            JMSException jmse =
            new com.sun.messaging.jms.JMSException
            (errorString,AdministeredObject.cr.X_VERSION_MISMATCH);

            ExceptionHandler.throwJMSException(jmse);
        }

        // Use Packet version 200 for brokers older than 3.5
        if (Version.compareVersions(bkrversion, "3.0.1", false) < 0) {
            ReadOnlyPacket.setDefaultVersion(Packet.VERSION2);
        }
    }

    private String getHostName() {
        if (useMQAddress)
            return addr.getHostName();

        return connection.getProperty(
            ConnectionConfiguration.imqBrokerHostName);
    }

    public int getHostPort() {
        if (useMQAddress)
            return addr.getPort();

        String prop = connection.getProperty(
            ConnectionConfiguration.imqBrokerHostPort);
        return Integer.parseInt(prop);
    }

    protected void readBrokerPorts() throws JMSException {

        String host = getHostName();
        //port mapper port
        int port = getHostPort();

        if ( debug ) {
            Debug.println("PortMapper connecting to host: " + host + "  port: " + port);
        }

        InputStream is = null;
        OutputStream os = null;
        Socket socket = null;
        try {
            String version =
                String.valueOf(PortMapperTable.PORTMAPPER_VERSION) + "\n";

            // bug 6696742 - add ability to set connect timeout 
            int timeout = connection.getSocketConnectTimeout();
            Integer sotimeout = connection.getPortMapperSoTimeout();
            socket = makePortMapperClientSocketWithTimeout(host, port, timeout, sotimeout);
              
            is = socket.getInputStream();
            os = socket.getOutputStream();

            // Write version of portmapper we support to broker
            try {
                os.write(version.getBytes());
                os.flush();
            } catch (IOException e) {
                // This can sometimes fail if the server already wrote
                // the port table and closed the connection
            }

            portMapperTable = new PortMapperTable();
            portMapperTable.read(is);

            is.close();
            socket.close();

        } catch ( Exception e ) {
            try {
                if (os != null) {
                    os.close();
                }
                if (is != null) {
                    is.close();
                }
                if (socket != null) {
                    socket.close();
                }
            } catch (Exception ee) {
                /* ignore */
            }
            connection.getExceptionHandler().handleConnectException (
                e, host, port);
        }
    }
    
    
    private Socket makePortMapperClientSocketWithTimeout (String host, int port, 
                                          int timeout, Integer sotimeout)
                                          throws IOException {
        Socket socket = null;
        if (timeout > 0) {
            ConnectionImpl.getConnectionLogger().fine ("connecting with timeout=" + timeout);

            socket = new Socket();
            InetSocketAddress socketAddr = new InetSocketAddress (host, port);
            socket.connect(socketAddr, timeout);
            socket.setSoTimeout(0);
        } else {
            ConnectionImpl.getConnectionLogger().fine ("connecting with no timeout ...");
            socket = new Socket(host, port);
        }
        if (sotimeout !=  null) {
            socket.setSoTimeout(sotimeout.intValue());
        }
        ConnectionImpl.getConnectionLogger().fine ("socket connected., host=" + host + ", port="+ port);
    	return socket;
    }

    public static void main (String args[]) {
        try {
            PortMapperClient pmc = new PortMapperClient (null);
            String protocol = "tcp";

            String prop = System.getProperty("protocol");
            if ( prop != null ) {
                protocol = prop;
            }

            int port = pmc.getPortForProtocol(protocol);

            if ( Debug.debug ) {
                Debug.println ("port = " + port );
            }

        } catch (Exception e) {
            Debug.printStackTrace(e);
        }
    }
}

