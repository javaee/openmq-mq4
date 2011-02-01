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
 * @(#)HttpsProtocol.java	1.7 06/29/07
 */ 

package com.sun.messaging.jmq.jmsserver.net.https;

import com.sun.messaging.jmq.transport.httptunnel.server.*;
import com.sun.messaging.jmq.transport.httptunnel.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.net.http.HTTPProtocol;
import com.sun.messaging.jmq.jmsserver.resources.*;
import java.net.*;
import java.io.IOException;
import java.util.Hashtable;

public class HttpsProtocol extends HTTPProtocol
{

    static private final String SERVLET_HOST_TRUSTED_PROP = "isHostTrusted";

    // we trust the servlet host by default
    protected boolean isServletHostTrusted = true;

    public HttpsProtocol() {
    }

    protected void createDriver() throws IOException {
        String name = InetAddress.getLocalHost().getHostName() + ":" +
            Globals.getConfigName();

        if (servletHost != null || servletPort != -1) {
            String host = servletHost;
            if (host == null)
                host = InetAddress.getLocalHost().getHostAddress();

            int port = servletPort;
            if (port == -1)
                port = HttpTunnelDefaults.DEFAULT_HTTPS_TUNNEL_PORT;

            InetAddress paddr = InetAddress.getLocalHost();
            InetAddress saddr = InetAddress.getByName(host);
            InetAddress laddr = InetAddress.getByName("localhost");

            if (port == Globals.getPortMapper().getPort() &&
                (saddr.equals(paddr) || saddr.equals(laddr))) {
                throw new IOException(Globals.getBrokerResources().getString(
                    BrokerResources.X_HTTP_PORT_CONFLICT));
            }

            driver = new HttpsTunnelServerDriver(name, host, port,
                isServletHostTrusted);
        } else {
            driver = new HttpsTunnelServerDriver(name, isServletHostTrusted);
        }

        driver.setInactiveConnAbortInterval(connectionTimeout);
        driver.setRxBufSize(rxBufSize);
    }

    protected HttpTunnelServerSocket createSocket() throws IOException {
        if (driver == null)
            createDriver();

        HttpTunnelServerSocket sock = new HttpTunnelServerSocket(driver);
        return sock;
    }

    public String toString() {
        return "https [ " + serversocket + "]";
    }

    public void setParameters(Hashtable params) {

        // check for SERVLET_HOST_TRUSTED_PROP
        String propval = (String)params.get(SERVLET_HOST_TRUSTED_PROP);
        if (propval != null) {
            try {
            boolean value = Boolean.valueOf(propval).booleanValue();
            isServletHostTrusted = value;
            } catch (Exception ex) {}
        }

        super.setParameters(params);
    }

}

/*
 * EOF
 */
