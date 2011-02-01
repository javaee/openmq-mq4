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

package com.sun.messaging.bridge.service.stomp;

import com.sun.grizzly.*;
import com.sun.grizzly.filter.*;
import com.sun.grizzly.util.ConnectionCloseHandler;
import com.sun.grizzly.util.AttributeHolder;
import com.sun.grizzly.util.Grizzly;

import java.io.*;
import java.net.URL;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.util.Locale;
import java.util.Properties;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.concurrent.CountDownLatch;
import javax.net.ssl.SSLContext;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import com.sun.messaging.bridge.service.BridgeContext;
import com.sun.messaging.bridge.service.MessageTransformer;
import com.sun.messaging.bridge.service.LogSimpleFormatter;
import com.sun.messaging.bridge.service.stomp.resources.StompBridgeResources;

/**
 * @author amyk 
 */
public class StompServer implements ConnectionCloseHandler {

    private final static String PROP_HOSTNAME_SUFFIX = ".hostname";
    private final static String PROP_TCPENABLED_SUFFIX = ".tcp.enabled";
    private final static String PROP_SSLENABLED_SUFFIX = ".tls.enabled";
    private final static String PROP_TCPPORT_SUFFIX = ".tcp.port";
    private final static String PROP_SSLPORT_SUFFIX = ".tls.port";
    private final static String PROP_SSL_REQUIRE_CLIENTAUTH_SUFFIX = ".tls.requireClientAuth";
    private final static String PROP_FLOWLIMIT_SUFFIX = ".consumerFlowLimit";
    private final static String PROP_MSGTRANSFORM_SUFFIX = ".messageTransformer";

    private final static String PROP_LOGFILE_LIMIT_SUFFIX = ".logfile.limit";
    private final static String PROP_LOGFILE_COUNT_SUFFIX = ".logfile.count";

    public final static int DEFAULT_TCPPORT = 7672;
    public final static int DEFAULT_SSLPORT = 7673;

    private static StompBridgeResources _sbr = getStompBridgeResources();

    private static Logger _logger = null;

    private  int TCPPORT = DEFAULT_TCPPORT;
    private  int SSLPORT = DEFAULT_SSLPORT;
    private InetAddress HOST = null;
    private String TCPHOSTNAMEPORT = null;
    private String SSLHOSTNAMEPORT = null;

    private Controller controller = null;

    private static MessageTransformer<Message, Message> _msgTransformer = null; 

    private BridgeContext _bc = null;
    private boolean _tcpEnabled = false;
    private boolean _sslEnabled = false;
    private boolean _inited = false;

    public synchronized void init(BridgeContext bc) throws Exception {
        _bc = bc;

        Properties props = bc.getConfig();

        String domain = props.getProperty(BridgeContext.BRIDGE_PROP_PREFIX);

        String cn = props.getProperty(domain+PROP_MSGTRANSFORM_SUFFIX);
        if (cn != null ) {
            _msgTransformer = (MessageTransformer<Message, Message>)
                                     Class.forName(cn).newInstance();
        }

        Properties jmsprop =  new Properties();
        String flowlimit = props.getProperty(domain+PROP_FLOWLIMIT_SUFFIX);
        if (flowlimit != null) {
            jmsprop.setProperty(
                    com.sun.messaging.ConnectionConfiguration.imqConsumerFlowLimit,
                                String.valueOf(Integer.parseInt(flowlimit)));
        }

        _logger = Logger.getLogger(domain);
        if (bc.isSilentMode()) {
            _logger.setUseParentHandlers(false);
        }

        String var = bc.getRootDir();
        File dir =  new File(var);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        String logfile = var+File.separator+"stomp%g.log";

        int limit = 0, count = 1;
        String limits = props.getProperty(domain+PROP_LOGFILE_LIMIT_SUFFIX);
        if (limits != null) {
            limit = Integer.parseInt(limits);
        }
        String counts = props.getProperty(domain+PROP_LOGFILE_COUNT_SUFFIX);
        if (counts != null) {
            count = Integer.parseInt(counts);
        }

        FileHandler h = new FileHandler(logfile, limit, count, true);
        h.setFormatter(new LogSimpleFormatter(_logger));  
        _logger.addHandler(h);

        _logger.log(Level.INFO, getStompBridgeResources().getString(StompBridgeResources.I_LOG_DOMAIN, domain));
        _logger.log(Level.INFO, getStompBridgeResources().getString(StompBridgeResources.I_LOG_FILE, logfile)+"["+limit+","+count+"]");

        if (!bc.isEmbededBroker()) {
            Controller.setLogger(_logger);
        }

        String v = props.getProperty(domain+PROP_TCPENABLED_SUFFIX, "true");
        if (v != null && Boolean.valueOf(v).booleanValue()) {
            String p = props.getProperty(domain+PROP_TCPPORT_SUFFIX, String.valueOf(DEFAULT_TCPPORT));
            TCPPORT = Integer.parseInt(p);
            _tcpEnabled = true;
        }

        v = props.getProperty(domain+PROP_SSLENABLED_SUFFIX, "false");
        if (v != null && Boolean.valueOf(v).booleanValue()) {
            String p = props.getProperty(domain+PROP_SSLPORT_SUFFIX, String.valueOf(DEFAULT_SSLPORT));
            SSLPORT = Integer.parseInt(p);
            _sslEnabled = true;
        }

        if (!_tcpEnabled && !_sslEnabled) {
            throw new IllegalArgumentException(getStompBridgeResources().getKString(StompBridgeResources.X_NO_PROTOCOL));
        }

        v = props.getProperty(domain+PROP_HOSTNAME_SUFFIX);
        if (v == null || v.length() == 0) {
            v = bc.getBrokerHostName();
        }
        String hn = null;
        if (v != null && v.length() > 0) {
            hn = v;
            HOST = InetAddress.getByName(v);
        } else {
            hn = InetAddress.getLocalHost().getCanonicalHostName();
        }
        URL u = new URL("http", hn, TCPPORT, "");
        TCPHOSTNAMEPORT = u.getHost()+":"+TCPPORT;
        u = new URL("http", hn, SSLPORT, "");
        SSLHOSTNAMEPORT = u.getHost()+":"+SSLPORT;

        int major = Grizzly.getMajorVersion();
        int minor = Grizzly.getMinorVersion();
        if (major != 1) {
            String[] params = { String.valueOf(major), Grizzly.getDotedVersion(), String.valueOf(1)};
            String emsg = getStompBridgeResources().getKString(
                          StompBridgeResources.X_INCOMPATIBLE_GRIZZLY_MAJOR_VERSION, params);
            _logger.log(Level.SEVERE, emsg);
            throw new UnsupportedOperationException(emsg);
        } else if (minor < 9) {
            String[] params = { String.valueOf(minor), Grizzly.getDotedVersion(), String.valueOf(9)};
            String emsg = getStompBridgeResources().getKString(
                          StompBridgeResources.X_INCOMPATIBLE_GRIZZLY_MINOR_VERSION, params);
            _logger.log(Level.SEVERE, emsg);
            throw new UnsupportedOperationException(emsg);
        }
        _logger.log(Level.INFO, getStompBridgeResources().getString(StompBridgeResources.I_INIT_GRIZZLY, Grizzly.getDotedVersion()));

        controller =  new Controller();

        if (_tcpEnabled)  {
            TCPSelectorHandler tcpSelectorHandler = new TCPSelectorHandler();
            BaseSelectionKeyHandler keyHandler = new BaseSelectionKeyHandler();
            keyHandler.setConnectionCloseHandler(this);
            tcpSelectorHandler.setSelectionKeyHandler(keyHandler);
            tcpSelectorHandler.setPort(TCPPORT);
            if (HOST != null) {
                tcpSelectorHandler.setInet(HOST);
            }
            controller.addSelectorHandler(tcpSelectorHandler);

            final ProtocolChain protocolChain = new DefaultProtocolChain();
            ((DefaultProtocolChain)protocolChain).setContinuousExecution(true);
            setProtocolChain(tcpSelectorHandler, protocolChain, null, jmsprop);
        }

        if (_sslEnabled) {
            _logger.log(Level.INFO, getStompBridgeResources().getString(StompBridgeResources.I_INIT_SSL));

            SSLSelectorHandler sslSelectorHandler = new SSLSelectorHandler();
            BaseSelectionKeyHandler keyHandler = new BaseSelectionKeyHandler();
            keyHandler.setConnectionCloseHandler(this);
            sslSelectorHandler.setSelectionKeyHandler(keyHandler);
            sslSelectorHandler.setPort(SSLPORT);
            if (HOST != null) {
                sslSelectorHandler.setInet(HOST);
            }
            controller.addSelectorHandler(sslSelectorHandler);

            Properties sslprops = _bc.getDefaultSSLContextConfig();
            SSLConfig sslcf = new SSLConfig(false);  
            sslcf.setKeyStoreFile(sslprops.getProperty(_bc.KEYSTORE_FILE));
            sslcf.setKeyStorePass(sslprops.getProperty(_bc.TRUSTSTORE_PASSWORD));
            sslcf.setKeyStoreType(sslprops.getProperty(_bc.KEYSTORE_TYPE));
            sslcf.setKeyManagerFactoryAlgorithm(sslprops.getProperty(_bc.KEYSTORE_ALGORITHM));

            sslcf.setTrustStoreFile(sslprops.getProperty(_bc.TRUSTSTORE_FILE));
            sslcf.setTrustStorePass(sslprops.getProperty(_bc.TRUSTSTORE_PASSWORD));
            sslcf.setTrustStoreType(sslprops.getProperty(_bc.TRUSTSTORE_TYPE));
            sslcf.setTrustManagerFactoryAlgorithm(sslprops.getProperty(_bc.TRUSTSTORE_ALGORITHM));

            sslcf.setSecurityProtocol(sslprops.getProperty(_bc.SECURESOCKET_PROTOCOL)); 

            v = props.getProperty(domain+PROP_SSL_REQUIRE_CLIENTAUTH_SUFFIX, "false");
            if (v != null && Boolean.valueOf(v).booleanValue()) {
                sslcf.setNeedClientAuth(true);
            }

            final ProtocolChain protocolChain = new DefaultProtocolChain();
            ((DefaultProtocolChain)protocolChain).setContinuousExecution(true);
            setProtocolChain(sslSelectorHandler, protocolChain, sslcf, jmsprop);
        }

        if (_tcpEnabled) {
            _bc.registerService("stomp[TCP]", "stomp", TCPPORT, null);
        }
        if (_sslEnabled) {
            _bc.registerService("stomp[SSL/TLS]", "stomp", SSLPORT, null);
        }
        _inited = true;
     }

     private void setProtocolChain(SelectorHandler selector, 
                                   final ProtocolChain pc,
                                   SSLConfig sslcf, Properties jmsprop) 
                                   throws Exception {

        ParserProtocolFilter ppf =  new ParserProtocolFilter() {

                                    public ProtocolParser newProtocolParser() {
                                        StompProtocolParser parser = 
                                                      new StompProtocolParser(_bc);
                                        return parser;
                                    }

                                    }; 
        if (sslcf != null) ppf.setSSLConfig(sslcf);
        pc.addFilter(ppf);

        pc.addFilter(new StompProtocolFilter(_bc, jmsprop));

        selector.setProtocolChainInstanceHandler(
            new ProtocolChainInstanceHandler() {
                 public ProtocolChain poll() {
                     return pc;
                 }
                 public boolean offer(ProtocolChain protocolChain) {
                     if (_logger.isLoggable(Level.FINEST)) {
                         _logger.log(Level.FINEST, "ProtocolChain.offer(): return false");
                     }
                     return false;
                }
            });
    }

    protected static Logger logger() {
        return _logger;
    }

    public synchronized void start() {

        if (!_inited) {
            String emsg = getStompBridgeResources().getKString(StompBridgeResources.X_STOMP_SERVER_NO_INIT);
            _logger.log(Level.SEVERE, emsg);
            throw new IllegalStateException(emsg);
        }
        if (_tcpEnabled) {
            _logger.log(Level.INFO, getStompBridgeResources().getString(
            StompBridgeResources.I_START_TRANSPORT, "TCP" , TCPHOSTNAMEPORT));
        }
        if (_sslEnabled) {
            _logger.log(Level.INFO, getStompBridgeResources().getString(
            StompBridgeResources.I_START_TRANSPORT, "SSL/TLS" , SSLHOSTNAMEPORT));
        }

        final CountDownLatch latch = new CountDownLatch(1);

        controller.addStateListener(new ControllerStateListenerAdapter() {

            @Override
            public void onReady() {
                latch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                if (latch.getCount() > 0) {
                    _logger.log(Level.SEVERE, getStompBridgeResources().getKString(
                            StompBridgeResources.E_START_TRANSPORT_FAILED, e.getMessage()), e);
                    latch.countDown();
                } else {
                    _logger.log(Level.SEVERE, getStompBridgeResources().getKString(
                            StompBridgeResources.E_ONEXCEPTION_TRANSPORT, e.getMessage()), e);
                }
            }
        });

        new Thread(controller).start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            _logger.log(Level.WARNING, getStompBridgeResources().getKString( 
                        StompBridgeResources.W_WAIT_FOR_START_INTERRUPTED, e.getMessage()));
        }

        if (!controller.isStarted()) {
            throw new IllegalStateException(getStompBridgeResources().getKString(
                                  StompBridgeResources.X_STOMP_SERVER_START_FAILED));
        } else {
            _logger.log(Level.INFO, getStompBridgeResources().getString(
                StompBridgeResources.I_START_TRANSPORT_OK, String.valueOf(controller.getReadThreadsCount())));
        }
    }

    public synchronized void stop() {

        if (!_inited) {
            String emsg = getStompBridgeResources().getKString(StompBridgeResources.X_STOMP_SERVER_NO_INIT);
            _logger.log(Level.SEVERE, emsg);
            throw new IllegalStateException(emsg);
        }

         _logger.log(Level.INFO, getStompBridgeResources().getString(
                                 StompBridgeResources.I_STOP_STOMP_SERVER));

        try {
            controller.stop();
        } catch(IOException e) {
            _logger.log(Level.WARNING, getStompBridgeResources().getKString(
                    StompBridgeResources.W_EXCEPTION_STOP_SERVER, e.getMessage()));
            e.printStackTrace();
        }
        _logger.log(Level.INFO, getStompBridgeResources().getString(
                                StompBridgeResources.I_STOMP_SERVER_STOPPED));
    }

    protected static Logger getLogger() {
        return _logger;
    }

    protected static MessageTransformer<Message, Message> getMessageTransformer() {
        return _msgTransformer;
    }

    /**
     * Invoked when the a {@link SelectionKey} is cancelled locally, e.g. by
     * one {@link SelectorHandler}, {@link ConnectionHandler} or {@link SelectionKeyHandler}
     *
     * @param key a {@link SelectionKey}
     */
    public void locallyClosed(SelectionKey key) {
        _logger.log(Level.INFO, getStompBridgeResources().getString(
                StompBridgeResources.I_SELECTION_KEY_LOCAL_CLOSED, (key == null ? "":key.toString())));
        if (key == null) return;
        Object o = key.attachment();

        if (o instanceof AttributeHolder) {
            AttributeHolder attr = (AttributeHolder)o;
            StompProtocolHandler sph = (StompProtocolHandler)attr.getAttribute(
                                        StompProtocolFilter.STOMP_PROTOCOL_HANDLER_ATTR);
            if (sph != null) {
                _logger.log(Level.INFO, getStompBridgeResources().getString(
                            StompBridgeResources.I_CLOSE_STOMP_HANDLER, 
                            (sph == null ?"":sph.toString()), (key == null ? "":key.toString())));
                sph.close(true);
            }
        }
    }

    /**
     * Invoked when a remote connection is being closed.
     *
     * @param  key a {@link SelectionKey}
     */
    public void remotlyClosed(SelectionKey key) {
        _logger.log(Level.INFO, getStompBridgeResources().getString(
                StompBridgeResources.I_SELECTION_KEY_REMOTE_CLOSED, (key == null ? "":key.toString())));
        if (key == null) return;

        Object o = key.attachment();
        if (o instanceof AttributeHolder) {
            AttributeHolder attr = (AttributeHolder)o;
            StompProtocolHandler sph = (StompProtocolHandler)attr.getAttribute(
                                        StompProtocolFilter.STOMP_PROTOCOL_HANDLER_ATTR);
            if (sph != null) {
                _logger.log(Level.INFO, getStompBridgeResources().getString(
                            StompBridgeResources.I_CLOSE_STOMP_HANDLER, 
                            (sph == null ?"":sph.toString()), (key == null ? "":key.toString())));
                sph.close(true);
            }
        }
    }

    public static StompBridgeResources getStompBridgeResources() {
        if (_sbr == null) {
            synchronized(StompServer.class) {
                if (_sbr == null) {
                    _sbr = StompBridgeResources.getResources(Locale.getDefault());
                }
            }
        }
        return _sbr;
    }
   
}
