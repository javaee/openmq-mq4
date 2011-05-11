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
 * @(#)Globals.java	1.121 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver;

import java.util.ResourceBundle;
import java.util.Locale;
import java.util.Properties;
import java.util.Enumeration;
import java.net.InetAddress;
import java.io.*;
import com.sun.messaging.jmq.jmsserver.data.PacketRouter;
import com.sun.messaging.jmq.jmsserver.data.TransactionList;
import com.sun.messaging.jmq.jmsserver.service.ConnectionManager;
import com.sun.messaging.jmq.jmsservice.BrokerEvent;
import com.sun.messaging.jmq.jmsserver.service.ServiceManager;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;
import com.sun.messaging.jmq.jmsserver.management.mbeans.resources.MBeanResources;
import com.sun.messaging.jmq.jmsserver.service.PortMapper;
import com.sun.messaging.jmq.jmsserver.service.HAMonitorService;
import com.sun.messaging.jmq.jmsserver.license.*;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.util.LoggerManager;
import com.sun.messaging.jmq.jmsserver.config.BrokerConfig;
import com.sun.messaging.jmq.jmsserver.cluster.*;
import com.sun.messaging.jmq.jmsserver.cluster.ha.*;
import com.sun.messaging.jmq.jmsserver.config.PropertyUpdateException;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.persist.StoreManager;
import com.sun.messaging.jmq.jmsserver.util.MetricManager;
import com.sun.messaging.jmq.jmsserver.util.memory.MemoryManager;
import com.sun.messaging.jmq.jmsserver.util.LockFile;
import com.sun.messaging.jmq.jmsserver.core.ClusterBroadcast;
import com.sun.messaging.jmq.jmsserver.core.ClusterRouter;
import com.sun.messaging.jmq.jmsserver.core.BrokerAddress;

import com.sun.messaging.jmq.jmsserver.audit.MQAuditService;
import com.sun.messaging.jmq.jmsserver.audit.MQAuditSession;

import com.sun.messaging.jmq.jmsserver.multibroker.heartbeat.HeartbeatService;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.util.timer.MQTimer;
import com.sun.messaging.jmq.Version;
import com.sun.messaging.jmq.io.MQAddress;
import com.sun.messaging.jmq.util.BrokerExitCode;
import com.sun.messaging.jmq.jmsserver.management.agent.Agent;
import com.sun.messaging.jmq.util.UID;
import com.sun.messaging.bridge.BridgeServiceManager;

import com.sun.messaging.jmq.jmsserver.data.protocol.Protocol;

import java.io.File;
/**
 * Singleton class which contains any Globals for the
 * system.<P>
 *
 * Other singleton classes which can be considered static
 * once they are retrieved (they do not need to be retrieved
 * from the static method each time they are used) should
 * also be defined here <P>
 */

public class Globals
{
    /**
     * Set of properties to save if save properties flag is true
     * NOTE: * is only supported as the last character at this point
     */
    private static final String[] saveStrings = {
               "imq.cluster.ha",
               "imq.cluster.clusterid",
               "imq.brokerid",
               "imq.persist.store",
               "imq.persist.jdbc.*"
    };

    /**
     * String that prefixes all properties.
     */
    public static final String IMQ = "imq";

    /**
     * Hostname that signifies bind-to-all interfaces
     */
    public static final String HOSTNAME_ALL = "*";

    /**
     * the thread pool OutOfMemory Error handler (if any)
     */
    private static GlobalErrorHandler errhandler = null;

    private static final Object lock = Globals.class;
    private static Globals globals = null;

    private static Protocol protocol = null;
    
    private static BrokerResources br = null;

    private static MBeanResources mbr = null;

    private static Logger logger = null;

    private static Version version = null;

    private static PortMapper portMapper = null;

    private static MQAddress mqAddress = null;

    private static LicenseManager licenseManager = null;

    private static LicenseBase currentLicense = null;

    private static ServiceManager serviceManager = null;

    private static ConnectionManager connectionManager = null;

    private static ClusterBroadcast messageBus = null;
    private static ClusterRouter messageRouter = null;

    private static HeartbeatService heartbeatService = null;

    private static MetricManager metricManager = null;

    private static TransactionList transList = null;

    private static MQTimer timer = null;

    private static boolean clearProps = false;
    private static Properties saveProps = null;

    private static Boolean HAEnabled = null;
    private static Boolean useSharedConfigRecord = null;

    private static String ClusterID = null;

    private static UID HAStoreSession =  new UID(-1);
    private static UID BrokerSession =  null;

    private static String BrokerID = null;

    private static String hostname = null;

    private static String jmxHostname = null;

    private static BrokerAddress myaddr = null;

    private static InetAddress brokerInetAddress = null;

    private static InetAddress jmxInetAddress = null;

    private static PacketRouter[] routers = null;

    private static MemoryManager mem_manager = null;
    private static boolean useMem = true;

    private static MQAuditSession audit = null;

    private static Agent agent = null;

    private static BrokerStateHandler stateHandler = null;

    private static HAMonitorService hasvc = null;

    public static final String INTERNAL_PREFIX = "MQ_";


    //------------------------------------------------------------------------
    //--                 static brokerConfig objects                 --
    //------------------------------------------------------------------------
  
    /**
     * default instance property name. This is the name used for this instance of 
     * the broker IF nothing has been specified on the command line 
     */
    public static final String DEFAULT_INSTANCE = "imqbroker";

    /**
     * instance name used by this BrokerConfig
     */
    private static String configName = DEFAULT_INSTANCE; 

    /**
     * passed in properties
     */
    private static Properties parameters = null; 

    /**
     * singleton instance of BrokerConfig
     */
    private static BrokerConfig config = null;


    /**
     * singleton instance of ClusterManager
     */
    private static ClusterManager clusterConfig = null;

    /**
     */
    private static BridgeServiceManager bridgeManager = null; 

    public static void cleanup()
    {
        clusterConfig = null;
        config = null;
        parameters = null; 
        globals = null;

        br = null;
        logger = null;
        version = null;
        portMapper = null;
        mqAddress = null;
        licenseManager = null;
        currentLicense = null;
        serviceManager = null;
        connectionManager = null;
        messageBus = null;
        messageRouter = null;
        heartbeatService = null;
        metricManager = null;
        transList = null;
        timer = null;
        clearProps = false;
        saveProps = null;
        HAEnabled = null;
        useSharedConfigRecord = null;
        ClusterID = null;
        HAStoreSession =  null;
        BrokerID = null;
        hostname = null;
        jmxHostname = null;
        myaddr = null;
        brokerInetAddress = null;
        jmxInetAddress = null;
        routers = null;
        mem_manager = null;
        MQAuditService.clear();
        audit = null;
        agent = null;
        stateHandler = null;
        pathinited = false;
        bridgeManager = null;

    }

    private Globals() {
    }

    /**
     * 
     * @param params Properties supplied on command line or read from standard input
     * @param clearprops
     * @param saveprops
     */
    public static void init(Properties params, boolean clearprops, boolean saveprops)
    {
        pathinit(null);
        clearProps = clearprops;
        if (params == null) return;

        if (saveprops) {
            saveProps = new Properties();
            for (int i=0; i < saveStrings.length; i++) {
                if (saveStrings[i].endsWith("*")) { // has wildcards
                    // OK - this is a pain, find all matching properties
                    // happily we only support wildcards at the end
                    String match = saveStrings[i].substring(0, saveStrings[i].length() - 1);
                    Enumeration e = params.propertyNames();
                    while (e.hasMoreElements()) {
                        String key = (String)e.nextElement();
                        if (key.startsWith(match)) {
                            String val = params.getProperty(key);
                            saveProps.put(key, val);
                        }
                    }
                    continue;
                }
                String val = params.getProperty(saveStrings[i]);
                if (val != null) saveProps.put(saveStrings[i],
                                 val);
            }
        }

        configName = params.getProperty(IMQ + ".instancename", DEFAULT_INSTANCE);

	// Make sure there is a jmq.home, jmq.varhome and a jmq.instancename
        // property set (these may be used by property variable expansion code).
        params.setProperty(JMQ_VAR_HOME_PROPERTY, JMQ_VAR_HOME);
        params.setProperty(JMQ_LIB_HOME_PROPERTY, JMQ_LIB_HOME);
        params.setProperty(JMQ_ETC_HOME_PROPERTY, JMQ_ETC_HOME);
        params.setProperty(JMQ_INSTANCES_HOME_PROPERTY, JMQ_INSTANCES_HOME);
        params.setProperty(JMQ_HOME_PROPERTY, JMQ_HOME);
        params.setProperty(IMQ + ".instancename", configName);

        parameters = params;


    }

    /**
     * Return whether the property imq.cluster.masterbroker was specified
     * on the command line or read from standard input
     * @return
     */
    public static boolean isMasterBrokerSpecified() {
        if (parameters == null) {
            return false;
        }
        return (parameters.get(ClusterManagerImpl.CONFIG_SERVER) != null);
    }

    /**
     * Return whether the property imq.jmqra.managed was specified
     * on the command line or read from standard input
     * @return
     */
    public static boolean isJMSRAManagedSpecified() {
        if (parameters == null) {
            return false;
        }
        String val = parameters.getProperty(JMSRA_MANAGED_PROPERTY);
        return (val != null && val.trim().toLowerCase().equals("true"));
    }

    public static boolean isJMSRAManagedBroker() {
        return getConfig().getBooleanProperty(JMSRA_MANAGED_PROPERTY, false);
    }


    public static void setMemMgrOn(boolean setting)
    {
        useMem = setting;
    }
        

    public static MemoryManager getMemManager() {
        if (!useMem) return null;
        if (mem_manager == null) {
            synchronized(lock) {
                if (mem_manager == null)
                    mem_manager = new MemoryManager();
            }
        }
        return mem_manager;
    }

    public static void setAgent(Agent ag)  {
        agent = ag;
    }

    public static Agent getAgent()  {
        return (agent);
    }

    public static String getPrimaryOwnerName()  {
        return(Globals.getConfig().getProperty(PRIMARY_OWNER_NAME_PROPERTY,
                       System.getProperty("user.name")));
    }

    public static String getPrimaryOwnerContact()  {
        return(Globals.getConfig().getProperty(PRIMARY_OWNER_CONTACT_PROPERTY,
                       System.getProperty("user.name")));
    }

    public static String[] getBrokerAdminDefinedRoles()  {
        String countPropName = BROKER_ADMIN_DEFINED_ROLES_PROPERTY_BASE + ".count";
        String countStr = Globals.getConfig().getProperty(countPropName);
	String ret[] = null;
	int count = 0;

	if ((countStr == null) || (countStr.equals("")))  {
	    return (getDefaultBrokerAdminDefinedRoles());
	}

	try  {
	    count = Integer.parseInt(countStr);
	} catch(Exception e)  {
            Logger logger = getLogger();
            logger.log(Logger.WARNING, "Invalid value for property "
			+ countPropName
			+ ": "
			+ countStr);
	    return (getDefaultBrokerAdminDefinedRoles());
	}

	if (count == 0)  {
	    return (getDefaultBrokerAdminDefinedRoles());
	}

	ret = new String [ count ];

	for (int i = 0; i < count; ++i)  {
	    String	propName = BROKER_ADMIN_DEFINED_ROLES_PROPERTY_BASE + ".name" + i;

	    ret[i] = Globals.getConfig().getProperty(propName);
	}

	return (ret);
    }

    public static String[] getDefaultBrokerAdminDefinedRoles()  {
        /**
         * Default admin defined role is simply the broker instance name.
         */
        String[] ret = {
		Globals.getConfig().getProperty("imq.instancename")
		};

	return(ret);
    }

    /*
     * Return install root for MQ. Basically returns what imq.home is set to.
     * Used by JESMF. This needs to match what is in the com.sun.cmm.mq.xml
     * file.
     *
     * Added a backdoor (imq.install.root) that can be used to override imq.home
     * since when this is run from a dev workspace, imq.home is set to each
     * dev workspace and won't match what's in com.sun.cmm.mq.xml.
     */
    public static String getInstallRoot()  {
        String installRoot = Globals.getConfig().getProperty(INSTALL_ROOT);

	if (installRoot != null)  {
	    return (installRoot);
	}

        return(Globals.getConfig().getProperty(JMQ_HOME_PROPERTY, "/"));
    }


    public static void setProtocol(Protocol impl) {
        protocol = impl;
    }

    public static Protocol getProtocol() {
       return protocol;
    }

    public static void setBrokerStateHandler(BrokerStateHandler sh)  {
        stateHandler = sh;
    }

    public static BrokerStateHandler getBrokerStateHandler()  {
        return (stateHandler);
    }

    public static void setHAMonitorService(HAMonitorService sh)  {
        hasvc = sh;
    }

    public static HAMonitorService getHAMonitorService()  {
        return (hasvc);
    }

    public static void setBridgeServiceManager(BridgeServiceManager bm)  {
        bridgeManager = bm;
    }

    public static BridgeServiceManager getBridgeServiceManager()  {
        return (bridgeManager);
    }

    public static boolean bridgeEnabled()  {
        return BridgeBaseContextAdapter.bridgeEnabled();
    }

    public static Globals getGlobals() {
        if (globals == null) {
            synchronized(lock) {
                if (globals == null)
                    globals = new Globals();
            }
        }
        return globals;
    }


    public static MQTimer getTimer() {
        return getTimer(false);
    }
    public static MQTimer getTimer(boolean purge) {
        if (timer == null) {
            synchronized(lock) {
                if (timer == null) {
                    timer = new MQTimer(true);
                    timer.setLogger(getLogger());
                    timer.initUncaughtExceptionHandler();
                }
            }
        }
        if (purge) timer.purge();
        return timer;
    }

    public static BrokerResources getBrokerResources() {
	if (br == null) {
            synchronized(lock) {
	        if (br == null) {
	            br = BrokerResources.getResources(
		    Locale.getDefault());
		}
	    }
	}
	return br;
    }

    public static MBeanResources getMBeanResources() {
	if (mbr == null) {
            synchronized(lock) {
	        if (mbr == null) {
	            mbr = MBeanResources.getResources(
		    Locale.getDefault());
		}
	    }
	}
	return mbr;
    }

    public static Logger getLogger() {
	if (logger == null) {
            synchronized(lock) {
	        if (logger == null) {
		    logger = new Logger(JMQ_VAR_HOME);
		    logger.setResourceBundle(getBrokerResources());
		}
	    }
	}
	return logger;
    }

    public static Version getVersion() {
	if (version == null) {
            synchronized(lock) {
	        if (version == null) {
		    version = new Version(false);
		}
	    }
	}
	return version;
    }

    public static PortMapper getPortMapper() {
	if (portMapper == null) {
            synchronized(lock) {
	        if (portMapper == null) {
		    portMapper = new PortMapper(configName);
                    try {
                        portMapper.setParameters(getConfig());
		        // Force portmapper to attempt to bind to port
		        portMapper.bind();
                    } catch (PropertyUpdateException e) {
                        Logger logger = getLogger();
                        logger.log(Logger.ERROR, e.getMessage());
                    }
		}
	    }
	}
	return portMapper;
    }

    /**
     * Get the current license manager object.
     */
    public static LicenseManager getLicenseManager() {
    	if (licenseManager == null) {
	    licenseManager = new LicenseManager();
	}

	return licenseManager;
    }

    /**
     * Get the current broker license.
     */
    public static LicenseBase getCurrentLicense(String licname)
        throws BrokerException {
    	if (currentLicense == null) {
	    currentLicense = getLicenseManager().getLicense(licname);
	}
	return currentLicense;
    }

    /**
     * Get the configured hostname. Can be null of imq.hostname is not
     * configured.
     */
    public static String getHostname() {
        return hostname;
    }

    /**
     * Get the configured hostname for JMX connections/traffic. Can be null 
     * if imq.jmx.hostname or imq.hostname is not configured.
     */
    public static String getJMXHostname() {
	if (jmxHostname != null)
            return jmxHostname;
	
	return (getHostname());
    }

    public static boolean getHAEnabled() {
        if (HAEnabled == null) {
            BrokerConfig conf = Globals.getConfig();
            boolean isHA = conf.getBooleanProperty(Globals.HA_ENABLED_PROPERTY, false);
            String clusterID = conf.getProperty(Globals.CLUSTERID_PROPERTY);
            if (isHA) {
                if (clusterID == null || clusterID.length() == 0) {
                    throw new RuntimeException(
                        Globals.getBrokerResources().getKString(
                            BrokerResources.X_CID_MUST_BE_SET_HA));
                }
                Globals.HAEnabled = Boolean.TRUE;
                Globals.ClusterID = clusterID;
            } else {
                if (clusterID != null && clusterID.length() != 0) {
                    Globals.ClusterID = clusterID;
                }
                Globals.HAEnabled = Boolean.FALSE;
            }
        }
        return HAEnabled.booleanValue();
    }


    public static ServiceManager getServiceManager() {
        return serviceManager;
    }

    public static MetricManager getMetricManager() {
        return metricManager;
    }

    public static TransactionList getTransactionList()
    {
        return transList;
    }

    public static ConnectionManager getConnectionManager() {
        return connectionManager;
    }


    public static ClusterBroadcast getClusterBroadcast() {
        return messageBus;
    }

    public static ClusterRouter getClusterRouter() {
        return messageRouter;
    }

    public static BrokerAddress getMyAddress() {
        return myaddr;
    }


    public static void setHostname(String hostname) {
        Globals.hostname = hostname;
    }

    public static void setJMXHostname(String hostname) {
        Globals.jmxHostname = hostname;
    }

    public static String getClusterID() {
        return ClusterID;
    }


    public static UID getStoreSession() throws BrokerException{
        if (HAStoreSession == null || HAStoreSession.longValue() == -1) {
            throw new BrokerException(
            BrokerResources.E_INTERNAL_ERROR, "HA store session UID has not been initialized"); 
        } 
        return HAStoreSession;
    }

    public static void setStoreSession(UID uid) {
        HAStoreSession = uid;
    }

    public static UID getBrokerSessionID() {
        return BrokerSession;
    }

    public static String getBrokerID()
    {
        if (BrokerID == null) { // hey, calculate it
            BrokerID = Globals.getConfig().getProperty(BROKERID_PROPERTY,
                       Globals.getConfig().getProperty(JDBCBROKERID_PROPERTY));
            //XXX if BrokerID is still null, should we use instancename
        }
        return Globals.BrokerID;
    }

    public static String getIdentityName() {
        String id = Globals.getBrokerID();
        if (id != null) return id;
        return Globals.getConfigName();
    }

    public static void setServiceManager(ServiceManager sm) {
        serviceManager = sm;
    }

    public static void setMetricManager(MetricManager mm) {
        metricManager = mm;
    }

    public static void setTransactionList(TransactionList mm) {
        transList = mm;
    }

    public static void setConnectionManager(ConnectionManager cm) {
        connectionManager = cm;
    }

    public static void setClusterBroadcast(ClusterBroadcast mm) {
        messageBus = mm;
    }
    public static void setClusterRouter(ClusterRouter mm) {
        messageRouter = mm;
    }

    public static void registerHeartbeatService(HeartbeatService hbs) {
        heartbeatService = hbs;
    }

    public static HeartbeatService getHeartbeatService() {
        return heartbeatService;
    }

    public static void setMyAddress(BrokerAddress mm) {
        myaddr = mm;
    }

    /**
     * Set the InetAddress for this broker.
     */
    public static void setBrokerInetAddress(InetAddress ia) {
        brokerInetAddress = ia;
    }

    /**
     * Get the InetAddress for this broker. Must have been previously
     * set
     */
    public static InetAddress getBrokerInetAddress() {
        return brokerInetAddress;
    }

    /**
     * Set the InetAddress for JMX traffic.
     */
    public static void setJMXInetAddress(InetAddress ia) {
        jmxInetAddress = ia;
    }

    /**
     * Get the InetAddress for JMX traffic. Must have been previously
     * set
     */
    public static InetAddress getJMXInetAddress() {
	if (jmxInetAddress != null)  {
            return jmxInetAddress;
	}

	return (getBrokerInetAddress());
    }

    /**
     * Get the hostname that this broker is running on. setBrokerInetAddress
     * must be called before calling this method otherwise this routine
     * will return null.
     */
    public static String getBrokerHostName() {

        if (hostname != null && !hostname.equals(Globals.HOSTNAME_ALL)) {
            return hostname;
        }

        if (brokerInetAddress == null) {
            return null;
        } else {
            // This is fast so we don't need to cache it.
            return brokerInetAddress.getCanonicalHostName();
        }
    }

    public static void setGlobalErrorHandler(GlobalErrorHandler handler) {
        errhandler = handler;
    }    

    public static void handleGlobalError(Throwable thr, String msg) {

        if (!errhandler.handleGlobalError(thr, msg)) {
            logger.logStack(Logger.ERROR,BrokerResources.E_INTERNAL_BROKER_ERROR, "received unexpected exception  ", thr);
            Throwable trace = new Throwable();
            trace.fillInStackTrace();
            logger.logStack(Logger.DEBUG,"Calling stack trace", trace);
        }
    }

    public static void setPacketRouters(PacketRouter[] newrouters) {
        routers = newrouters;
    }

    public static PacketRouter getPacketRouter(int type) 
        throws IndexOutOfBoundsException
    {
        if (routers == null || type > routers.length) {
            throw new IndexOutOfBoundsException(
                Globals.getBrokerResources().getKString(
                    BrokerResources.X_INTERNAL_EXCEPTION,
                "requested invalid packet router " + type ));
        }
        return routers[type];
    }


    //------------------------------------------------------------------------
    //--               static methods for the singleton pattern             --
    //------------------------------------------------------------------------
    
    /**
     * method to return the singleton config class
     */
    public static BrokerConfig getConfig() {
        if (config == null) {
            synchronized (lock) {
                if (config == null) {
                    try {
                        config = new BrokerConfig(configName, parameters, clearProps, saveProps);
                    } catch (BrokerException ex) {
                        getLogger().logStack(Logger.ERROR, "Internal Error: Unable to load broker, configuration properties are not available. Exiting", ex.getCause());
                        Broker.getBroker().exit(-1,
                            "Internal Error: Unable to load broker,"
                            + " configuration properties are not available. Exiting",
                            BrokerEvent.Type.FATAL_ERROR);
                    }


                    // now handle parameters
                    if (parameters != null) {
                        // set any non-jmq properties as system properties

                        Enumeration en = parameters.propertyNames();
                        Properties sysprops = System.getProperties();
                        while (en.hasMoreElements()) {
                            String name = (String)en.nextElement();
                            if (!name.startsWith(IMQ + ".")) {
                                sysprops.put(name, 
                                    parameters.getProperty(name));
                             }
                        }

                    }

		    // First thing we do after reading in configuration
		    // is to initialize the Logger
		    Logger l = getLogger();
		    l.configure(config, IMQ );
		    // LoggerManager will register as a config listener
		    // to handle dynamic updates to logger properties
		    new LoggerManager(logger, config);
		    l.open();
                }
            }
        }
        return config;
    }


    /**
     * method to return the singleton config class
     */
    public static void initClusterManager(MQAddress address)
        throws BrokerException {
        synchronized (lock) {
            if (clusterConfig == null) {
                String classname = Globals.getConfig().
                        getProperty(Globals.IMQ + 
                             ".cluster.manager.class");
                if (getHAEnabled()) {
                    classname = Globals.getConfig().
                        getProperty(Globals.IMQ + 
                         ".hacluster.manager.class");
                }
                try {
                     Class c = Class.forName(classname);
                     clusterConfig = (ClusterManager)
                           c.newInstance();
                     clusterConfig.initialize(address);
                     mqAddress = address;
                     
                     ClusteredBroker bkr = clusterConfig.getLocalBroker();
                     BrokerSession = bkr.getBrokerSessionUID();
                } catch (Exception ex) {
                    if (ex instanceof BrokerException) {
                        // Just re-throw the exception
                        throw (BrokerException)ex;
                    }
                    throw new BrokerException(
                       Globals.getBrokerResources().getKString(
                        BrokerResources.E_INITING_CLUSTER),ex);
                }
            }
        }
    }
    public static ClusterManager getClusterManager() {
        return clusterConfig;
    }


    /**
     * METHOD FOR UNIT TEST ONLY <P>
     * method to re-initialize the config singleton config class (for testing)
     * @param name the name used by the broker, passed in at startup
     */
    public static void reInitializeConfig(String name) {
        config = null;
        if (name == null) name = DEFAULT_INSTANCE;
        configName = name;
    }

    /**
     * method to return the current name of this broker
     */
    public static String getConfigName() {
        return configName;
    }

    /**
     * method to return path name of the instance directory
     */
    public static String getInstanceDir() {
        return JMQ_INSTANCES_HOME + File.separator + configName;
    }

    /**
     * method to return path name of the instance/etc directory
     */
    public static String getInstanceEtcDir() {
        return JMQ_INSTANCES_HOME + File.separator + configName +
			File.separator + JMQ_ETC_HOME_default_etc;
    }

    /**
     * method to return the singleton Store instance
     */
    public static Store getStore() throws BrokerException {
	return StoreManager.getStore();
    }

    /**
     * Get audit session
     */
    public static MQAuditSession getAuditSession() {
        if (audit == null) {
            synchronized(lock) {
                if (audit == null) {
                    MQAuditService.init();
                    try {
                    	audit = MQAuditService.getAuditSession();
                    	LockFile lf = LockFile.getCurrentLockFile();
                		if (lf != null) {
                			audit.setInstance(lf.getInstance(),lf.getHost(), lf.getPort());
						}
                    } catch (BrokerException ex) {
                    	getLogger().logStack(Logger.ERROR, ex.toString(), ex);
                        Broker.getBroker().exit(BrokerExitCode.ERROR, ex.toString(), BrokerEvent.Type.EXCEPTION);
                    }
                }
            }
        }
        return audit;
    } 

    /**
     * method to release the singleton Store instance
     */
    public static void releaseStore() throws BrokerException {
	StoreManager.releaseStore(true); // always do clean up
    }

    public static void setMQAddress(MQAddress addr) {
        ClusterManager c = getClusterManager();
        try {
            c.setMQAddress(addr);
        } catch (Exception ex) {
            logger.log(logger.INFO,
                BrokerResources.E_INTERNAL_BROKER_ERROR,
                    "Received bad address " + addr +
                            " ignoring", ex);
            return;
        }
        mqAddress = addr;
    }
    public static MQAddress getMQAddress() {
        return mqAddress;
    }

    public static boolean nowaitForMasterBroker() {
        return getConfig().getBooleanProperty(NOWAIT_MASTERBROKER_PROP, false);
    }

    public static boolean dynamicChangeMasterBrokerEnabled() {
        return getConfig().getBooleanProperty(
            DYNAMIC_CHANGE_MASTERBROKER_ENABLED_PROP, false);
    }

    public static boolean useMasterBroker() {
        if (getHAEnabled()) {
            return false;
        }
        if (useSharedConfigRecord()) {
            return false;
        }
        return (getClusterManager().getMasterBroker() != null);
    }
 
    public static boolean useSharedConfigRecord() {
        if (useSharedConfigRecord == null) {
            if (getHAEnabled()) {
                useSharedConfigRecord = Boolean.FALSE;
            } else {
                boolean nomb = getConfig().getBooleanProperty(
                               Globals.NO_MASTERBROKER_PROP, false);
                if (nomb) {
                    if (getClusterID() == null) {
                        throw new RuntimeException(
                            Globals.getBrokerResources().getKString(
                            BrokerResources.X_CID_MUST_BE_SET_NOMASTER,
                            Globals.CLUSTERID_PROPERTY,
                            Globals.NO_MASTERBROKER_PROP+"=true"));
                    }
                    useSharedConfigRecord = Boolean.TRUE;
                } else {
                    useSharedConfigRecord = Boolean.FALSE;
                }
            }
        }
        return useSharedConfigRecord.booleanValue();
    }

    public static boolean isConfigForCluster() {
        return (getHAEnabled() || 
                getConfig().getProperty(AUTOCONNECT_CLUSTER_PROPERTY) != null ||
                getConfig().getProperty(MANUAL_AUTOCONNECT_CLUSTER_PROPERTY) != null); 
    }

    /*---------------------------------------------
     *          global static variables
     *---------------------------------------------*/

    /**
     * system property name for the non-editable JMQ home location
     */
    public static final String JMQ_HOME_PROPERTY=IMQ + ".home";

    /**
     * system property name for the editable JMQ home location
     */
    public static final String JMQ_VAR_HOME_PROPERTY=IMQ + ".varhome";

    /**
     * system property name for the editable IMQ instances home location
     */
    public static final String JMQ_INSTANCES_HOME_PROPERTY=IMQ + ".instanceshome";

    /**
     * system property name for the /etc location
     */
    public static final String JMQ_ETC_HOME_PROPERTY=IMQ + ".etchome";

    /**
     * system property name for the /usr/share/lib location
     */
    public static final String JMQ_LIB_HOME_PROPERTY=IMQ + ".libhome";

    /**
     * default value for the non-editable JMQ home location (used if
     * the system property is not set)
     */
    public static final String JMQ_HOME_default = ".";

    /**
     * default value for the non-editable JMQ home location (used if
     * the system property is not set)
     */
    public static final String JMQ_VAR_HOME_default = "var";

    /**
     * default value for the etc JMQ home location (used if
     * the system property is not set). This is the second
     * location to try.
     */
    public static final String JMQ_ETC_HOME_default_etc = "etc";

    /**
     * default value for the etc JMQ home location (used if
     * the system property is not set) - this is the first location
     * to try.
     */
    public static final String JMQ_ETC_HOME_default_etcmq = "etc/mq";

    /**
     * location the configuration is using for the non-editable home location
     */
    public static String JMQ_HOME; 

    /**
     * location the configuration is using for the editable home location
     */
    public static String JMQ_VAR_HOME;

    /**
     * location the configuration is using for the etc home location
     */
    public static String JMQ_ETC_HOME;

    /**
     * location the configuration is using for the share lib home location
     */
    public static String JMQ_LIB_HOME;


    /**
     * location for storing instance specific data
     */
    public static final String INSTANCES_HOME_DIRECTORY="instances";

    public static String JMQ_INSTANCES_HOME;

    public static boolean pathinited = false;

    public static void pathinit(Properties props)
    {
        if (pathinited) return;
        pathinited = true;
        if (props == null)
            props = System.getProperties();
        String path = props.getProperty(JMQ_HOME_PROPERTY,JMQ_HOME_default);
        try {
             path = new File(path).getCanonicalPath();
        } catch (IOException ex) {
             logger.log(Logger.ERROR, BrokerResources.E_BAD_JMQHOME,
                   path, ex);
        }
        JMQ_HOME = path ; 

        path = props.getProperty(JMQ_VAR_HOME_PROPERTY,JMQ_HOME + File.separator + JMQ_VAR_HOME_default);
        try {
             path = new File(path).getCanonicalPath();
        } catch (IOException ex) {
             logger.log(Logger.ERROR, BrokerResources.E_BAD_JMQVARHOME,
                   path, ex);
        }
        JMQ_VAR_HOME = path ; 

        path = props.getProperty(JMQ_LIB_HOME_PROPERTY,JMQ_HOME + File.separator + "lib");
        try {
             path = new File(path).getCanonicalPath();
        } catch (IOException ex) {
             logger.log(Logger.ERROR, BrokerResources.E_BAD_JMQLIBHOME,
                   path, ex);
        }
        JMQ_LIB_HOME = path ; 

        // BUG: 6812136
        // if would be nice if the right etc home is passed in, but if its not
        // look in two places (etc/mq and etc)
        // this addresses the case where an inprocess broker doesn't set etchome
        // and we have to try and find the right one
        path = props.getProperty(JMQ_ETC_HOME_PROPERTY);
        // see if valid
        if (path != null) {
            try {
                File f = new File(path);
                if (!f.exists()) {
                     Globals.getLogger().log(Logger.ERROR, BrokerResources.E_BAD_JMQETCHOME,
                       path);
                } else {
                    path = new File(path).getCanonicalPath();
                }
            } catch (IOException ex) {
                 Globals.getLogger().log(Logger.ERROR, BrokerResources.E_BAD_JMQETCHOME,
                       path, ex);
            }
        } else { // default case - try both
            //first try etcmq
            path = JMQ_HOME + File.separator + JMQ_ETC_HOME_default_etcmq;
            File f = new File(path);
            if (!f.exists()) {
                path = JMQ_HOME + File.separator + JMQ_ETC_HOME_default_etc;
                f = new File(path);
            }
            try {
                path = f.getCanonicalPath();
            } catch (IOException ex) {
                 logger.log(Logger.ERROR, BrokerResources.E_BAD_JMQETCHOME,
                       path, ex);
            }
        }
        JMQ_ETC_HOME = path ; 

        JMQ_INSTANCES_HOME=JMQ_VAR_HOME + File.separator
                 + INSTANCES_HOME_DIRECTORY;
    }

    /**
     * subdirectory under either the editable or non-editable location where the 
     * configuration files are location
     */
    public static final String JMQ_BROKER_PROP_LOC = "props"+File.separator + "broker"+File.separator;


    public final static String
        KEYSTORE_USE_PASSFILE_PROP = Globals.IMQ + ".passfile.enabled",
        KEYSTORE_PASSDIR_PROP      = Globals.IMQ + ".passfile.dirpath",
        KEYSTORE_PASSFILE_PROP     = Globals.IMQ + ".passfile.name";

    /**
     * If this property is set to true then the broker will read properties (including passwords) from standard input
     */
    public static final String READ_PROPERTIES_FROM_STDIN = Globals.IMQ + ".readstdin.enabled";

     //--------------------------------------------------------------
     // HA property names
     //--------------------------------------------------------------
    /**
     * The property name to retrieve this brokers id.
     */
    public static final String BROKERID_PROPERTY =
        Globals.IMQ + ".brokerid";

    /**
     * The property name to retrieve this brokers id.
     */
    public static final String JDBCBROKERID_PROPERTY =
        Globals.IMQ + ".persist.jdbc.brokerid";

    /**
     * The property name to retrieve the cluster's id.
     */
    public static final String CLUSTERID_PROPERTY =
        Globals.IMQ + ".cluster.clusterid";

    /**
     * The property name to retrieve if HA is enabled.
     */
    public static final String HA_ENABLED_PROPERTY =
         Globals.IMQ + ".cluster.ha";

    /**
     * The property name for this broker's primary owner name.
     * This defaults to the value of the system property user.name.
     *
     * Brokers can be run for different applications, for different
     * projects, this property helps identify the person/owner
     * of a particular broker. This is currently only used by
     * JESMF.
     */
    public static final String PRIMARY_OWNER_NAME_PROPERTY =
        Globals.IMQ + ".primaryowner.name";

    /**
     * The property name for this broker's primary owner contact info.
     * This defaults to the value of the system property user.name.
     *
     * Brokers can be run for different applications, for different
     * projects, this property helps identify the person/owner's
     * contact info of a particular broker. This is currently only 
     * used by JESMF.
     */
    public static final String PRIMARY_OWNER_CONTACT_PROPERTY =
        Globals.IMQ + ".primaryowner.contact";

    /**
     * Property base name for the admin defined roles for the broker.
     * Example setting of this:
     *
     *  imq.broker.adminDefinedRoles.count=2
     *  imq.broker.adminDefinedRoles.name0=JMS provider for domain1 appserver instance on host myhost
     *  imq.broker.adminDefinedRoles.name1=Used by test harness running on host myhost
     *
     * This is currently used by JESMF.
     */
    public static final String BROKER_ADMIN_DEFINED_ROLES_PROPERTY_BASE =
        Globals.IMQ + ".broker.adminDefinedRoles";

    /**
     * Property name for the install root for MQ.
     *
     * This is currently only used by JESMF.
     */
    public static final String INSTALL_ROOT =
        Globals.IMQ + ".install.root";

    public static final String NOWAIT_MASTERBROKER_PROP =
        IMQ + ".cluster.nowaitForMasterBroker";

    public static final String DYNAMIC_CHANGE_MASTERBROKER_ENABLED_PROP =
        IMQ + ".cluster.dynamicChangeMasterBrokerEnabled";

    public static final String NO_MASTERBROKER_PROP =
        IMQ + ".cluster.nomasterbroker";

    public static final String AUTOCONNECT_CLUSTER_PROPERTY =
        IMQ + ".cluster.brokerlist";

    public static final String MANUAL_AUTOCONNECT_CLUSTER_PROPERTY =
        IMQ + ".cluster.brokerlist.manual";

    public static final String JMSRA_MANAGED_PROPERTY = IMQ + ".jmsra.managed";

    public static boolean txnLogEnabled() {
        return StoreManager.txnLogEnabled();
    }
    
    public static boolean isNewTxnLogEnabled() {
        return StoreManager.newTxnLogEnabled();
    }

    // whether non-transacted persistent message sent should be logged
    private static Boolean _logNonTransactedMsgSend = null;

    public static final String LOG_NONTRANSACTEDMSGSEND_PROP =
        Globals.IMQ + ".persist.file.txnLog.nonTransactedMsgSend.enabled";

    public static boolean logNonTransactedMsgSend() {

        if (_logNonTransactedMsgSend == null) {
            _logNonTransactedMsgSend = Boolean.valueOf(
                txnLogEnabled() &&
                getConfig().getBooleanProperty(LOG_NONTRANSACTEDMSGSEND_PROP));
        }
        return _logNonTransactedMsgSend.booleanValue();
    }
    
   
    
    
    /**
     * whether to use minimum write optimisations
     */
    private static Boolean _minimizeWrites = null;

    public static final String MINIMIZE_WRITES_PROP =
        Globals.IMQ + ".persist.file.minimizeWrites";

    public static boolean isMinimizeWrites() {

        if (_minimizeWrites == null) {
        	_minimizeWrites = Boolean.valueOf(
                getConfig().getBooleanProperty(MINIMIZE_WRITES_PROP,false));
         getLogger().log(Logger.INFO, "imq.persist.file.minimizeWrites="+_minimizeWrites);
        }
        return _minimizeWrites.booleanValue();
    }
    
    
    /**
     * whether delivery data is persisted
     */
    private static Boolean _deliveryStateNotPersisted = null;

    public static final String DELIVERY_STATE_NOT_PERSITED_PROP =
        Globals.IMQ + ".persist.file.deliveryStateNotPersisted";

    public static boolean isDeliveryStateNotPersisted() {

        if (_deliveryStateNotPersisted == null) {
        	_deliveryStateNotPersisted = Boolean.valueOf(
                getConfig().getBooleanProperty(DELIVERY_STATE_NOT_PERSITED_PROP));
         }
        return _deliveryStateNotPersisted.booleanValue();
    }
    
    /**
     * Return whether properties (including passwords) should be read from stdin at broker startup
     * @return
     */
    public static boolean isReadPropertiessFromStdin(){
    	return getConfig().getBooleanProperty(READ_PROPERTIES_FROM_STDIN);
    }
   
}

