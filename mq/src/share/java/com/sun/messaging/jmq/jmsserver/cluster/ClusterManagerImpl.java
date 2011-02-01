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
 * @(#)ClusterManagerImpl.java	1.36 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.cluster;

import com.sun.messaging.jmq.io.MQAddress;
import com.sun.messaging.jmq.util.log.*;
import java.util.*;
import com.sun.messaging.jmq.util.CacheHashMap;
import com.sun.messaging.jmq.jmsserver.config.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.core.BrokerMQAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.util.UID;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;


/**
 * This class represents the non-ha implementation of ClusterManager.
 * Configuration information is not retrieved from the store.<p>
 */
public class ClusterManagerImpl implements ClusterManager, ConfigListener
{

    static final String DEBUG_ALL_PROP = Globals.IMQ + ".cluster.debug.all";
    public static boolean debug_CLUSTER_ALL = Globals.getConfig().getBooleanProperty(DEBUG_ALL_PROP);
    public static boolean DEBUG_CLUSTER_ALL = debug_CLUSTER_ALL;

    static final String DEBUG_LOCK_PROP = Globals.IMQ + ".cluster.debug.lock";
    public static boolean debug_CLUSTER_LOCK = Globals.getConfig().getBooleanProperty(DEBUG_LOCK_PROP);
    public static boolean DEBUG_CLUSTER_LOCK = debug_CLUSTER_LOCK;

    static final String DEBUG_TXN_PROP = Globals.IMQ + ".cluster.debug.txn";
    public static boolean debug_CLUSTER_TXN = Globals.getConfig().getBooleanProperty(DEBUG_TXN_PROP);
    public static boolean DEBUG_CLUSTER_TXN = debug_CLUSTER_TXN;

    static final String DEBUG_TAKEOVER_PROP = Globals.IMQ + ".cluster.debug.takeover";
    public static boolean debug_CLUSTER_TAKEOVER = Globals.getConfig().getBooleanProperty(DEBUG_TAKEOVER_PROP);
    public static boolean DEBUG_CLUSTER_TAKEOVER = debug_CLUSTER_TAKEOVER;

    static final String DEBUG_MSG_PROP = Globals.IMQ + ".cluster.debug.msg";
    public static boolean debug_CLUSTER_MSG = Globals.getConfig().getBooleanProperty(DEBUG_MSG_PROP);
    public static boolean DEBUG_CLUSTER_MSG = debug_CLUSTER_MSG;


    static final String DEBUG_CONN_PROP = Globals.IMQ + ".cluster.debug.conn";
    public static boolean debug_CLUSTER_CONN = Globals.getConfig().getBooleanProperty(DEBUG_CONN_PROP);
    public static boolean DEBUG_CLUSTER_CONN = debug_CLUSTER_CONN;

    static final String DEBUG_PING_PROP = Globals.IMQ + ".cluster.debug.ping";
    public static boolean debug_CLUSTER_PING = Globals.getConfig().getBooleanProperty(DEBUG_PING_PROP);
    public static boolean DEBUG_CLUSTER_PING = debug_CLUSTER_PING;

    static final String DEBUG_PKT_PROP = Globals.IMQ + ".cluster.debug.packet";
    public static boolean debug_CLUSTER_PACKET = Globals.getConfig().getBooleanProperty(DEBUG_PKT_PROP);
    public static boolean DEBUG_CLUSTER_PACKET = debug_CLUSTER_PACKET;


   /**
    * Turns on/off debugging.
    */
   private static boolean DEBUG = false;

   /**
    * The basic configuration (name, value pairs) for the
    * broker.
    */
   protected BrokerConfig config = Globals.getConfig();

   /**
    * The class used for logging. 
    */
   protected Logger logger = Globals.getLogger();

   /**
    * The set of listeners waiting state or configuration
    * changed information. 
    * @see ClusterListener
    */
   protected Set listeners = null;

   /**
    * The current transport type for the cluster. 
    */
   protected String transport = null;

   /**
    * The current hostname used for the cluster service.  
    * A value of null indicates bind to all hosts.
    */
   protected String clusterhost = null;

   /**
    * The current port used for the cluster service.
    * A value of 0 indicates that the value should be obtained 
    * dynamically.
    */
   protected int clusterport = 0;

   /**
    * This is a private initialization flag.
    */
   protected boolean initialized = false;

   /**
    * This is the brokerid for the local broker.
    */
   protected String localBroker = null;

   /**
    * The brokerid for the master broker,
    * null indicates no master broker.
    */
   protected String masterBroker = null;

   /**
    * The list of all brokers in the cluster.
    * The list contains ClusteredBroker objects.
    * @see ClusteredBroker
    */
   protected Map allBrokers = null;


    /**
     * This is the property name used to set the list of brokers
     * in a cluster. This property is only the list of
     * brokers defined on the command line and does NOT
     * include any brokers passed in with -cluster.
     */
    public static final String AUTOCONNECT_PROPERTY =
        Globals.AUTOCONNECT_CLUSTER_PROPERTY;

    protected static final String MANUAL_AUTOCONNECT_PROPERTY =
        Globals.MANUAL_AUTOCONNECT_CLUSTER_PROPERTY;


    /**
     * Private property name used to set the number of entries
     * in the session list.
     */
    protected static final String MAX_OLD_SESSIONS =
        Globals.IMQ + ".cluster.maxTakeoverSessions";

    protected int maxTakeoverSessions = Globals.getConfig().getIntProperty(
                    MAX_OLD_SESSIONS, 10);

    /**
     * list of the last X sessions takeover by this broker
     */
    protected CacheHashMap oldSessionMap = new CacheHashMap(maxTakeoverSessions);

    /**
     * The id of the cluster.
     */
    private String clusterid = Globals.getClusterID();


    private BrokerResources br = Globals.getBrokerResources();

    /**
     * Creates a ClusterManagerImpl. 
     */
    public ClusterManagerImpl()
    {
       listeners = new LinkedHashSet();  
    } 

   /**
    * Retrieves the cluster id associated with this cluster. 
    *
    * @return the id or null if this is not an HA cluster
    */
   public String getClusterId() {
       return clusterid;
   }



   /**
    * Internal method to return the map which contains
    * all brokers. This method may be overridden by
    * sub-classes.
    */
   protected Map getAllBrokers()
        throws BrokerException
   {

       return new HashMap();
   }


   /**
    * Changes the host/port of the local broker. 
    * 
    * @param address MQAddress to the portmapper
    * @throws BrokerException if something goes wrong
    *         when the address is changed
    */
   public void setMQAddress(MQAddress address)
       throws Exception 
   {
       if (!initialized) {
           initialize(address); // sets up cluster state
       } else {
           mqAddressChanged(address);
       }
   }

   /**
    * Retrieves the host/port of the local broker.
    * 
    * @return the MQAddress to the portmapper
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public MQAddress getMQAddress() {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       return getLocalBroker().getBrokerURL();
   }

      
   /**
    * Sets a listener for notification when the state/status
    * or configuration of the cluster changes. 
    * 
    * <p>
    * This api is used by the Monitor Service to determine when
    * a broker should be monitored because it may be down.
    *
    * @see  ClusterListener
    * @param listener the listener to add
    */
   public void addEventListener(ClusterListener listener)
   {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

   /**
    * Removes a listener for notification when the state changes.
    * 
    * <p>
    * This api is used by the Monitor Service to determine when
    * a broker should be monitored because it may be down.
    *
    * @return true if the item existed and was removed.
    * @see  ClusterListener
    * @param listener the listener to remove
    */
   public boolean removeEventListener(ClusterListener listener)
   {
        synchronized (listeners) {
            return listeners.remove(listener);
        }
   }
   
   /**
    * Retrieves the ClusteredBroker which represents
    * this broker.
    *
    * @return the local broker
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    * @see ClusterManagerImpl#getBroker(String)
    */
   public ClusteredBroker getLocalBroker()
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");
       return getBroker(localBroker);
   }
   
   /**
    * Returns the current number of brokers in the
    * cluster. In a non-ha cluster, this includes all
    * brokers which have a BrokerLink to the local broker and
    * the local broker.
    * @return count of all brokers in the cluster. 
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public int getKnownBrokerCount()
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       synchronized (allBrokers) {
           return allBrokers.size();
       }
   }

   /**
    * Returns the current number of brokers in the
    * configuration propperties. In a non-ha cluster, this includes all
    * brokers listed by -cluster or the cluster property.
    * @return count of configured brokers in the cluster. 
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public int getConfigBrokerCount()
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       int cnt = 0;
       synchronized (allBrokers) {
           Iterator itr = allBrokers.values().iterator();
           while (itr.hasNext()) {
               ClusteredBroker cb = (ClusteredBroker)itr.next();
               if (cb.isConfigBroker())
                   cnt ++;
           }
       }
       return cnt;
   }

   /**
    * Returns the current number of brokers in the
    * cluster. In a non-ha cluster, this includes all
    * brokers which have an active BrokerLink to the local broker and
    * the local broker.
    * @return count of all brokers in the cluster. 
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public int getActiveBrokerCount()
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       int cnt = 0;
       synchronized (allBrokers) {
           Iterator itr = allBrokers.values().iterator();
           while (itr.hasNext()) {
               ClusteredBroker cb = (ClusteredBroker)itr.next();
               if (BrokerStatus.getBrokerLinkIsUp(cb.getStatus()))
                   cnt ++;
           }
       }
       return cnt;
   }
         
   /**
    * Returns an iterator of ClusteredBroker objects for
    * all brokers in the cluster. This is a copy of
    * the current list. 
    * 
    * @param refresh if true refresh current list then return it
    * @return iterator of ClusteredBrokers
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public Iterator getKnownBrokers(boolean refresh)
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       HashSet brokers = null;
       synchronized (allBrokers) {
           brokers = new HashSet(allBrokers.values());
       }
       return brokers.iterator();
   }

         

   class ConfigInterator implements Iterator
   {
       Object nextObj = null;
       Iterator parent = null;

       public ConfigInterator(Iterator parentItr)
       {
           parent = parentItr;
       }

       public boolean hasNext() {
           if (nextObj != null) return true;
           if (nextObj == null && !parent.hasNext())
                return false;
           while (nextObj == null) {
               if (!parent.hasNext()) break;
               nextObj = parent.next();
               ClusteredBroker cb = (ClusteredBroker)nextObj;
               if (!cb.isConfigBroker()) {
                   parent.remove();
                   nextObj = null; // not valid
               }
           }
           return nextObj != null;
       }
       public Object next() {
           // ok, skip to the right location
           if (!hasNext())
               throw new NoSuchElementException("no more");
           Object ret = nextObj;
           nextObj = null;
           return ret;           
       }

       public void remove() {
            parent.remove();
       }
    }

   /**
    * Returns an iterator of ClusteredBroker objects for
    * all brokers in the cluster. This is a copy of
    * the current list and is accurate at the time getBrokers was
    * called.
    * 
    * @return iterator of ClusteredBrokers
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public Iterator getConfigBrokers()
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       HashSet brokers = null;
       synchronized (allBrokers) {
           brokers = new HashSet(allBrokers.values());
           return new ConfigInterator(brokers.iterator());
       }

   }

   class ActiveInterator implements Iterator

   {
       Object nextObj = null;
       Iterator parent = null;

       public ActiveInterator(Iterator parentItr)
       {
           parent = parentItr;
       }

       public boolean hasNext() {
           if (nextObj != null) return true;
           if (nextObj == null && !parent.hasNext())
                return false;
           while (nextObj == null) {
               if (!parent.hasNext()) break;
               nextObj = parent.next();
               ClusteredBroker cb = (ClusteredBroker)nextObj;
               if (BrokerStatus.getBrokerLinkIsDown(cb.getStatus())) {
                   parent.remove();
                   nextObj = null; // not valid
               }
           }
           return nextObj != null;
       }
       public Object next() {
           // ok, skip to the right location
           if (!hasNext())
               throw new NoSuchElementException("no more");
           Object ret = nextObj;
           nextObj = null;
           return ret;           
       }

       public void remove() {
            parent.remove();
       }
    }


   /**
    * Returns an iterator of ClusteredBroker objects for
    * all active brokers in the cluster. This is a copy of
    * the current list and is accurate at the time getBrokers was
    * called.
    * 
    * @return iterator of ClusteredBrokers
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public Iterator getActiveBrokers()
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       HashSet brokers = null;
       synchronized (allBrokers) {
           brokers = new HashSet(allBrokers.values());
           return new ActiveInterator(brokers.iterator());
       }
   }
         
   /**
    * Returns a specific ClusteredBroker object by name.
    * 
    * @param brokerid the id associated with the broker
    * @return the broker associated with brokerid or null
    *         if the broker is not found
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public ClusteredBroker getBroker(String brokerid)
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       synchronized (allBrokers) {
           return (ClusteredBroker)allBrokers.get(brokerid);
       }
   }
         
         
   /**
    * Method used in a dynamic cluster, it updates the
    * system when a new broker is added.
    *
    * @param URL the MQAddress of the new broker
    * @param uid the brokerSessionUID associated with this broker (if known)
    * @param instName the instance name of the broker to be activated
    * @param userData optional data associated with the status change
    * @throws NoSuchElementException if the broker can not
    *              be added to the cluster (for example if
    *              the cluster is running in HA mode and
    *              the URL is not in the shared database)
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    * @return the uid associated with the new broker
    */
   public String activateBroker(MQAddress URL, UID uid, 
                                String instName, Object userData)
                                throws NoSuchElementException,
                                       BrokerException
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       // does it exist yet ?
       String brokerid = lookupBrokerID(URL);
       if (brokerid == null) {
           brokerid = addBroker(URL, false, false, uid);
       }
       
       return activateBroker(brokerid, uid, instName, userData );
    }


   /**
    * method used in a all clusters, it updates the
    * system when a new broker is added.
    *
    * @param brokerid the id of the broker (if known)
    * @param uid the broker sessionUID
    * @param instName the broker instance name 
    * @param userData optional data associated with the status change
    * @throws NoSuchElementException if the broker can not
    *              be added to the cluster (for example if
    *              the cluster is running in HA mode and
    *              the brokerid is not in the shared database)
    * @throws BrokerException if the database can not be accessed
    * @return the uid associated with the new broker
    */
   public String activateBroker(String brokerid, UID uid, 
                                String instName, Object userData)
                                throws NoSuchElementException, 
                                BrokerException
   {
       ClusteredBroker cb = getBroker(brokerid);
       if (cb == null) {
           throw new BrokerException("Unknown broker " + brokerid);
       }
       cb.setInstanceName(instName);
       if (uid != null)
           cb.setBrokerSessionUID(uid);
       cb = updateBroker(cb);
       cb.setStatus(BrokerStatus.ACTIVATE_BROKER, userData);
       return brokerid;

   }

    /**
     * protected method used to update a newly activated
     * broker if necessary
     */
    protected ClusteredBroker updateBroker(ClusteredBroker broker) {
      
        // does nothing
        return broker;
    }

   
   protected String addBroker(MQAddress URL, boolean isLocal, boolean config, 
                    UID sid)
             throws NoSuchElementException, BrokerException
   {
         ClusteredBroker cb = new ClusteredBrokerImpl(URL, isLocal, sid);
         ((ClusteredBrokerImpl)cb).setConfigBroker(config);
         synchronized (allBrokers) {
             allBrokers.put(cb.getBrokerName(), cb);
         }
         brokerChanged(ClusterReason.ADDED, 
                cb.getBrokerName(), null, cb, sid, null);

         return cb.getBrokerName();

   }

   /**
    * method used in a dynamic cluster, it updates the
    * system when a broker is removed.
    *
    * @param URL the MQAddress associated with the broker
    * @param userData optional data associated with the status change
    * @throws NoSuchElementException if the broker can not
    *              be found in the cluster.
    */
   public void deactivateBroker(MQAddress URL, Object userData)
       throws NoSuchElementException
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       // does it exist yet ?
       String brokerid = lookupBrokerID(URL);
       if (brokerid == null) 
           throw new NoSuchElementException("Unknown URL " + brokerid);
       deactivateBroker(brokerid, userData);
    }       

   /**
    * Method used in a dynamic cluster, it updates the
    * system when a broker is removed.
    *
    * @param brokerid the id associated with the broker
    * @param userData optional data associated with the status change
    * @throws NoSuchElementException if the broker can not
    *              be found in the cluster.
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public void deactivateBroker(String brokerid, Object userData)
       throws NoSuchElementException
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       ClusteredBroker cb = null;

       boolean removed = false;

       synchronized (allBrokers) {
           cb = (ClusteredBroker)allBrokers.get(brokerid);
           if (cb == null) throw new NoSuchElementException("Unknown Broker" + brokerid);
           if (!cb.isConfigBroker()) { // remove it if its dynamic
               allBrokers.remove(brokerid);
               removed = true;
           }
           cb.setInstanceName(null);

       }
       // OK, set the broker link down
       cb.setStatus(BrokerStatus.setBrokerLinkIsDown(
                          cb.getStatus()), userData);
       if (removed)
           brokerChanged(ClusterReason.REMOVED, cb.getBrokerName(),
                        cb, null, cb.getBrokerSessionUID(),  null);

   }     


   /**
    * Finds the brokerid associated with the given host/port.
    *
    * @param broker the MQAddress of the new broker
    * @return the id associated with the broker or null if the broker does not exist
    * @throws RuntimeException if the cluster has not be initialized
    *              (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */  
   public String lookupBrokerID(MQAddress broker)
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       // the safe thing to do is to iterate
       synchronized (allBrokers) {
           Iterator itr = allBrokers.values().iterator();
           while (itr.hasNext()) {
            
               ClusteredBroker cb = (ClusteredBroker)itr.next();
               MQAddress addr = cb.getBrokerURL();
               if (addr.equals(broker))
                   return cb.getBrokerName();
           }
       }
       return null;
   }

   /**
    * finds the brokerid associated with the given session.
    *
    * @param uid is the session uid to search for
    * @return the uid associated with the session or null we cant find it.
    */
   public String lookupStoreSessionOwner(UID uid) {
       return null;
   }

   public String getStoreSessionCreator(UID uid) {
       return null;
   }

   /**
    * finds the brokerid associated with the given session.
    *
    * @param uid is the session uid to search for
    * @return the uid associated with the session or null we cant find it.
    */
   public String lookupBrokerSessionUID(UID uid) {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       // the safe thing to do is to iterate
       synchronized (allBrokers) {
           Iterator itr = allBrokers.values().iterator();
           while (itr.hasNext()) {
            
               ClusteredBroker cb = (ClusteredBroker)itr.next();
               UID buid = cb.getBrokerSessionUID();
               if (buid.equals(uid))
                   return cb.getBrokerName();
           }
       }
       return null;
   }

   /**
    * @return true if allow configured master broker
    */

   protected boolean allowMasterBroker() {
       return true;
   }

   /**
    * The master broker in the cluster (if any).
    *
    * @return the master broker (or null if none)
    * @see ClusterManagerImpl#getBroker(String)
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public ClusteredBroker getMasterBroker()
   {
       if (masterBroker == null) return null;

       return getBroker(masterBroker);
   }


   /**
    * The transport (as a string) used by
    * the cluster of brokers.
    *
    * @return the transport (tcp, ssl)
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */

   public String getTransport()
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       return transport;
   }


   /**
    * Returns the port configured for the cluster service.
    * @return the port (or 0 if a dynamic port should be used)
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public int getClusterPort()
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       return clusterport;
   }

   /**
    * Returns the host that the cluster service should bind to .
    * @return the hostname (or null if the service should bind to all)
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */
   public String getClusterHost()
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       return clusterhost;
   }

   /**
    * Is the cluster "highly available" ?
    *
    * @return true if the cluster is HA
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    * @see Globals#getHAEnabled()
    */
   public boolean isHA() {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       return false;
   }


   /**
    * Reload cluster properties from config 
    *
    */
   public void reloadConfig() throws BrokerException {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       String[] props = { CLUSTERURL_PROPERTY, 
                          AUTOCONNECT_PROPERTY }; //XXX
       config.reloadProps(Globals.getConfigName(), props, false); 
   }

   /**
    * Initializes the cluster (loading all configuration). This
    * methods is called the first time setMQAddress is called
    * after the broker is created.
    *
    * @param address the address of the local broker
    * @throws BrokerException if the cluster can not be initialized
    * @see ClusterManagerImpl#setMQAddress
    */
   public String initialize(MQAddress address)  
        throws BrokerException
   {
        initialized = true;

        allBrokers = getAllBrokers();

        // set up listeners
        config.addListener(TRANSPORT_PROPERTY, this);
        config.addListener(HOST_PROPERTY, this);
        config.addListener(PORT_PROPERTY, this);
        config.addListener(AUTOCONNECT_PROPERTY, this);
        config.addListener(CONFIG_SERVER, this);

        config.addListener(DEBUG_ALL_PROP, this);
        config.addListener(DEBUG_LOCK_PROP, this);
        config.addListener(DEBUG_TXN_PROP, this);
        config.addListener(DEBUG_TAKEOVER_PROP, this);
        config.addListener(DEBUG_MSG_PROP, this);
        config.addListener(DEBUG_CONN_PROP, this);
        config.addListener(DEBUG_PING_PROP, this);
        config.addListener(DEBUG_PKT_PROP, this);

        // handle parsing transport
        transport = config.getProperty(TRANSPORT_PROPERTY);
        if (transport == null) {
            transport = "tcp";
        }
        clusterhost = config.getProperty(HOST_PROPERTY);
        //if not set, try imq.hostname
        if (clusterhost == null ) {
            clusterhost = Globals.getHostname();
            if (clusterhost != null && clusterhost.equals(Globals.HOSTNAME_ALL)) {
                clusterhost = null; 
            }
        }
        clusterport = config.getIntProperty(PORT_PROPERTY, 0);

        Set s = null;
        try {
            s = parseBrokerList();
        } catch (Exception ex) {
            logger.logStack(Logger.ERROR, Globals.getBrokerResources().getKString(
                BrokerResources.X_BAD_ADDRESS_BROKER_LIST, ex.toString()), ex);
            throw new BrokerException(ex.getMessage(), ex);
        }
        
        localBroker =  addBroker(address, true, s.remove(address), new UID() );
        getLocalBroker().setStatus(BrokerStatus.ACTIVATE_BROKER, null);

        // handle broker list



        Iterator itr = s.iterator();
        while (itr.hasNext()) {
            MQAddress addr = (MQAddress)itr.next();
            try {
                // ok, are we the local broker ?
                ClusteredBroker lcb = getLocalBroker();

                if (addr.equals(getMQAddress())) {
                    if (lcb instanceof ClusteredBrokerImpl)
                         ((ClusteredBrokerImpl)lcb)
                           .setConfigBroker(true);
                } else {
                    String name = addBroker(addr, false, true, null);
                }
            } catch (NoSuchElementException ex) {
                logger.log(Logger.INFO,
                     BrokerResources.E_INTERNAL_BROKER_ERROR,
                     "bad address in the broker list ", ex);
            }
        }

        // handle master broker
        String mbroker = config.getProperty(CONFIG_SERVER);
        if (!allowMasterBroker()) {
            if (DEBUG || logger.getLevel() <= Logger.DEBUG) {
            logger.log(Logger.INFO, "This broker does not allow "+CONFIG_SERVER+
                       " to be configured."+ (mbroker == null? "":" Ignore "+ 
                       CONFIG_SERVER+"="+mbroker)); 
            }
            mbroker = null;
        } else if (Globals.useSharedConfigRecord()) {
            if (mbroker == null) {
                logger.log(logger.INFO, br.getKString(br.I_USE_SHARECC_STORE));
            } else {
                logger.log(logger.WARNING, br.getKString(
                br.I_USE_SHARECC_STORE_IGNORE_MB, CONFIG_SERVER+"="+mbroker));
            }
            mbroker = null;
        }

        if (mbroker != null) {
            // ok, see if we exist
            MQAddress addr = null;
            try {
                addr = BrokerMQAddress.createAddress(mbroker);
            } catch (Exception ex) {
                logger.log(Logger.ERROR,
                     BrokerResources.E_INTERNAL_BROKER_ERROR,
                        "bad address while parsing "
                        + "the broker list ", ex);
            }

            masterBroker = lookupBrokerID(addr);
            if (masterBroker == null) { // wasnt in list, add it
                logger.log(Logger.WARNING,
                      BrokerResources.W_MB_UNSET,
                      addr.toString());
                masterBroker = addBroker(addr, false, true, null);
            }
            masterBroker = lookupBrokerID(addr);
        }

        if (DEBUG) {
            logger.log(Logger.DEBUG,"Cluster is:" + toString());
        }

        return localBroker;

   }

   /**
    * Method which determines the list of brokers in the cluster.
    * For non-ha clusters, this is determined by the configuration
    * properties for this broker.
    * @return a Set containing all MQAddress objects associated with
    *         the broker cluster (except the local broker)
    * @throws MalformedURLException if something is wrong with the
    *         properties and an MQAddress can not be created.
    */
   protected Set parseBrokerList()
       throws MalformedURLException, UnknownHostException
   {
       // OK, handles the broker list removing

        /*
         * "imq.cluster.brokerlist" is usually kept in the
         * cluster configuration file. Administrators can use this
         * list to setup a 'permanent' set of brokers that will
         * join this cluster.
         */
        String propfileSetting = config.getProperty(AUTOCONNECT_PROPERTY);
        String cmdlineSetting = config.getProperty(MANUAL_AUTOCONNECT_PROPERTY);

        String values = null;
        if (propfileSetting == null && cmdlineSetting == null) {
            return new HashSet();
        }
        if (propfileSetting == null) {
            values = cmdlineSetting;
        } else if (cmdlineSetting == null) {
            values = propfileSetting;
        } else {
            values = cmdlineSetting + "," + propfileSetting;
        }
        return parseBrokerList(values);
    }

    public static Set parseBrokerList(String values) 
        throws MalformedURLException, UnknownHostException {
        // we want to pull out dups .. so we use a hashmap
        // with host:port as a key

        HashMap tmpMap = new HashMap();

        // OK, parse properties
        StringTokenizer st = new StringTokenizer(values, ",");
        // Parse the given broker address list.
        while (st.hasMoreTokens()) {
            String s = st.nextToken();
            MQAddress address = BrokerMQAddress.createAddress(s);
            tmpMap.put(address.toString(),
                    address);
        }
        // OK, we can now return the list of MQAddresses

        return new HashSet(tmpMap.values());
   }


   /**
    * Returns a user-readable string representing this class.
    * @return the user-readable represeation.
    */
   public String toString() {
       String str = "ClusterManager: [local=" + localBroker
                   + ", master = " + masterBroker + "]\n";
       synchronized(allBrokers) {
           Iterator itr = allBrokers.values().iterator();
           while (itr.hasNext()) str += "\t"+itr.next() + "\n";
       }
       return str;
   }

    /**
     * Gets the UID associated with the local broker
     *
     * @return null (this cluster type does not support session)
     */
    public synchronized UID getStoreSessionUID()
    {
        return null;
    }

    /**
     * Gets the UID associated with the local broker
     *
     * @return null (this cluster type does not support session)
     */
    public synchronized UID getBrokerSessionUID()
    {
        return getLocalBroker().getBrokerSessionUID();
    }


   /**
    * Adds an old UID to the list of supported sessions
    * for this broker.
    *
    * @param uid the broker's store session UID that has been taken over
    */
   protected void addSupportedStoreSessionUID(UID uid) {
       oldSessionMap.put(uid, uid);
   }

   /**
    * Returns a list of supported session UID's for this
    * broker (not including its own sessionUID).<p>
    * This list may not include all sessionUID's that have
    * been supported by this running broker (ids may age
    * out over time).
    * 
    *
    * @return the set of sessionUIDs
    */
   public Set getSupportedStoreSessionUIDs() {
       Set s =  new HashSet(oldSessionMap.values());
       if (getStoreSessionUID() != null)
           s.add(getStoreSessionUID());
       return s;
   }



    /**
     * Handles reparsing the broker list if it changes.
     *
     * @throws BrokerException if something goes wrong during
     *  parsing
     */
    protected void brokerListChanged()
        throws BrokerException
    {
        // OK .. get the new broker list 
        Set s = null;
        try {
            s = parseBrokerList();
            if (DEBUG) {
            logger.log(Logger.INFO, "ClusterManagerImpl.parseBrokerList:"+s);
            }
        } catch (Exception ex) {
            logger.log(Logger.ERROR,
                  BrokerResources.E_INTERNAL_BROKER_ERROR,
                  "bad address in brokerListChanged ",
                       ex);
            s = new HashSet();
        }

        Iterator itr = s.iterator();
        while (itr.hasNext()) {
            MQAddress addr = (MQAddress)itr.next();
            if (lookupBrokerID(addr) == null) {
                String name = addBroker(addr,false, true, null);
            }
        }

        // OK, we need to clean up the allBroker's list

        List oldBrokers = new ArrayList();
        synchronized (allBrokers) {
            itr = allBrokers.values().iterator();
            while (itr.hasNext()) {
                ClusteredBroker cb = (ClusteredBroker)itr.next();
                ((ClusteredBrokerImpl)cb)
                           .setConfigBroker(true);
                MQAddress addr = cb.getBrokerURL();
                if (s.contains(addr)) {
                    s.remove(addr);
                    continue; 
                } else if (!cb.isLocalBroker()) {
                    oldBrokers.add(cb);
                    itr.remove();
                }
           }
        }
        // send out remove notifications
        itr = oldBrokers.iterator();
        while (itr.hasNext()) {
            ClusteredBroker cb = (ClusteredBroker)itr.next();
            brokerChanged(ClusterReason.REMOVED, cb.getBrokerName(),
                        cb, null, cb.getBrokerSessionUID(),  null);
            itr.remove();
        }
        
        // now add any remaining brokers
        itr = s.iterator();
        while (itr.hasNext()) {
               addBroker((MQAddress)itr.next(), false, true, null);
        }
    }
   
    /**
     * Handles changing the name of the master broker.
     *
     * @param mbroker the brokerid associated with the
     *                master broker
     * @throws BrokerException if something goes wrong
     */
    protected void masterBrokerChanged(String mbroker)
        throws BrokerException
    {
        // handle master broker
        
        ClusteredBroker oldMaster = getMasterBroker();
        masterBroker = null;
        if (mbroker != null) {
            // ok, see if we exist
            MQAddress addr = null;
            try {
                addr = BrokerMQAddress.createAddress(mbroker);

            } catch (Exception ex) {
                logger.log(Logger.ERROR,
                    BrokerResources.W_BAD_MB,
                    mbroker, ex);
            }

            masterBroker = lookupBrokerID(addr);
            if (addr == null) { // wasnt in list, add it
                masterBroker = addBroker(addr, false, true, null);
            }
        }
        ClusteredBroker newMaster = getMasterBroker();
        brokerChanged(ClusterReason.MASTER_BROKER_CHANGED,
                       null, oldMaster, newMaster, null,  null);

    }

    /**
     * Method called when the MQAddress is changed on the system.
     * @param address the new address of the local brokers portmapper
     */
    protected void mqAddressChanged(MQAddress address) throws Exception
    {
        ClusteredBroker cb = getLocalBroker();
        MQAddress oldAddress = cb.getBrokerURL();
        cb.setBrokerURL(address);
        brokerChanged(ClusterReason.ADDRESS_CHANGED, 
                cb.getBrokerName(), oldAddress, address,null,  null);
    }

    /**
     * Validates an updated property.
     * @see ConfigListener
     * @param name the name of the property to be changed
     * @param value the new value of the property
     * @throws PropertyUpdateException if the value is
     *          invalid (e.g. format is wrong, property
     *          can not be changed)
     */
    public void validate(String name, String value)
        throws PropertyUpdateException
    {
        if (name.equals(TRANSPORT_PROPERTY)) {
            // XXX - is there a valid value
            throw new PropertyUpdateException(
                br.getString(br.X_BAD_PROPERTY, name));
        } else if (name.equals(HOST_PROPERTY)) {
            // nothing to validate
            throw new PropertyUpdateException(
                br.getString(br.X_BAD_PROPERTY, name));
        } else if (name.equals(PORT_PROPERTY)) {
            // validate its an int
            try {
                Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                throw new PropertyUpdateException(
                     PropertyUpdateException.InvalidSetting,
                     PORT_PROPERTY + " should be set to an int"
                     + " not " + value );
            }
        } else if (name.equals(AUTOCONNECT_PROPERTY)) {
            // XXX - is there a valid value
        } else if (name.equals(CONFIG_SERVER)) {
            try {
                BrokerMQAddress.createAddress(value);
            } catch (Exception e) {
                throw new PropertyUpdateException(
                br.getString(br.X_BAD_PROPERTY, value)+": "+e.getMessage());
            }
        }
    }

    /**
     * Updates a new configuration property.
     * @see ConfigListener
     * @param name the name of the property to be changed
     * @param value the new value of the property
     * @return true if the property took affect immediately,
     *         false if the broker needs to be restarted.
     */
    public boolean update(String name, String value)
    {
        if (name.equals(TRANSPORT_PROPERTY)) {
            transport = value;
            if (transport == null || transport.length() == 0) {
                transport = "tcp";
            }
            clusterPropertyChanged(name, value);
        } else if (name.equals(HOST_PROPERTY)) {
            clusterPropertyChanged(name, value);
        } else if (name.equals(PORT_PROPERTY)) {
            clusterPropertyChanged(name, value);
        } else if (name.equals(AUTOCONNECT_PROPERTY)) {
            if (DEBUG) {
                logger.log(logger.INFO, "ClusterManagerImpl.update("+name+"="+value+")");
            }
            try {
                brokerListChanged();
            } catch (Exception ex) {
                 logger.log(Logger.INFO,"INTERNAL ERROR", ex);
            }
        } else if (name.equals(CONFIG_SERVER)) {
            try {
                masterBrokerChanged(value);
            } catch (Exception ex) {
                 logger.log(Logger.INFO,"INTERNAL ERROR", ex);
            }
        } else if (name.equals(DEBUG_ALL_PROP)) {
            DEBUG_CLUSTER_ALL = Boolean.valueOf(value);
            DEBUG_CLUSTER_LOCK = Boolean.valueOf(value);
            DEBUG_CLUSTER_TXN = Boolean.valueOf(value);
            DEBUG_CLUSTER_TAKEOVER = Boolean.valueOf(value);
            DEBUG_CLUSTER_MSG = Boolean.valueOf(value);
            DEBUG_CLUSTER_CONN = Boolean.valueOf(value);
            DEBUG_CLUSTER_PING = Boolean.valueOf(value);
            DEBUG_CLUSTER_PACKET = Boolean.valueOf(value);
        } else if (name.equals(DEBUG_LOCK_PROP)) {
            DEBUG_CLUSTER_LOCK = Boolean.valueOf(value);
        } else if (name.equals(DEBUG_TXN_PROP)) {
            DEBUG_CLUSTER_TXN = Boolean.valueOf(value);
        } else if (name.equals(DEBUG_TAKEOVER_PROP)) {
            DEBUG_CLUSTER_TAKEOVER = Boolean.valueOf(value);
        } else if (name.equals(DEBUG_MSG_PROP)) {
            DEBUG_CLUSTER_MSG = Boolean.valueOf(value);
        } else if (name.equals(DEBUG_CONN_PROP)) {
            DEBUG_CLUSTER_CONN = Boolean.valueOf(value);
        } else if (name.equals(DEBUG_PING_PROP)) {
            DEBUG_CLUSTER_PING = Boolean.valueOf(value);
        } else if (name.equals(DEBUG_PKT_PROP)) {
            DEBUG_CLUSTER_PACKET = Boolean.valueOf(value);
        }

        return true;
    }



   /**
    * Called to notify ClusterListeners when the cluster service
    * configuration. Configuration changes include:
    * <UL><LI>cluster service port</LI>
    *     <LI>cluster service hostname</LI>
    *     <LI>cluster service transport</LI>
    * </UL>
    * @param name the name of the changed property
    * @param value the new value of the changed property
    * @see ClusterListener
    */
   public void clusterPropertyChanged(String name, String value)
   {
       synchronized (listeners) {
           if ( listeners.size() == 0 )
               return;
           Iterator itr = listeners.iterator(); 
           while (itr.hasNext()) {
               ClusterListener listen = (ClusterListener)
                        itr.next();
               listen.clusterPropertyChanged(name, value);
           }
       }
   }

   /**
    * Called to notify ClusterListeners when state of the cluster
    * is changed.
    * Reasons for changes include:
    *  <UL><LI>A broker has been added to the cluster</LI>
    *      <LI>A broker has been removed from the cluster</LI>
    *      <LI>the master broker has changed</LI>
    *      <LI>the portmapper address has changed</LI>
    *      <LI>the protocol version of a broker has changed
    *          (this should only happen when a broker reconnects to
    *          the cluster)</LI>
    *      <LI>the dynamic status of the broker has changed</LI>
    *      <LI>the state of the broker has changed</LI>
    * </UL>
    * <P>
    * The data passed to the listener is determined by the
    * reason this method is being called:
    * <TABLE border=1>
    *   <TR><TH>Reason</TH><TH>brokerid</TH><TH>oldvalue</TH><TH>newvalue</TH></TR>
    *   <TR><TD>ADDED</TD><TD>added broker</TD><TD>null</TD><TD>ClusteredBroker added</TD></TR>
    *   <TR><TD>REMOVED</TD><TD>removed broker</TD><TD>ClusteredBroker removed</TD><TD>null</TD></TR>
    *   <TR><TD>STATUS_CHANGED</TD><TD>changed broker</TD><TD>Integer (old status)</TD>
    *                               <TD>Integer (new status)</TD></TR>
    *   <TR><TD>STATE_CHANGED</TD><TD>changed broker</TD><TD>BrokerState (old state)</TD>
    *                               <TD>BrokerState (new state)</TD></TR>
    *   <TR><TD>VERSION_CHANGED</TD><TD>changed broker</TD><TD>Integer (old version)</TD>
    *                               <TD>Integer (new version)</TD></TR>
    *   <TR><TD>ADDRESS_CHANGED</TD><TD>changed broker</TD><TD>MQAddress (old address)</TD>
    *                               <TD>MQAddress (new address)</TD></TR>
    *   <TR><TD>MASTER_BROKER_CHANGED</TD><TD>null</TD><TD>old master (ClusteredBroker)</TD>
    *                    <TD>new master (ClusteredBroker)</TD>
    *                    <TD>the master broker  has changed</TD></TR>
    * </TABLE>
    *
    * @param reason why this listener is being called
    * @param brokerid broker affected (if applicable)
    * @param oldvalue old value if applicable
    * @param newvalue new value if applicable
    * @param optional user data (if applicable)
    * @see ClusterListener
    */

   protected void brokerChanged(ClusterReason reason, 
                 String brokerid, Object oldvalue,
                 Object newvalue, UID suid,
                 Object userData)
   {

       synchronized (listeners) {
           if (listeners.size() == 0)
                return;
       }

       BrokerChangedEntry bce = new BrokerChangedEntry(reason,
               brokerid, oldvalue, newvalue, suid,  userData); 

       // OK, if we are processing, queue up next entry
       //
       synchronized(brokerChangedEntryList) {
           brokerChangedEntryList.add(bce);
           if (brokerChangedProcessing == true) 
               return; // let other guy handle it
           brokerChangedProcessing = true;
       }

       try {
           BrokerChangedEntry process = null;

           while (true) {

               ClusterListener[] alisteners = null;
               synchronized (listeners) {
                   synchronized (brokerChangedEntryList) {    
                       if (listeners.size() == 0 || brokerChangedEntryList.isEmpty()) {
                           // nothing to do
                           brokerChangedProcessing = false;
                           break;
                       }    
                       process = (BrokerChangedEntry)brokerChangedEntryList.removeFirst();
                   } 
                   alisteners = new ClusterListener[listeners.size()];
                   alisteners = (ClusterListener[])listeners.toArray(new ClusterListener[0]);
               }

               for (int i = 0; i < alisteners.length; i++) {

                       ClusterListener listen = (ClusterListener)alisteners[i];
                       synchronized(listeners) {
                           if (!listeners.contains(listen)) continue;
                       }

                       if (process.reason == ClusterReason.ADDED) {
                           listen.brokerAdded((ClusteredBroker)process.newValue,
                                  process.brokerSession );

                       } else if (process.reason == ClusterReason.REMOVED) {
                           listen.brokerRemoved((ClusteredBroker)process.oldValue,
                                  process.brokerSession);

                       } else if (process.reason == ClusterReason.STATUS_CHANGED) {
                           listen.brokerStatusChanged(process.brokerid,
                                  ((Integer)process.oldValue).intValue(),
                                  ((Integer)process.newValue).intValue(),
                                  process.brokerSession,
                                  process.userData);

                       } else if (process.reason == ClusterReason.STATE_CHANGED) {
                           listen.brokerStateChanged(process.brokerid,
                                  (BrokerState)process.oldValue,
                                  (BrokerState)process.newValue);
                       } else if (process.reason == ClusterReason.VERSION_CHANGED) {
                           listen.brokerVersionChanged(process.brokerid,
                                  ((Integer)process.oldValue).intValue(),
                                  ((Integer)process.newValue).intValue());
                       } else if (process.reason == ClusterReason.ADDRESS_CHANGED) {
                           listen.brokerURLChanged(process.brokerid,
                                  (MQAddress)process.oldValue,
                                  (MQAddress)process.newValue);

                       } else if (process.reason == ClusterReason.MASTER_BROKER_CHANGED) {
                           listen.masterBrokerChanged((ClusteredBroker)process.oldValue,
                                         (ClusteredBroker)process.newValue);
                       }
               }

           }
        } finally {
            synchronized (brokerChangedEntryList) {    
                brokerChangedProcessing = false;
            }
        }
       
   }

   /**
    * flag used to determine if we are in the middle of
    * listener processing when a call occurs (to make sure
    * notifications are processed in order).
     */
   private boolean brokerChangedProcessing = false;

   /**
    * list used to make sure listeners are processed in order
    */
   LinkedList brokerChangedEntryList = new LinkedList();

   /**
    * container class used when listeners are processed in order.
    */
   private class BrokerChangedEntry
   {
       ClusterReason reason = null;
       String brokerid = null;
       Object oldValue = null;
       Object newValue = null;
       Object userData = null;
       UID brokerSession = null;

       public BrokerChangedEntry(ClusterReason reason, String brokerid,
                Object oldValue, Object newValue, UID bs,
                Object userData) {
           this.reason = reason;
           this.brokerid = brokerid;
           this.oldValue = oldValue;
           this.newValue = newValue;
           this.userData = userData;
           this.brokerSession = bs;
       }
   }


   // NOTE: for clustered brokers, the id is really
   // the same as the host:port

   int brokerindx = 0;

   /**
    * Non-HA implementation of ClusteredBroker.
    */
   class ClusteredBrokerImpl implements ClusteredBroker
   {

        /**
         * Name associated with this broker. For non-ha clusters
         * it is of the form broker# and is not the same across
         * all brokers in the cluster (although it is unique on
         * this broker).
         */
        String brokerName = null;

        /**
         * The portmapper for this broker.
         */
        MQAddress address = null;

        /**
         * The instance name of this broker
         */
        transient String instanceName = null;

        /**
         * Is this the local (in this vm) broker.
         */
        boolean local = false;

        /**
         * Is this broker setup by confguration (vs dynamic).
         */
        boolean configed = false;

        /**
         * Current status of the broker.
         */
        Integer status = new Integer(BrokerStatus.BROKER_UNKNOWN);

        /**
         * Current state of the broker.
         */
        BrokerState state = BrokerState.INITIALIZING;

        /** 
         * Protocol version of this broker.
         */
        Integer version = new Integer(0);


        /**
         * Broker SessionUID for this broker.
         * This uid changes on each restart of the broker.
         */
         UID brokerSessionUID = null;

         /**
          * has brokerID been generated
          */
         boolean isgen = false;
      
        /** 
         * Create a instace of ClusteredBroker.
         *
         * @param url the portampper address of this broker
         * @param local is this broker local
         */
        public ClusteredBrokerImpl(MQAddress url, boolean local, UID id)
        {
            this.local = local;
            this.address = url;
            brokerSessionUID = id;
            synchronized (this) {
                if (local) {
                    this.brokerName = Globals.getBrokerID();
                    this.instanceName = Globals.getConfigName();
                }
                if (this.brokerName == null) {
                    isgen = true;
                    brokerindx ++;
                    this.brokerName = "broker" + brokerindx;
                }
            }
        }

        private ClusteredBrokerImpl() {
        }

        public boolean equals(Object o) {
            if (! (o instanceof ClusteredBroker)) 
                return false;
            return this.getBrokerName().equals(((ClusteredBroker)o).getBrokerName());
        }

        public int hashCode() {
             return this.getBrokerName().hashCode();
        }


        /** 
         * String representation of this broker.
         */
        public String toString() {
            if (!local)
                return brokerName + "(" + address + ")";
             return brokerName + "* (" + address + ")";
                   
        }

        /**
         * a unique identifier assigned to the broker
         * (randomly assigned).<P>
         *
         * This name is only unique to this broker. The
         * broker at this URL may be assigned a different name
         * on another broker in the cluster.
         *
         * @return the name of the broker
         */
        public String getBrokerName()
        {
             return brokerName;
        }
    
        /**
         * the URL to the portmapper of this broker.
         * @return the URL of this broker
         */
        public MQAddress getBrokerURL()
        {
             return address;
        }

        /**
         * @return the instance name of this broker, null if not available
         */
        public String getInstanceName() {
            return instanceName;
        }

        /**
         * @param Set the instance name of this broker, can be null
         */
        public void setInstanceName(String instName) {
             instanceName = instName;
        }
 
        /**
         * sets the URL to the portmapper of this broker.
         * @param address the URL of this broker
         * @throws UnsupportedOperationException if this change
         *         can not be made on this broker
         */
        public void setBrokerURL(MQAddress address) throws Exception
        {
             MQAddress oldaddress = this.address;
             this.address = address;
             brokerChanged(ClusterReason.ADDRESS_CHANGED, 
                  this.getBrokerName(), oldaddress, this.address, null, null);
        }

    
        /**
         * Is this the address of the broker running in this
         * VM.
         * @return true if this is the broker running in the
         *         current vm
         */
        public boolean isLocalBroker()
        {
            return local;
        }
    
        /**
         * gets the status of the broker.
         *
         * @see BrokerStatus
         * @return the status of the broker
         */
        public synchronized int getStatus() {
            return status.intValue();
        } 
    
        /**
         * gets the protocol version of the broker .
         * @return the current cluster protocol version (if known)
         *        or 0 if not known
         */
        public synchronized int getVersion()
        {
            return (version == null ? 0 : version.intValue());
        }  
    
        /**
         * sets the protocol version of the broker .
         * @param version the current cluster protocol version (if known)
         *        or 0 if not known
         * @throws UnsupportedOperationException if the change is not allowed
         */
        public synchronized void setVersion(int version) throws Exception 

        {
            Integer oldversion = this.version;
            this.version = new Integer(version);
            brokerChanged(ClusterReason.VERSION_CHANGED, 
                  this.getBrokerName(), oldversion, this.version, null, null);
        }  

    
        /**
         * sets the status of the broker (and notifies listeners).
         *
         * @param status the status to set
         * @param userData optional user data associated with the status change
         * @see ConfigListener
         */
        public void setStatus(int newstatus, Object userData)
        {
            Integer oldstatus = null;
            UID uid = null;

            // ok - for standalone case, adjust so that LINK_DOWN=DOWN
            if (BrokerStatus.getBrokerIsDown(newstatus))
                newstatus = BrokerStatus.setBrokerLinkIsDown(newstatus);
            else if (BrokerStatus.getBrokerLinkIsDown(newstatus))
                newstatus = BrokerStatus.setBrokerIsDown(newstatus);
            else if (BrokerStatus.getBrokerLinkIsUp(newstatus))
                newstatus = BrokerStatus.setBrokerIsUp(newstatus);
            else if (BrokerStatus.getBrokerIsUp(newstatus))
                newstatus = BrokerStatus.setBrokerLinkIsUp(newstatus);

            synchronized (this) {
                if (this.status.intValue() == newstatus)
                    return;
                oldstatus = this.status;
                this.status = new Integer(newstatus);
                uid = getBrokerSessionUID();
            }
            // notify
            brokerChanged(ClusterReason.STATUS_CHANGED, 
                  this.getBrokerName(), oldstatus, this.status, 
                  uid, userData);

            // ok for non-HA we also can not expect notification that the state
            // has changed - deal w/ it here
            try {
                if (BrokerStatus.getBrokerIsUp(newstatus))
                    setState(BrokerState.OPERATING);
                if (BrokerStatus.getBrokerIsDown(newstatus))
                    setState(BrokerState.SHUTDOWN_COMPLETE);
            } catch (Exception ex) {
                logger.logStack(Logger.DEBUG,"Error setting state ", ex);
            }

        }

        /**
         * Updates the BROKER_UP bit flag on status.
         * 
         * @param userData optional user data associated with the status change
         * @param up setting for the bit flag (true/false)
         */
        public void setBrokerIsUp(boolean up, UID brokerSession, Object userData)
        {
        
            UID uid = brokerSession;
            Integer oldstatus = null;
            Integer newstatus = null;
            synchronized (this) {
                if (!up && !uid.equals(getBrokerSessionUID())) {
                    logger.log(logger.INFO, br.getKString(
                        BrokerResources.I_DOWN_STATUS_ON_BROKER_SESSION,
                        "[BrokerSession:"+uid+"]", this.toString()));
                    oldstatus = new Integer(BrokerStatus.BROKER_INDOUBT);
                    newstatus = BrokerStatus.setBrokerIsDown(oldstatus);

                } else {

                    oldstatus = this.status;
                    int newStatus = 0;
                    if (up) {
                        newStatus = BrokerStatus.setBrokerIsUp
                                        (this.status.intValue());
                    } else {
                        newStatus = BrokerStatus.setBrokerIsDown
                                        (this.status.intValue());
                    }
                    this.status = new Integer(newStatus);
                    uid = getBrokerSessionUID();
                    newstatus = this.status;
                }
            }
            // notify
            brokerChanged(ClusterReason.STATUS_CHANGED, 
                  this.getBrokerName(), oldstatus, newstatus, uid, userData);
            try {
                if (up)
                    setState(BrokerState.OPERATING);
                else
                    setState(BrokerState.SHUTDOWN_COMPLETE);
            } catch (Exception ex) {
                logger.logStack(Logger.DEBUG,"Error setting state ", ex);
            }

        }

        /**
         * Updates the BROKER_LINK_UP bit flag on status.
         * 
         * @param userData optional user data associated with the status change
         * @param up setting for the bit flag (true/false)
         */
        public void setBrokerLinkUp(boolean up, Object userData)
        {
            // on non-HA clusters status should always be set to UP if
            // LINK_UP
        
            Integer oldstatus = null;
            UID uid = null;
            synchronized (this) {
                oldstatus = this.status;
                uid = getBrokerSessionUID();

                int newStatus = 0;
                if (up) {
                   newStatus = BrokerStatus.setBrokerLinkIsUp
                        (BrokerStatus.setBrokerIsUp(this.status.intValue()));
                } else {
                   newStatus = BrokerStatus.setBrokerLinkIsDown
                        (BrokerStatus.setBrokerIsDown(this.status.intValue()));
                }
                this.status = new Integer(newStatus);
            }
            // notify
            brokerChanged(ClusterReason.STATUS_CHANGED, 
                  this.getBrokerName(), oldstatus, this.status,
                  uid, userData);
            try {
                if (up)
                    setState(BrokerState.OPERATING);
                else
                    setState(BrokerState.SHUTDOWN_COMPLETE);
            } catch (Exception ex) {
                logger.logStack(Logger.DEBUG,"Error setting state ", ex);
            }

        }


        /**
         * Updates the BROKER_INDOUBT bit flag on status.
         * 
         * @param userData optional user data associated with the status change
         * @param up setting for the bit flag (true/false)
         */
        public void setBrokerInDoubt(boolean up, Object userData)
        {
            UID uid = (UID)userData;
            Integer oldstatus = null;
            Integer newstatus = null;
            synchronized (this) {
                if (up && !uid.equals(getBrokerSessionUID())) {
                    logger.log(logger.INFO, br.getKString(
                        BrokerResources.I_INDOUBT_STATUS_ON_BROKER_SESSION,
                        "[BrokerSession:"+uid+"]", this.toString()));
                    oldstatus = new Integer(BrokerStatus.ACTIVATE_BROKER);
                    newstatus = BrokerStatus.setBrokerInDoubt(oldstatus);
                } else {
                    oldstatus = this.status;
                    int newStatus = 0;
                    uid = getBrokerSessionUID();
                    if (up) {
                        newStatus = BrokerStatus.setBrokerInDoubt
                                        (this.status.intValue());
                    } else {
                        newStatus = BrokerStatus.setBrokerNotInDoubt
                                        (this.status.intValue());
                    }
                    this.status = new Integer(newStatus);
                    newstatus =this.status;
                }
            }
            // notify
            brokerChanged(ClusterReason.STATUS_CHANGED, 
                  this.getBrokerName(), oldstatus, newstatus, uid, userData);

        }

        /**
         * marks this broker as destroyed. This is equivalent to setting
         * the status of the broker to DOWN.
         *
         * @see BrokerStatus#DOWN
         */
        public void destroy() {
            synchronized (this) {
                status = new Integer(BrokerStatus.setBrokerIsDown(
                              status.intValue()));
            }
            synchronized (allBrokers) {
                if (!isConfigBroker()) {
                   allBrokers.remove(getBrokerName());
                }
            }
            brokerChanged(ClusterReason.REMOVED, getBrokerName(),
                  this, null, getBrokerSessionUID(), null);
        }

        /**
         * gets the state of the broker .
         *
         * @throws BrokerException if the state can not be retrieve
         * @return the current state
         */
        public BrokerState getState()
            throws BrokerException
        {
            return state;
        }

        /**
         * sets the state of the broker  (and notifies any listeners).
         * @throws IllegalAccessException if the broker does not have
         *               permission to change the broker (e.g. one broker
         *               is updating anothers state).
         * @throws IllegalStateException if the broker state changed
         *               unexpectedly.
         * @throws IllegalArgumentException if the state is not supported
         *               for this cluster type.
         * @param state the state to set for this broker
         * @see ConfigListener
         */
        public void setState(BrokerState state)
             throws IllegalAccessException, IllegalStateException,
                IllegalArgumentException
        {
            BrokerState oldState = this.state;
            this.state = state;
            brokerChanged(ClusterReason.STATE_CHANGED, 
                  this.getBrokerName(), oldState, this.state, null,  null);
        }


        /**
         * Is the broker static or dynmically configured
         */
        public boolean isConfigBroker()
        {
             return configed;
        }

        /**
         * Is the broker static or dynmically configured
         */
        protected void setConfigBroker(boolean config)
        {
             configed = config;
        }


        public synchronized UID getBrokerSessionUID() {
            return brokerSessionUID;
        }

        public synchronized void setBrokerSessionUID(UID session) {
            brokerSessionUID = session;
        }

        public boolean isBrokerIDGenerated()
        {
            return isgen;
        }


   }

    /**
     * Typesafe enum class which represents a Reason passed into broker changed
     */
    public static class ClusterReason
    {
        
        /**
         * descriptive string associated with the reason
         */
        private final String name;
        
        /**
         * private constructor for ClusterReason
         */
        private ClusterReason(String name) {
            this.name = name;
        }

        /**
         * a string representation of the object
         */
        public String toString() {
            return "ClusterReason["+ name +"]";
        }

        /**
         * A broker has been added to the cluster.
         */
        public static final ClusterReason ADDED = 
                 new ClusterReason("ADDED");

        /**
         * A broker has been removed from the cluster.
         */
        public static final ClusterReason REMOVED = 
                 new ClusterReason("REMOVED");

        /**
         * The status of a broker has changed.
         * @see BrokerStatus
         */
        public static final ClusterReason STATUS_CHANGED = 
                 new ClusterReason("STATUS_CHANGED");

        /**
         * The state of a broker has changed.
         * @see BrokerState
         */
        public static final ClusterReason STATE_CHANGED = 
                 new ClusterReason("STATE_CHANGED");

        /**
         * The protocol version of a broker has changed.
         */
        public static final ClusterReason VERSION_CHANGED = 
                 new ClusterReason("VERSION_CHANGED");

        /**
         * The portmapper address of a broker has changed.
         */
        public static final ClusterReason ADDRESS_CHANGED = 
                 new ClusterReason("ADDRESS_CHANGED");

        /**
         * The address of the master broker in the cluster
         * has changed.
         */
        public static final ClusterReason MASTER_BROKER_CHANGED = 
                 new ClusterReason("MASTER_BROKER_CHANGED");
    
    }


}

