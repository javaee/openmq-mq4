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
 * @(#)HAClusterManagerImpl.java	1.77 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.cluster.ha;

import com.sun.messaging.jmq.io.MQAddress;
import com.sun.messaging.jmq.io.Status;
import com.sun.messaging.jmq.util.log.*;
import com.sun.messaging.jmq.util.UID;
import java.util.*;
import com.sun.messaging.jmq.jmsserver.service.TakingoverTracker;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.core.BrokerMQAddress;
import com.sun.messaging.jmq.jmsserver.config.*;
import com.sun.messaging.jmq.jmsserver.persist.Store;
import com.sun.messaging.jmq.jmsserver.persist.TakeoverStoreInfo;
import com.sun.messaging.jmq.jmsserver.persist.HABrokerInfo;
import com.sun.messaging.jmq.jmsserver.cluster.*;
import com.sun.messaging.jmq.jmsserver.resources.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import java.net.MalformedURLException;

// XXX FOR TEST CLASS
import java.io.*;


/**
 * This class extends ClusterManagerImpl and is used to obtain and
 * distribute cluster information in an HA cluster.
 */

public class HAClusterManagerImpl extends ClusterManagerImpl 
      implements ClusterManager
{

// testing only property
    private static boolean IGNORE_HADB_ERRORS = true;

    /**
     * The brokerid associated with the local broker.
     * The local broker is running in the current vm.
     */

    private String localBrokerId = null;


    /**
     * The version of the cluster protocol.
     * <b>NOTE:</b> XXX - this should be retrieved from the multibroker 
     * code.
     */
    private int VERSION = 40;


    UID localSessionUID = null;


   /**
    * Creates an instance of HAClusterManagerImpl.
    * @throws BrokerException if the cluster information could not be loaded
    *      because of a configuration issue.
    */
   public HAClusterManagerImpl() 
       throws BrokerException
   {
       super();
   }


   /**
    * Returns if the cluster is "highly available".
    *
    * @return true if the cluster is HA
    * @throws RuntimeException if called before the cluster has
    *         been initialized by calling ClusterManager.setMQAddress
    * @see ClusterManager#setMQAddress
    */
   public boolean isHA() {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       return true;
   }


   /**
    * Reload the cluster properties from config 
    *
    */
   public void reloadConfig() throws BrokerException {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       String[] props = { CLUSTERURL_PROPERTY };
       config.reloadProps(Globals.getConfigName(), props, false);
   }


   /**
    * Retrieves the Map used to store all objects. In the HA broker,
    * the map automatically checks the database if a broker can not
    * be found in memory.
    * @return the map used to store the brokers.
    * @throws BrokerException if something goes wrong loading brokers
    *             from the database
    */
   protected Map getAllBrokers() 
        throws BrokerException
   {
       return new HAMap();
   }


   /**
    * Method which initializes the broker cluster. (Called by
    * ClusterManager.setMQAddress()).
    *
    * @param address the address for the portmapper
    *
    * @see ClusterManager#setMQAddress
    * @throws BrokerException if something goes wrong during intialzation 
    */
   public String initialize(MQAddress address) 
        throws BrokerException
   {
        logger.log(Logger.DEBUG, "initializingCluster at " + address);

        localBrokerId = Globals.getBrokerID();

        if (localBrokerId == null) {
            throw new BrokerException(
                Globals.getBrokerResources().getKString(
                BrokerResources.E_BAD_BROKER_ID, "null"));
        }

        // make sure master broker is not set
        String mbroker = config.getProperty(CONFIG_SERVER);

        if (mbroker != null) {
            logger.log(Logger.WARNING, 
                   Globals.getBrokerResources().getKString(
                   BrokerResources.W_HA_MASTER_BROKER_NOT_ALLOWED, 
                   CONFIG_SERVER+"="+mbroker));
        }
       // make sure store is JDBC
       Store store = Globals.getStore();
       if (!store.isJDBCStore()) {
           throw new BrokerException(
                 Globals.getBrokerResources().getKString(
                 BrokerResources.E_HA_CLUSTER_INVALID_STORE_TYPE));
       }

        super.initialize(address);

        return localBrokerId;
   }

   /**
    * @return true if allow configured master broker
    */
   protected boolean allowMasterBroker() {
       return false; 
   }


   /**
    * Method used to retrieve the list of brokers. In HA, this
    * method displays warnings if the old cluster properties
    * have been set and then loads the brokers from the database.
    *
    * @return a set of MQAddress objects which contains all known
    *         brokers except the local broker
    * @throws MalformedURLException if the address of a broker
    *         stored in the database is invalid.
    */
   protected Set parseBrokerList()
       throws MalformedURLException
   {
       // ignore properties, we get the list from the 
       // database
   
        String propfileSetting = config.getProperty(AUTOCONNECT_PROPERTY);
        String cmdlineSetting = config.getProperty(Globals.IMQ 
                                + ".cluster.brokerlist.manual");

        if (propfileSetting != null) {
             logger.log(Logger.INFO,
                 BrokerResources.I_HA_IGNORE_PROP,
                 AUTOCONNECT_PROPERTY);
        }
        if (cmdlineSetting != null) {
             logger.log(Logger.INFO,
                 BrokerResources.I_HA_IGNORE_PROP,
                 Globals.IMQ + ".cluster.brokerlist.manual");
        }
        Set brokers = new HashSet();
        synchronized(allBrokers) {
            Iterator itr = allBrokers.values().iterator();
            while (itr.hasNext()) {
                Object obj = itr.next();
                HAClusteredBroker hab = (HAClusteredBroker)obj;
                if (!hab.isLocalBroker())
                    brokers.add(hab.getBrokerURL());
            }
        }
        return brokers;
   }

   /**
    * Method used in ClusterManagerImpl (both in initialization and
    * in normal operation) to add a broker.
    * <p>
    *<b>NOTE:</b> broker created is an HAClusteredBroker.<p>
    * @param URL the MQAddress of the new broker
    * @param isLocal indicates if this is the current broker in this
    *                vm.
    * @throws NoSuchElementException if the broker listed is
    *              not available in the shared store (since this
    *              indicates a misconfiguration).
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    * @return the uid associated with the new broker
    */
   protected String addBroker(MQAddress URL, boolean isLocal, boolean isConfig,
             UID brokerUID)
       throws NoSuchElementException, BrokerException
   {
       // NOTE we are always a config broker in the HA case, ignore this argument

       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       String brokerid = null;

       ClusteredBroker cb = null;
       if (isLocal) { // get the broker id
           brokerid = localBrokerId;

           // see if the broker exists, if not create one
           cb = getBroker(brokerid);

           // NOTE: in HA, the Monitor class will have to validate the
           // URL and update if necessary

           if (cb == null) {
               cb = new HAClusteredBrokerImpl(brokerid,
                        URL,  VERSION, BrokerState.INITIALIZING,
                        brokerUID);
               ((HAClusteredBrokerImpl)cb).setIsLocal(true);
               cb.setInstanceName(Globals.getConfigName());
           } else {
               ((HAClusteredBrokerImpl)cb).setIsLocal(true);
               cb.setInstanceName(Globals.getConfigName());
           }
           synchronized(allBrokers) {
               allBrokers.put(brokerid, cb);
           }
       } else { // lookup id
           brokerid = lookupBrokerID(URL);
           if (brokerid != null) {
               cb = getBroker(brokerid);
           }
       }
       if (brokerUID != null)
           ((HAClusteredBrokerImpl)cb).setBrokerSessionUID(brokerUID);

       // OK, if we are here we need to create a new one
       if (brokerid == null ) {
           throw new NoSuchElementException(
              Globals.getBrokerResources().getKString(
              BrokerResources.E_BROKERINFO_NOT_FOUND_IN_STORE, brokerid));
       }
       
       if (isLocal) { // OK, we know the local broker is up
                      // for all others, activate must be called
           cb.setStatus(BrokerStatus.ACTIVATE_BROKER, null);
       } else {
           updateBroker(cb);
       }
       brokerChanged(ClusterReason.ADDED, cb.getBrokerName(),
                     null, cb, cb.getBrokerSessionUID(), null);

       return brokerid;

   }

    public ClusteredBroker updateBroker(ClusteredBroker broker)  
    {
         // force an update
         synchronized(allBrokers) {
             return (HAClusteredBroker)((HAMap)allBrokers).get(broker.getBrokerName(), true);
         }
    }

   /**
    * Method used in a dynamic cluster, it updates the
    * system when a broker is removed.
    *
    * @param brokerid the id associated with the broker
    * @param userData optional user data
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

       ClusteredBroker cb = getBroker(brokerid);

       if (cb == null) throw new NoSuchElementException(
             "Unknown brokerid " + brokerid);

       cb.setInstanceName(null);

           // OK, set the broker link down
       synchronized (this) {
             cb.setStatus(BrokerStatus.setBrokerLinkIsDown(
                              cb.getStatus()), userData);
       }

   }     


   /**
    * finds the brokerid associated with the given session.
    *
    * @param uid is the session uid to search for
    * @return the uid associated with the session or null we cant find it.
    */
   public String lookupStoreSessionOwner(UID uid) {

       try {
           // for HA, check the database if necessary
           return Globals.getStore().getStoreSessionOwner(uid.longValue());
       } catch (Exception ex) {
           logger.logStack(logger.INFO, BrokerResources.E_INTERNAL_ERROR, ex);
       }

       return null;
   }

   
   /**
    * Retrieve the broker that creates the specified store session ID.
    * @param uid store session ID
    * @return the broker ID
    */
   public String getStoreSessionCreator(UID uid)
   {
       try {
           return Globals.getStore().getStoreSessionCreator(uid.longValue());
       } catch (Exception ex) {
           logger.logStack(logger.INFO, BrokerResources.E_INTERNAL_ERROR, ex);
       }
       return null;
   }


   /**
    * Finds the brokerid associated with the given host/port.
    *
    * @param address the MQAddress of the new broker
    * @return the id associated with the broker or null if the broker does not exist
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    */  

   public String lookupBrokerID(MQAddress address)
   {
        // for HA, check the database if necessary
        try {
            synchronized(allBrokers) {
                ((HAMap)allBrokers).updateHAMap();
            }
        } catch (BrokerException ex) {
            logger.logStack(logger.INFO, BrokerResources.E_INTERNAL_ERROR, ex);
        }
        return super.lookupBrokerID(address);
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
        return super.getKnownBrokerCount();
   }


   /**
    * Returns an iterator of <i>HAClusteredBroker</i> objects for
    * all other brokers in the cluster. (this is a copy of
    * the current list)
    *
    * @param refresh if true refresh current list then return it
    * @return iterator of <i>HAClusteredBroker</i>
    * @throws RuntimeException if called before the cluster has
    *         been initialized by calling ClusterManager.setMQAddress
    * @see ClusterManager#setMQAddress
    */
   public Iterator getKnownBrokers(boolean refresh)
   {
       if (!initialized)
           throw new RuntimeException("Cluster not initialized");

       HashSet brokers = null;
       if (refresh) {
           try {
               synchronized(allBrokers) {
                   ((HAMap)allBrokers).updateHAMap(true);
               }
           } catch (BrokerException ex) {
               logger.logStack(logger.WARNING, BrokerResources.E_INTERNAL_ERROR, ex);
           }
       }

       synchronized (allBrokers) {
           brokers = new HashSet(allBrokers.values());
       }
       return brokers.iterator();
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
        return getKnownBrokers(true);
    }


    /**
     * Called when the master broker is set or changed
     *
     * @param mbroker the brokerid associated with the
     *                master broker
     * @throws UnsupportedOperationException if called since
     *              a master broker is not allowed with an HA
     *              cluster.
     */

    protected void masterBrokerChanged(String mbroker)
    {
        // no master broker allowed !!!
        throw new UnsupportedOperationException(
             "Can not use/set/ change masterbroker");
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
        // no master broker allowed !!!
        if (name.equals(CONFIG_SERVER)) {
            throw new PropertyUpdateException(
                  Globals.getBrokerResources().getKString(
                  BrokerResources.X_HA_MASTER_BROKER_UNSUPPORTED));
        }
        super.validate(name, value);
    }

//------------------------------------------------------------
// apis added for javadoc documentation ONLY
//------------------------------------------------------------
   /**
    * Retrieves the <i>HAClusteredBroker</i> which represents
    * this broker.
    *
    * @return the local broker
    * @throws RuntimeException if the cluster has not be initialized
    *          (which occurs the first time the MQAddress is set)
    * @see ClusterManagerImpl#setMQAddress
    * @see HAClusterManagerImpl#getBroker(String)
    */
   public ClusteredBroker getLocalBroker()
   {
       return super.getLocalBroker();
   }
   /**
    * Gets the session UID associated with the local broker
    *
    * @return the broker session uid (if known)
    */
   public UID getStoreSessionUID()
   {
       if (localSessionUID == null) {
           localSessionUID = ((HAClusteredBroker)getLocalBroker()).getStoreSessionUID();
       }
       return localSessionUID;
   }

   /**
    * Returns a specific <i>HAClusteredBroker</i> object by name.
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
        ClusteredBroker cb = super.getBroker(brokerid);
        if (cb != null) 
            return cb;

        // for HA, check the database if necessary
        try {
            synchronized(allBrokers) {
                ((HAMap)allBrokers).updateHAMap(true);
            }
        } catch (BrokerException ex) {
            logger.logStack(logger.WARNING, BrokerResources.E_INTERNAL_ERROR, ex);
        }
        return super.getBroker(brokerid);
    }


   /**
    * Implementation of HAClusteredBroker (and ClusteredBroker)
    * used by this implementation of ClusterManager
    */

   public class HAClusteredBrokerImpl implements HAClusteredBroker
   {
        /**
         * Is this the local (in this vm) broker.
         */
        boolean local = false;

        /**
         * Current status of the broker.
         */
        Integer status = new Integer(BrokerStatus.BROKER_UNKNOWN);

        /**
         * Current state of the broker.
         */
        BrokerState state = BrokerState.INITIALIZING;

        /**
         * Name associated with this broker. This name is
         * unique across the cluster.
         */
        String brokerid = null;

        /**
         * The portmapper for this broker.
         */
        MQAddress address = null; 

        /**
         * The instance name for this broker. can be null
         */
        transient String instanceName = null;


        /** 
         * Protocol version of this broker.
         */
        Integer version = new Integer(0);

        /**
         * The unique ID associated with this instance of a store
         * (used when takeover occurs).
         */
        UID session = null;

        /**
         * The last time (in milliseconds) the heartbeat on the database
         * was updated.
         */
        long heartbeat = 0;

        /**
         * The brokerid associated with the broker (if any) who took over
         * this brokers store.
         */
        String takeoverBroker = null;


        /**
         * The uid associated with broker session
         */
        UID brokerSessionUID;
       
        /** 
         * Create a instace of HAClusteredBrokerImpl (which is already available
         * in the database).
         *
         * @param brokerid is the id associated with this broker
         * @throws BrokerException if something is wrong during loading
         */
        public HAClusteredBrokerImpl(String brokerid)
            throws BrokerException
        {
             this(brokerid, Globals.getStore().getBrokerInfo(brokerid));

        }

        /** 
         * Create a instace of HAClusteredBrokerImpl from a map of 
         * data (called when store.getAllBrokerInfoByState() is used
         * to retrieve a set of brokers).
         *
         * @param brokerid is the id associated with this broker
         * @param m is the fields associated with the broker's entry in the
         *               jdbc store.
         * @throws BrokerException if something is wrong during loading
         */
        public HAClusteredBrokerImpl(String brokerid, HABrokerInfo m)
            throws BrokerException
        {
             this.brokerid = brokerid;
             this.status = new Integer(BrokerStatus.BROKER_UNKNOWN);
             String urlstr = m.getUrl();
             try {
                 address = BrokerMQAddress.createAddress(urlstr);
             } catch (Exception ex) {
                 throw new BrokerException(
                       Globals.getBrokerResources().getKString(
                           BrokerResources.E_INTERNAL_BROKER_ERROR,
                           "invalid URL stored on disk " + urlstr, ex));
             }
             version = new Integer(m.getVersion());
             state = BrokerState.getState(m.getState());
             session = new UID(m.getSessionID());
             takeoverBroker = m.getTakeoverBrokerID();
             heartbeat = m.getHeartbeat();
        }

        /** 
         * Create a <i>new</i> instace of HAClusteredBrokerImpl  and
         * stores it into the database.
         *
         * @param brokerid is the id associated with this broker
         * @param url is the portmapper address
         * @param version is the cluster version of the broker
         * @param state is the current state of the broker
         * @param session is this broker's current store session.
         * @throws BrokerException if something is wrong during creation
         */
        public HAClusteredBrokerImpl(String brokerid,
                 MQAddress url, int version, BrokerState state,
                 UID session)
            throws BrokerException
        {
             this.brokerid = brokerid;
             this.local = local;
             this.status = new Integer(BrokerStatus.BROKER_UNKNOWN);
             this.address = url;
             this.version = new Integer(version);
             this.state = state;
             this.session = session;
             this.takeoverBroker = "";
             this.brokerSessionUID = new UID();

             Store store = Globals.getStore();
             store.addBrokerInfo(brokerid, address.toString(), state,
                    this.version.intValue(), session.longValue(),
                    heartbeat);

             this.heartbeat = store.getBrokerHeartbeat(brokerid);
        }

        public boolean equals(Object o) {
            if (! (o instanceof ClusteredBroker)) 
                return false;
            return this.getBrokerName().equals(((ClusteredBroker)o).getBrokerName());
        }

        public int hashCode() {
             return this.getBrokerName().hashCode();
        }

        public void update(HABrokerInfo m) {
             MQAddress oldaddr = address;

             synchronized (this) {

             this.brokerid = m.getId();
             String urlstr = m.getUrl();
             try {
                 address = BrokerMQAddress.createAddress(urlstr);
             } catch (Exception ex) {
                 logger.logStack(logger.WARNING, ex.getMessage(), ex);
                 address = oldaddr;
             }
             version = new Integer(m.getVersion());
             state = BrokerState.getState(m.getState());
             session = new UID(m.getSessionID());
             takeoverBroker = m.getTakeoverBrokerID();
             heartbeat = m.getHeartbeat();

             }

             if (!oldaddr.equals(address)) {
                 brokerChanged(ClusterReason.ADDRESS_CHANGED,
                     this.getBrokerName(), oldaddr, this.address, null, null);
             }
         }

        /**
         * Sets if this broker is local or not/
         * @param local true if the broker is running in the current vm
         * @see #isLocalBroker
         */

        void setIsLocal(boolean local)
        {
            this.local = local;
        }

        /** 
         * String representation of this broker.
         */
        public String toString() {
            if (!local)
                return "-" +brokerid + "@" + address + ":" + state +
                        "[StoreSession:" + session + ", BrokerSession:"+brokerSessionUID+"]"+  ":"+
                        BrokerStatus.toString( status.intValue());
             return "*" +brokerid + "@" + address + ":" + state + 
                        "[StoreSession:" + session + ", BrokerSession:"+brokerSessionUID+"]"+  ":"+
                        BrokerStatus.toString( status.intValue());
                   
        }

        /**
         * a unique identifier assigned to the broker
         * (randomly assigned)<P>
         *
         * This name is only unique to this broker. The
         * broker at this URL may be assigned a different name
         * on another broker in the cluster.
         *
         * @return the name of the broker
         */
        public String getBrokerName()
        {
             return brokerid;
        }
    
        /**
         * the URL to the portmapper of this broker
         * @return the URL of this broker
         */
        public MQAddress getBrokerURL()
        {             
             return address;
        }
 
        /**
         * @return The instance name of this broker, can be null
         */
        public String getInstanceName()
        {
             return instanceName;
        }

        /**
         * @param instName The instance name of this broker, can be null
         */
        public void setInstanceName(String instName)
        {
            instanceName = instName;
        }

        /**
         * the URL to the portmapper of this broker
         */
        public void setBrokerURL(MQAddress address) throws Exception
        {
             if (!local) {
                 throw new UnsupportedOperationException(
                    "Only the local broker can have its url changed");
             }

             MQAddress oldaddress = this.address;
             try {
                 updateEntry(HABrokerInfo.UPDATE_URL, null, address.toString());
                 this.address = address;
             } catch (Exception ex) {
                 logger.logStack(logger.ERROR, 
                     ex.getMessage()+"["+oldaddress+", "+address+"]"+brokerid, ex);
                 throw ex;
             }
             brokerChanged(ClusterReason.ADDRESS_CHANGED, 
                  this.getBrokerName(), oldaddress, this.address, null, null);

        }


        /**
         * resets the takeover broker
         */
         public void resetTakeoverBrokerReadyOperating() throws Exception
         {
             if (!local) {
                 throw new UnsupportedOperationException(
                 "Only the local broker can have its takeover broker reset");
             }
             getState();
             if (state == BrokerState.FAILOVER_PENDING ||
                 state == BrokerState.FAILOVER_STARTED) {
                 String otherb = getTakeoverBroker();
                 throw new IllegalStateException(
                     Globals.getBrokerResources().getKString(
                     BrokerResources.X_A_BROKER_TAKINGOVER_THIS_BROKER,
                     (otherb == null ? "":otherb), this.toString()));
             }
             try {
                 UID curssid = updateEntry(
                               HABrokerInfo.RESET_TAKEOVER_BROKER_READY_OPERATING,
                               state, getBrokerSessionUID());
                 if (curssid != null) {
                     this.session = curssid; 
                     localSessionUID = curssid; 
                 }
             } catch (Exception ex) {
                 logger.logStack(logger.ERROR, ex.getMessage()+"["+brokerid+"]", ex);
                 throw ex;
             }
         }

    
        /**
         * Is this the address of the broker running in this
         * VM
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
         * @throws UnsupportedOperationException if this change
         *         can not be made on this broker
         */
        public synchronized void setVersion(int version) throws Exception

        {
            Integer oldversion = this.version; 
            Integer newVersion = new Integer(version);
            if (local) {
                 try {
                     updateEntry(HABrokerInfo.UPDATE_VERSION, this.version, newVersion);
                     this.version = newVersion;
                 } catch (Exception ex) {
                     logger.logStack(logger.WARNING,
                         ex.getMessage()+"["+oldversion+", "+version+"]"+brokerid, ex);
                     throw ex;
                 }
            }
            brokerChanged(ClusterReason.VERSION_CHANGED, 
                  this.getBrokerName(), oldversion, this.version, null,  null);
        }  

    
        /**
         * sets the status of the broker (and notifies listeners).
         *
         * @param newstatus the status to set
         * @param userData optional user data
         * @see ConfigListener
         */
        public void setStatus(int newstatus, Object userData)
        {
            UID uid = null;
            Integer oldstatus = null;
            synchronized (this) {
                if (this.status.intValue() == newstatus)
                    return;
                uid = getBrokerSessionUID();
                oldstatus = this.status;
                this.status = new Integer(newstatus);
            }
            // notify
            if (! oldstatus.equals(this.status))
                brokerChanged(ClusterReason.STATUS_CHANGED, 
                  this.getBrokerName(), oldstatus, this.status, uid, userData);

        }


        /**
         * Updates the BROKER_UP bit flag on status.
         * 
         * @param userData optional user data
         * @param up setting for the bit flag (true/false)
         */
        public void setBrokerIsUp(boolean up, UID brokerSession, Object userData)
        {
        
            UID uid = brokerSession;
            Integer oldstatus = null;
            Integer newstatus = null;
            synchronized (this) {
                if (!up && !uid.equals(getBrokerSessionUID())) {
                    logger.log(logger.INFO, Globals.getBrokerResources().getKString(
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
                        // ok, we CANT have INDOUBT and BrokerDown
                        newStatus = BrokerStatus.setBrokerIsDown
                                        (this.status.intValue());
                        newStatus = BrokerStatus.setBrokerNotInDoubt
                                        (newStatus);
                    }
                    uid = getBrokerSessionUID();
                    this.status = new Integer(newStatus);
                    newstatus = this.status;
                }
            }
            // notify
            if (! oldstatus.equals(this.status))
                brokerChanged(ClusterReason.STATUS_CHANGED, 
                  this.getBrokerName(), oldstatus, newstatus, uid, userData);

        }

        /**
         * Updates the BROKER_LINK_UP bit flag on status.
         * 
         * @param userData optional user data
         * @param up setting for the bit flag (true/false)
         */
        public void setBrokerLinkUp(boolean up, Object userData)
        {
        
            UID uid = null;
            Integer oldstatus = null;
            synchronized (this) {
                oldstatus = this.status;

                int newStatus = 0;
                uid = getBrokerSessionUID();
                if (up) {
                   newStatus = BrokerStatus.setBrokerLinkIsUp
                        (this.status.intValue());
                } else {
                   newStatus = BrokerStatus.setBrokerLinkIsDown
                        (this.status.intValue());
                }
                this.status = new Integer(newStatus);
            }
            // notify
            if (! oldstatus.equals(this.status))
                brokerChanged(ClusterReason.STATUS_CHANGED, 
                      this.getBrokerName(), oldstatus, this.status, uid, userData);

        }


        /**
         * Updates the BROKER_INDOUBT bit flag on status.
         * 
         * @param userData optional user data
         * @param up setting for the bit flag (true/false)
         */
        public void setBrokerInDoubt(boolean up, Object userData)
        {
        
            UID uid = (UID)userData;
            Integer oldstatus = null;
            Integer newstatus = null;
            synchronized (this) {
                if (up && !uid.equals(getBrokerSessionUID())) {
                    logger.log(logger.INFO, Globals.getBrokerResources().getKString(
                               BrokerResources.I_INDOUBT_STATUS_ON_BROKER_SESSION,
                               "[BrokerSession:"+uid+"]", this.toString()));
                    oldstatus = new Integer(BrokerStatus.ACTIVATE_BROKER);
                    newstatus = BrokerStatus.setBrokerInDoubt(oldstatus);

                } else {

                    oldstatus = this.status;
                    int newStatus = 0;
                    if (up) {
                        newStatus = BrokerStatus.setBrokerInDoubt
                                        (this.status.intValue());
                    } else {
                        newStatus = BrokerStatus.setBrokerNotInDoubt
                                         (this.status.intValue());
                    }
                    uid = getBrokerSessionUID();
                    this.status = new Integer(newStatus);
                    newstatus = this.status;
                }
            }
            // notify
            if (! oldstatus.equals(this.status))
                brokerChanged(ClusterReason.STATUS_CHANGED, 
                      this.getBrokerName(), oldstatus, newstatus, uid, userData);

        }

        /**
         * marks this broker as destroyed. This is equivalent to setting
         * the status of the broker to DOWN.
         *
         * @see BrokerStatus#setBrokerIsDown
         */
        public void destroy() {
            synchronized (this) {
                status = new Integer(BrokerStatus.setBrokerIsDown(
                              status.intValue()));
            }
        }


        /**
         * Gets the UID associated with the store session.
         *
         * @return the store session uid (if known)
         */
        public synchronized UID getStoreSessionUID()
        {
            return session;
        }

        /**
         * Gets the UID associated with the broker session.
         *
         * @return the broker session uid (if known)
         */
        public synchronized UID getBrokerSessionUID()
        {
            return brokerSessionUID;
        }

    
        /**
         * Sets the UID associated with the broker session.
         *
         * @param uid the new broker session uid 
         */
        public synchronized void setBrokerSessionUID(UID uid)
        {
             brokerSessionUID = uid;
        }

        public boolean isBrokerIDGenerated()
        {
            return false;
        }


      /**
       * Retrieves the id of the broker who has taken over this broker's store.
       *
       * @return the broker id of the takeover broker (or null if there is not
       *      a takeover broker).
       */
        public synchronized String getTakeoverBroker() throws BrokerException
        {
            HABrokerInfo bkrInfo = Globals.getStore().getBrokerInfo(brokerid);
            if (bkrInfo == null) {
                logger.log(Logger.ERROR,
                    BrokerResources.E_BROKERINFO_NOT_FOUND_IN_STORE, brokerid);
                return null;
            }
            takeoverBroker = bkrInfo.getTakeoverBrokerID();
            return takeoverBroker;
        }
    

       /**
       * Returns the heartbeat timestamp associated with this broker.
       * <b>Note:</b> the heartbeat is always retrieved from the store
       * before it is returned (so its current).
       * 
       *  @return the heartbeat in milliseconds
       *  @throws BrokerException if the heartbeat can not be retrieve.
       */
        public long getHeartbeat() 
            throws BrokerException
        {
            heartbeat = Globals.getStore().getBrokerHeartbeat(brokerid);
            return heartbeat;
        }

        public synchronized long updateHeartbeat() throws BrokerException {
            return updateHeartbeat(false);
        }

        /**
         *  Update the timestamp associated with this broker.
         *  @return the updated heartbeat in milliseconds
         * @throws BrokerException if the heartbeat can not be set or retrieve.
         */
        public synchronized long updateHeartbeat(boolean reset)
            throws BrokerException
        {
            // this one always accesses the backing store
            Store store = Globals.getStore();
            Long newheartbeat  = null; 

            if (reset) {
                if ((newheartbeat = store.updateBrokerHeartbeat(brokerid)) == null) {
                    throw new BrokerException(Globals.getBrokerResources().getKString(
                          BrokerResources.X_UPDATE_HEARTBEAT_TS_2_FAILED, brokerid,
                          "Failed to reset heartbeat timestamp."));
                }
            } else {

            if ((newheartbeat = store.updateBrokerHeartbeat(brokerid, heartbeat)) == null) {
                // TS is out of sync so log warning msg and force update
                logger.log(Logger.WARNING,
                    Globals.getBrokerResources().getKString(
                    BrokerResources.X_UPDATE_HEARTBEAT_TS_2_FAILED, brokerid,
                    "Reset heartbeat timestamp due to synchronization problem." ) );

                if ((newheartbeat = store.updateBrokerHeartbeat(brokerid)) == null) {
                    // We really have a problem
                    throw new BrokerException(
                        Globals.getBrokerResources().getKString(
                        BrokerResources.X_UPDATE_HEARTBEAT_TS_2_FAILED, brokerid,
                        "Failed to reset heartbeat timestamp."));
                }
            }
            }

            heartbeat = newheartbeat.longValue();
            return heartbeat;
        }
    
        /**
         * updates the broker entry (this may be just timestamp or
         * state, sessionid or brokerURL may be changed)
         *
         * @param updateType update type
         * @param oldValue old value depending on updateType
         * @param newValue new value depending on updateType
         * @return current store session UID if requested by updateType
         * @throws IllegalStateException if the information stored
         *               with the broker does not match what we expect
         * @throws IllegalAccessException if the broker does not have
         *               permission to change the broker (e.g. one broker
         *               is updating anothers timestamp).
         */
        private synchronized UID updateEntry(int updateType, 
                                             Object oldValue, Object newValue)
                                             throws Exception
        {
             if (!local) {
                 throw new IllegalAccessException(
                 "Can not update entry " + " for broker " + brokerid);
             }

             Store store = Globals.getStore();
             UID ssid = store.updateBrokerInfo(brokerid, updateType, oldValue, newValue);
             try {
                 heartbeat = store.getBrokerHeartbeat(brokerid);
             } catch (Exception ex) {
                 logger.logStack(logger.WARNING, ex.getMessage()+"["+brokerid+"]", ex);
             }
             return ssid;
        }


    
        /**
         * gets the state of the broker .
         * <b>Note:</b> the state is always retrieved from the store
         * before it is returned (so its current).
         * 
         *
         * @throws BrokerException if the state can not be retrieve
         * @return the current state
         */
        public BrokerState getState()
            throws BrokerException
        {
//          this should always retrieve the state on disk (I think)
            BrokerState oldState = state;
            state =  Globals.getStore().getBrokerState(brokerid);
            if (oldState != state && state != BrokerState.FAILOVER_PENDING) {
                 brokerChanged(ClusterReason.STATE_CHANGED, 
                      this.getBrokerName(), oldState, this.state, null,  null);
            }
            return state;
 
        }

        public void setStateFailoverProcessed(UID storeSession) throws Exception {
            if (local) {
                throw new IllegalAccessException(
                "Cannot update self state to "+BrokerState.FAILOVER_PROCESSED);
            }
            BrokerState newstate = BrokerState.FAILOVER_PROCESSED;
            try {
                BrokerState oldState = getState();
                synchronized(this) {
                    if (storeSession.equals(session)) {
                        state = newstate;
                    } else {
                        oldState = BrokerState.FAILOVER_COMPLETE;
                    }
                }

                brokerChanged(ClusterReason.STATE_CHANGED, 
                    this.getBrokerName(), oldState, newstate, storeSession,  null);

            } catch (Exception ex) {
                IllegalStateException e = 
                     new IllegalStateException("Failed to update state "+
                             BrokerState.FAILOVER_COMPLETE+" for " + brokerid);
                 e.initCause(ex);
                 throw e;
            }
        }

        public void setStateFailoverFailed(UID brokerSession) throws Exception {
            if (local) {
                throw new IllegalAccessException(
                "Cannot update self state to "+BrokerState.FAILOVER_FAILED);
            }
            BrokerState newstate = BrokerState.FAILOVER_FAILED;
            try {
                BrokerState oldState = getState();
                synchronized(this) {
                    if (brokerSessionUID.equals(brokerSession)) {
                        state = newstate;
                    } else {
                        oldState = BrokerState.OPERATING;
                    }
                }

                brokerChanged(ClusterReason.STATE_CHANGED, 
                    this.getBrokerName(), oldState, newstate, brokerSession,  null);

            } catch (Exception ex) {
                IllegalStateException e = 
                     new IllegalStateException("Failed to update state to "+
                             BrokerState.FAILOVER_FAILED+ " for " + brokerid);
                 e.initCause(ex);
                 throw e;
            }
        }

    
        /**
         * sets the persistent state of the broker 
         * @throws IllegalAccessException if the broker does not have
         *               permission to change the broker (e.g. one broker
         *               is updating anothers state).
         * @throws IllegalStateException if the broker state changed
         *               unexpectedly.
         * @throws IndexOutOfBoundsException if the state value is not allowed.
         */
        public void setState(BrokerState newstate)
             throws IllegalAccessException, IllegalStateException,
                    IndexOutOfBoundsException
        {
             if (!local && newstate != BrokerState.FAILOVER_PROCESSED
                 && newstate != BrokerState.FAILOVER_STARTED
                 && newstate != BrokerState.FAILOVER_COMPLETE
                 && newstate != BrokerState.FAILOVER_FAILED) {
                 // a remote broker should only be updated during failover
                 // FAILOVER_PENDING is set with specific method
                 throw new IllegalAccessException("Cannot update state "
                      + " for broker " + brokerid);
             }

             try {
                 BrokerState oldState = getState();
                 if (newstate != BrokerState.FAILOVER_PENDING
                     && newstate != BrokerState.FAILOVER_PROCESSED
                     && newstate != BrokerState.FAILOVER_FAILED) {
                     if (!Globals.getStore().updateBrokerState(brokerid, newstate, state, local))
                     throw new IllegalStateException(
                     "Could not update broker state from "+oldState+" to state "+newstate+ " for " + brokerid);
                 }
                 state = newstate;
                 brokerChanged(ClusterReason.STATE_CHANGED, 
                      this.getBrokerName(), oldState, this.state, null,  null);
             } catch (BrokerException ex) {
                 IllegalStateException e = 
                     new IllegalStateException(
                           Globals.getBrokerResources().getKString(
                               BrokerResources.E_INTERNAL_BROKER_ERROR,
                               "Failed to update state for " + brokerid));
                 e.initCause(ex);
                 throw e;
             }
        }

        /**
         * attempt to take over the persistent state of the broker
         * 
         * @throws IllegalStateException if this broker can not takeover.
         * @return data associated with previous broker
         */
        public TakeoverStoreInfo takeover(boolean force, TakingoverTracker tracker)
                                          throws BrokerException
        {
            int delay = config.getIntProperty(
                Globals.IMQ + ".cluster.takeover.delay.interval", 0);
            if (delay > 0) {
                try {
                    Thread.sleep(delay*1000L);
                } catch (InterruptedException e) {}
            }             

            boolean gotLock = false;
             boolean sucessful = false;
             BrokerState curstate = getState();

             if (!force) {
                 if (curstate == BrokerState.INITIALIZING || 
                     curstate == BrokerState.SHUTDOWN_STARTED ||
                     curstate == BrokerState.SHUTDOWN_COMPLETE ) {
                         throw new BrokerException(
                             Globals.getBrokerResources().getKString(
                               BrokerResources.I_NOT_TAKEOVER_BKR,
                               brokerid),
                             Status.NOT_ALLOWED);
                 }
                 if (curstate == BrokerState.FAILOVER_PENDING ||
                     curstate == BrokerState.FAILOVER_STARTED ||
                     curstate == BrokerState.FAILOVER_COMPLETE ||
                     curstate == BrokerState.FAILOVER_PROCESSED) { 
                         throw new BrokerException(
                             Globals.getBrokerResources().getKString(
                               BrokerResources.I_NOT_TAKEOVER_BKR,
                               brokerid),
                             Status.CONFLICT);
                 }
             }

             long newtime = System.currentTimeMillis();
             BrokerState newstate = BrokerState.FAILOVER_PENDING;
             Globals.getStore().getTakeOverLock(localBroker, brokerid, tracker.getLastHeartbeat(),
                         curstate, newtime, newstate, force, tracker);
             gotLock = true;
             state = newstate;
             logger.log(Logger.DEBUG,"state = FAILOVER_PENDING " + brokerid);
             brokerChanged(ClusterReason.STATE_CHANGED, 
                      brokerid, curstate, newstate , null,  null);

             TakeoverStoreInfo o = null;
             try {
                 // OK, explicitly retrieve old state from disk
                logger.log(Logger.DEBUG,"state = FAILOVER_STARTED " + brokerid);
                setState(BrokerState.FAILOVER_STARTED);
                o = Globals.getStore().takeOverBrokerStore(localBroker, brokerid, tracker);
                logger.log(Logger.DEBUG,"state = FAILOVER_COMPLETE " + brokerid);
                 // fix for bug 6319711
                 // higher level processing needs to set 
                 // failover complete AFTER routing is finished
                 //REMOTE: setState(BrokerState.FAILOVER_COMPLETE);

                 sucessful = true;
             } catch (IllegalAccessException ex) {
                 throw new RuntimeException("Internal error, shouldnt happen", ex);
             } finally {
                 if (gotLock && !sucessful) {
                     try {
                         setStateFailoverFailed(tracker.getBrokerSessionUID());
                     } catch (Exception ex) {
                         logger.log(logger.INFO,
                         "Unable to set state to failed for broker "
                          +this+": "+ex.getMessage(), ex);
                     }
                     logger.log(Logger.WARNING,"Failed to takeover :"
                              + brokerid + " state expected is " + curstate );
                 }
             }
             heartbeat = newtime;
             addSupportedStoreSessionUID(session);
             takeoverBroker=localBroker;
             return o;
        }

        /**
         * Is the broker static or dynmically configured
         */
        public boolean isConfigBroker()
        {
             return true;
        }

   }



// OK, we need to verify the list of brokers in the following
// situations:
//          - a broker is added
//          - a broker is removed (does NOT update database)
//          - the first time a broker is started
//

   /**
    * A subclass of Map which knows how to populate itself from
    * the jdbc store.
    */
   public class HAMap extends HashMap implements Map
   {
        /**
         * Create an instance of  HAMap.
         * @throws BrokerException if something goes wrong loading the
         *                         jdbc store.
         */
        public HAMap() throws BrokerException
        {
            // OK, load everything in the store that is OPERATING
            Map map = Globals.getStore().getAllBrokerInfos();
            Iterator itr = map.entrySet().iterator();

            while (itr.hasNext()) {
                Map.Entry entry = (Map.Entry)itr.next();
                String key = (String)entry.getKey();
                HABrokerInfo bi = (HABrokerInfo)entry.getValue();
                HAClusteredBroker cb =  new HAClusteredBrokerImpl(
                       bi.getId(), bi);
                put(key,cb);
                brokerChanged(ClusterReason.ADDED, cb.getBrokerName(),
                     null, cb, cb.getBrokerSessionUID(), null);
            }
        }


        /**
         * Method which reloads the contents of this map from the
         * current information in the JDBC store.
         * @throws BrokerException if something goes wrong loading the
         *                         jdbc store.
         */
        public void updateHAMap() 
            throws BrokerException
        {
            updateHAMap(false);
        }

        public void updateHAMap(boolean all)
            throws BrokerException
        {
            if (all) {
                updateHAMapForState(null);
            } else {
                updateHAMapForState(BrokerState.OPERATING);
            } 
        }

        private void updateHAMapForState(BrokerState state)
            throws BrokerException
        {
            // OK, load everything in the store that is the right state
            Map map;
            if (state == null) {
                // Load everything if state is not specified
                map = Globals.getStore().getAllBrokerInfos();
            } else {
                map = Globals.getStore().getAllBrokerInfoByState(state);
            }

            Iterator itr = map.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry entry = (Map.Entry)itr.next();
                String key = (String)entry.getKey();
                HABrokerInfo bi = (HABrokerInfo)entry.getValue();
                HAClusteredBrokerImpl impl = (HAClusteredBrokerImpl)get(key);
                if (impl == null) {
                    HAClusteredBroker cb =
                        new HAClusteredBrokerImpl(bi.getId(), bi);
                    put(key,cb);
                    brokerChanged(ClusterReason.ADDED, cb.getBrokerName(),
                         null, cb, cb.getBrokerSessionUID(), null);
                } else { // update
                    // already exists
                    impl.update(bi);
                }
            }

            // Sanity check when do load everything from the DB; remove from
            // memory if any broker has been removed from the broker table
            if (state == null) {
                itr = entrySet().iterator();
                while (itr.hasNext()) {
                    Map.Entry entry = (Map.Entry)itr.next();
                    String key = (String)entry.getKey();
                    if (!map.containsKey(key)) {
                        itr.remove();
                        HAClusteredBrokerImpl impl = (HAClusteredBrokerImpl)entry.getValue();
                        brokerChanged(ClusterReason.REMOVED, impl.getBrokerName(),
                             impl, null, impl.getBrokerSessionUID(), null);
                    }
                }
            }
        }

          
        /**
         * Retrieves the HAClusteredBroker associated with the passed in 
         * broker id. 
         * If the id is not found in the hashtable, the database will be
         * checked.
         * @param key the brokerid to lookup
         * @return the HAClusteredBroker object (or null if one can't be found)
         */
         public Object get(Object key) {
             return get(key, false);
         }

        /**
         * Retrieves the HAClusteredBroker associated with the passed in 
         * broker id. 
         * If the id is not found in the hashtable, the database will be
         * checked.
         * @param key the brokerid to lookup
         * @param update update against store
         * @return the HAClusteredBroker object (or null if one can't be found)
         */
        public Object get(Object key, boolean update) {
            // always check against the backing store
            Object o = super.get(key);
            if (o == null || update) {
                try {
                    HABrokerInfo m= Globals.getStore().getBrokerInfo((String)key);
                    if (m != null && o == null) {
                         HAClusteredBroker cb =  new HAClusteredBrokerImpl(
                           (String)key, m);
                         put(key,cb);
                         brokerChanged(ClusterReason.ADDED, cb.getBrokerName(),
                             null, cb, cb.getBrokerSessionUID(), null);
                         o = cb;
                     }
                     if (m != null && update) {
                         ((HAClusteredBrokerImpl)o).update(m);
                     }
                 } catch (BrokerException ex) {
                         logger.log(Logger.INFO,
                                BrokerResources.E_INTERNAL_BROKER_ERROR,
                                " exception while creating broker entry "
                                 + key , ex);
                }
            }
            return o;
        }
   }

}
