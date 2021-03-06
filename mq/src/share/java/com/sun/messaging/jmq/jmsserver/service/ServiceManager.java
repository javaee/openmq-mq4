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
 * @(#)ServiceManager.java	1.43 06/29/07
 */ 

package com.sun.messaging.jmq.jmsserver.service;

import java.util.*;
import java.io.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.Broker;
import com.sun.messaging.jmq.jmsserver.config.BrokerConfig;
import com.sun.messaging.jmq.jmsserver.util.*;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.util.ServiceType;
import com.sun.messaging.jmq.util.ServiceState;
import com.sun.messaging.jmq.jmsserver.resources.BrokerResources;
import com.sun.messaging.jmq.jmsserver.management.agent.Agent;

/**
 * This class is the manager which creates Services
 * by calling the service handler associated with the
 * service type.<P>
 *
 * A service is defined with a minimum set of properties:<BR>
 * <BLOCKQUOTE>
 *     jmq.service.<instance name>.type = <class type>
 *     jmq.service.serviceclass.<classtype>.class = <class>
 *     jmq.service.serviceclass.<classtype>.props =
 *              < , seperated list of properties to watch>
 *
 * </BLOCKQUOTE> <BR>
 *   e.g. a "standard service" uses both a protocol (with
 *   its properties and a service<P>
 *
 *   A service called "broker" properties might look like:<BR>
 *
 * <BLOCKQUOTE>
 *      # list of known services
 *      jmq.service.list = broker,admin,ssl,http,httpadmin
 *
 *      # list of active services
 *      jmq.service.activelist = broker,admin
 *
 *      # definitions for the standard service
 *      jmq.service.broker.type=dedicated
 *      jmq.service.serviceclass.standard.class=
 *                com.sun.messaging.jmq.jmsserver.service.imq.IMQService
 * </BLOCKQUOTE> <BR>
 *
 * This code does a lot of string manipulation when the service is
 * created which will make it fairly slow.  Since this code is
 * rarely called after startup, the performance should be acceptable.     
 */


public class ServiceManager
{

    private static boolean DEBUG = false;
    private Logger logger = Globals.getLogger();

    private static final String DEFAULT_HANDLER="dedicated";

    // maps services names to services & service handler instances
    Hashtable services= null;

    // maps service handler names -> service handler class
    Hashtable servicehandlers = null;

    BrokerConfig config = Globals.getConfig();

    ConnectionManager conmgr = null;

    public ServiceManager(ConnectionManager conmgr) {
        services = new Hashtable();
        servicehandlers = new Hashtable();
        this.conmgr = conmgr;
    }

    private ServiceFactory createServiceFactory(String handlername)
        throws ClassNotFoundException, 
               InstantiationException, 
               IllegalAccessException
    {
        String classname = config.getProperty(Globals.IMQ +
                    ".service_handler." + handlername + ".class");
        if (classname == null)
            throw new ClassNotFoundException(Globals.getBrokerResources().getString(
                    BrokerResources.X_INTERNAL_EXCEPTION,"Unable to locate class for " +
                    handlername + ", property is " + Globals.IMQ +
                        ".service_handler."
                     + handlername + ".class" + " value is " + classname ));
        ServiceFactory hdlr = (ServiceFactory)
                      Class.forName(classname).newInstance();
        hdlr.setConnectionManager(conmgr);
	return hdlr;
               
    }

    private int getServiceStateProp(String service) 
    {

        String servicetypestr= config.getProperty(Globals.IMQ + "." + 
                  service + ".state");
        if (servicetypestr == null) return ServiceState.UNKNOWN;
        
        return ServiceState.getStateFromString(servicetypestr);
    }

    private void removeServiceStateProp(String service)  
          throws IOException
    {
         config.updateRemoveProperty(Globals.IMQ + "." + 
                  service + ".state", true);

    }

   private void setServiceStateProp(String service, int state) 
   {
        try {
            if (state != ServiceState.PAUSED) {
                removeServiceStateProp(service);
                return;
            }
            String statestr = ServiceState.getString(state);
            config.updateProperty(Globals.IMQ + "."+service+".state", statestr);
        } catch (Exception ex) {
            logger.logStack(Logger.WARNING,
                BrokerResources.E_INTERNAL_BROKER_ERROR, 
                "storing service state for " + service, ex);
        }
    }

    private String getHandlerName(String service)
    {
	String handlerName = null;

        handlerName =  config.getProperty(Globals.IMQ + "." + service +
                            ".handler_name");

	if (handlerName == null)  {
            handlerName =  config.getProperty(Globals.IMQ + "." + service +
                ".threadpool_model",DEFAULT_HANDLER);
	}

	return (handlerName);
    }

    public Service createService(String service) 
        throws ClassNotFoundException, 
               InstantiationException, 
               IllegalAccessException,
               BrokerException
    {
        String handlername = getHandlerName(service);
        ServiceFactory handler = (ServiceFactory)servicehandlers.get(handlername);
        if (handler == null) { // create a new handler
            handler = createServiceFactory(handlername);
            servicehandlers.put(handlername, handler);
        }
        String servicetypestr= config.getProperty(Globals.IMQ + "." +
            service + ".servicetype");
//XXX

        int servicetype = ServiceType.getServiceType(servicetypestr);

        if (DEBUG) {
            logger.log(Logger.DEBUG,
                 "Creating service {0} of type {1}",
                 service, ServiceType.getString(servicetype));
        }

        // OK now get the service type
        Service s = handler.createService(service, servicetype);

        ServiceInfo si = new ServiceInfo(s, handler);
       
        services.put(service, si);

        return s;
    
    }

    public static List getAllServiceNames() {
	List activateList = Globals.getConfig().getList(Globals.IMQ + ".service.activate"),
	     allSvcNames = Globals.getConfig().getList(Globals.IMQ + ".service.list"),
	     additionalSvcNames = Globals.getConfig().getList(Globals.IMQ + ".service.runtimeAdd");

    if (additionalSvcNames != null) {
        if (allSvcNames != null) allSvcNames.addAll(additionalSvcNames);
        else allSvcNames = additionalSvcNames;
    }

	if (activateList != null)  {
	    Iterator iter = activateList.iterator();
	    while (iter.hasNext())  {
	        String service = (String)iter.next();

	        if (!allSvcNames.contains(service))  {
	            allSvcNames.add(service);
	        }
	    }
	}
        return (allSvcNames);
    }

    public static List getAllActiveServiceNames() {
	List activateList = Globals.getConfig().getList(Globals.IMQ + ".service.activate"),
	     allActiveSvcNames = Globals.getConfig().getList(Globals.IMQ + ".service.activelist"),
	     additionalSvcNames = Globals.getConfig().getList(Globals.IMQ + ".service.runtimeAdd");

    if (additionalSvcNames != null) {
        if (allActiveSvcNames != null) allActiveSvcNames.addAll(additionalSvcNames);
        else allActiveSvcNames = additionalSvcNames;
    }

	if (activateList != null)  {
	    Iterator iter = activateList.iterator();
	    while (iter.hasNext())  {
	        String service = (String)iter.next();

	        if (!allActiveSvcNames.contains(service))  {
	            allActiveSvcNames.add(service);
	        }
	    }
	}
        return (allActiveSvcNames);
    }

    public static String getServiceTypeString(String name) {
        return Globals.getConfig().getProperty(Globals.IMQ + "." + name + ".servicetype");
    }

    public Set getAllActiveServices() {
        return services.keySet();
    }

    public int getServiceState(String name) {
        ServiceInfo info = (ServiceInfo)services.get(name);
        if (info == null) return ServiceState.UNKNOWN;
        return info.getState();
    }

    public int getServiceType(String name) {
        ServiceInfo info = (ServiceInfo)services.get(name);
        if (info == null) return ServiceType.UNKNOWN;
        return info.getServiceType();
   }

   /**
    * Get a Service by its name
    */
   public Service getService(String name) {
        ServiceInfo info = (ServiceInfo)services.get(name);
        if (info == null) return null;
	return info.getService();
   }


    /**
     * start a service, by name 
     */
    public void startService(String servicename, boolean pauseAtStartup)
        throws BrokerException
    {
        ServiceInfo info = (ServiceInfo)services.get(servicename);
        if (info != null) {
            info.start(pauseAtStartup);
	    setServiceStateProp(servicename, ServiceState.RUNNING);
        } else { // handle error
        }
    }

    /**
     * Stop a service, by name.
     * Stopping a service frees up all available resources
     */
    public void stopService(String servicename, boolean all)
        throws BrokerException
    {
        ServiceInfo info = (ServiceInfo)services.get(servicename);
        if (info != null) {
            info.stop(all);
        } else { // handle error
        }
    }

    /**
     * Pause a service by name either stoping just new
     * connections or stopping all interaction
     */
    public void pauseService(String servicename, boolean pause_all)
        throws BrokerException
    {
        ServiceInfo info = (ServiceInfo)services.get(servicename);
        if (info != null) {
            info.pause(pause_all);
	    setServiceStateProp(servicename, ServiceState.PAUSED);

            Agent agent = Globals.getAgent();
            if (agent != null)  {
                agent.notifyServicePause(servicename);
	    }
        } else { // handle error
        }
    }

    /**
     * stop new connections to a service by name
     */
    public void stopNewConnections(String servicename)
        throws BrokerException
    {
        ServiceInfo info = (ServiceInfo)services.get(servicename);
        if (info != null) {
            info.stopNewConnections();
        } else { // handle error
        }
    }

    /**
     * stop new connections to a service by name
     */
    public void startNewConnections(String servicename)
        throws BrokerException
    {
        ServiceInfo info = (ServiceInfo)services.get(servicename);
        if (info != null) {
            info.startNewConnections();
        } else { // handle error
        }
    }

    /**
     * all connections for all active services of the specified type
     */
    public void startNewConnections(int service_type)
        throws BrokerException{

        Set activeServices = getAllActiveServices();
        Iterator iter = activeServices.iterator();
        while (iter.hasNext()) {
            String name = (String)iter.next();
            Service service = getService(name);

            if (getServiceType(name) == service_type &&
                  service.getState() != ServiceState.RUNNING) {
                startNewConnections(name);
            }
                
        }
    }

    /**
     * connection count for all services of passed in type
     */
    public int getConnectionCount(int service_type)
    {
        int count = 0;
        Set activeServices = getAllActiveServices();
        Iterator iter = activeServices.iterator();
        while (iter.hasNext()) {
            String name = (String)iter.next();
            Service service = getService(name);

            if (getServiceType(name) == service_type ) {
                count += conmgr.getNumConnections(service);
            }
                
        }
        return count;
    }


    /**
     * Resume a paused service
     */
    public void resumeService(String servicename)
        throws BrokerException
    {
        ServiceInfo info = (ServiceInfo)services.get(servicename);
        if (info != null) {
            info.resume();
	    setServiceStateProp(servicename, ServiceState.RUNNING);

            Agent agent = Globals.getAgent();
            if (agent != null)  {
                agent.notifyServiceResume(servicename);
	    }
        } else { // handle error
        }
    }


    /**
     * Stop all active services
     */
    public void stopAllActiveServices(boolean all)
        throws BrokerException
    {
        Set activeServices = getAllActiveServices();
        Iterator iter = activeServices.iterator();
        while (iter.hasNext()) {
            String name = (String)iter.next();
            Service service = getService(name);
            stopService(name, all);
        }
    }

    /**
     * Pause all active services of the specified type
     */
    public void stopNewConnections(int service_type)
        throws BrokerException{

        Set activeServices = getAllActiveServices();
        Iterator iter = activeServices.iterator();
        while (iter.hasNext()) {
            String name = (String)iter.next();
            Service service = getService(name);

            if (getServiceType(name) == service_type &&
                  service.getState() == ServiceState.RUNNING) {
                stopNewConnections(name);
            }
                
        }
    }

    /**
     * Pause all active services of the specified type
     */
    public void pauseAllActiveServices(int service_type, boolean pause_all)
        throws BrokerException{

        Set activeServices = getAllActiveServices();
        Iterator iter = activeServices.iterator();
        while (iter.hasNext()) {
            String name = (String)iter.next();
            Service service = getService(name);

            if (getServiceType(name) == service_type &&
                  service.getState() == ServiceState.RUNNING) {
                pauseService(name, pause_all);
            }
                
        }
    }

    /**
     * Pause all active services of the specified type
     */
    public void resumeAllActiveServices(int service_type)
        throws BrokerException{
         resumeAllActiveServices(service_type, false);
    }

    public void resumeAllActiveServices(int service_type, boolean startup)
        throws BrokerException{

        try {

        Set activeServices = getAllActiveServices();
        Iterator iter = activeServices.iterator();
        while (iter.hasNext()) {
            String name = (String)iter.next();
            Service service = getService(name);

            if (getServiceType(name) == service_type &&
                  service.getState() == ServiceState.PAUSED) {
                resumeService(name);
            }
                
        }

        } finally {
        if (startup) {
            Globals.getTransactionList().postProcess();
        }
        }
    }

    /**
     * Update the list of services
     */
    public void updateServiceList(List updatedsvcs) {
    	updateServiceList(updatedsvcs, ServiceType.ALL);
    }

    public void updateServiceList(List updatedsvcs, int service_type) {
        updateServiceList(updatedsvcs, service_type, false);
    }

    public void updateServiceList(List updatedsvcs, int service_type,
          boolean pauseIfStarting ) {
        updateServiceList(updatedsvcs, service_type, pauseIfStarting, false);
    }

    public void updateServiceList(List updatedsvcs, int service_type,
        boolean pauseIfStarting, boolean startup )
    {
        try {

        // two stages ...
        // first stop/destroy no longer used services
        // then start/create existing services

        // destroying services
        Set running = getAllActiveServices();
        Iterator itr = running.iterator();

        while (itr.hasNext()) {
            String service = (String)itr.next();
	    if (service_type != ServiceType.ALL &&
	    	getServiceType(service) != service_type)
		continue;

            if (!updatedsvcs.contains(service)) {
                if (DEBUG) {
                    logger.log(Logger.DEBUG,"Destroying service {0}", service);
                }
                ServiceInfo info = (ServiceInfo)services.get(service);
                try {
                    info.stop(true);
                } catch (Exception ex) {
                    logger.logStack(Logger.WARNING,
                        BrokerResources.W_CANT_STOP_SERVICE, service, ex);
                }
                try {
                    info.destroy();
                } catch (Exception ex) {
                    logger.logStack(Logger.WARNING,
                        BrokerResources.W_CANT_DESTROY_SERVICE, service, ex);
                }
                setServiceStateProp(service, ServiceState.UNKNOWN);
                services.remove(service);
            }
        }               
 

        for (int i =0; i < updatedsvcs.size(); i ++) {
            String service = (String)updatedsvcs.get(i);
            if (DEBUG) {
                logger.log(Logger.DEBUG,"Checking service {0}", service);
            }
            ServiceInfo info = (ServiceInfo)services.get(service);

	    try {
		int state = getServiceStateProp(service);

		if (info == null ||
		    info.getState() == ServiceState.DESTROYED ) {
		    // no service
		    if (DEBUG) {
			logger.log(Logger.DEBUG,
			"Creating service {0}", service);
		    }
                    createService(service);
		}
		/*
		 * Falcon HA : Update services selectively.  During
		 * initialization, only ADMIN services are started.
		 * Normal services remain in UNINITIALIZED state.
		 */
		if (service_type == ServiceType.ALL ||
		    getServiceType(service) == service_type) {

		    int curstate =  (info == null ?  
			   ServiceState.UNKNOWN : info.getState());
		    if (curstate < ServiceState.RUNNING || 
			  curstate > ServiceState.PAUSED)
			startService(service, pauseIfStarting);
		    if (state == ServiceState.PAUSED && 
			curstate != ServiceState.PAUSED) {
                        logger.log(Logger.INFO,BrokerResources.I_PAUSING_SVC, 
                               service);
			pauseService(service, true);
		    }
		}
	    } catch (BrokerException ex) {
		String str = ex.getMessage();
		if (ex.getCause() != null) {
		    str += (": " + ex.getCause().getMessage());
		}
		logger.log(Logger.ERROR, BrokerResources.E_ERROR_STARTING_SERVICE, service + ": " + str);
		logger.log(Logger.DEBUG, "",  ex);
	    } catch (OutOfMemoryError err) {
		// throw error up to be handled by memory handling
		throw err;
	    } catch (Throwable ex) {
		logger.log(Logger.ERROR, BrokerResources.E_ERROR_STARTING_SERVICE,service, ex);
	    }
	}

        } finally {
        if (startup) {
            Globals.getTransactionList().postProcess();
        }
        }
    }

    public void addServiceRestriction(int service_type, ServiceRestriction svcres) {
        Set activeServices = getAllActiveServices();
        Iterator itr = activeServices.iterator();
        Service service = null;
        while (itr.hasNext()) {
            String name = (String)itr.next();
            if (getServiceType(name) != service_type) continue;
            service = getService(name);
            if (service == null) continue;
            service.addServiceRestriction(svcres);
        }
    }

    public void removeServiceRestriction(int service_type, ServiceRestriction svcres) {
        Set activeServices = getAllActiveServices();
        Iterator itr = activeServices.iterator();
        Service service = null;
        while (itr.hasNext()) {
            String name = (String)itr.next();
            if (getServiceType(name) != service_type) continue;
            service = getService(name);
            if (service == null) continue;
            service.removeServiceRestriction(svcres);
        }
    }

}

class ServiceInfo {
    Service service;
    ServiceFactory handler;

    public ServiceInfo(Service service, ServiceFactory handler)
    {
        this.service = service;
        this.handler = handler;
    }
    public Service getService() {
        return service;
    }
    public ServiceFactory getServiceFactory() {
        return handler;
    }

    public int getState() {
        return service.getState();
    }

    public int getServiceType() {
        return service.getServiceType();
    }

    public void start(boolean pauseAtStart) 
        throws BrokerException
    {
        handler.updateService(service);
        handler.startMonitoringService(service);
        service.startService(pauseAtStart);
    }

    public void stop(boolean all) 
        throws BrokerException
    {
        service.stopService(all);
        handler.stopMonitoringService(service);
    }

    public void pause(boolean all)
        throws BrokerException
    {

        service.pauseService(all);
    }

    public void stopNewConnections()
    {

        try {
            service.stopNewConnections();
        } catch (IOException ex) {
        }
    }

    public void startNewConnections()
    {

        try {
            service.startNewConnections();
        } catch (IOException ex) {
        }
    }

    public void resume() 
        throws BrokerException
    {

        service.resumeService();
    }

    public void destroy() 
        throws BrokerException
    {

        stop(true);
        service.destroyService();
    }
}
