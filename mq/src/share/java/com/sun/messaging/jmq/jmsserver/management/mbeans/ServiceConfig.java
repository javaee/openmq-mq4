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
 * @(#)ServiceConfig.java	1.18 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.management.mbeans;

import java.util.Date;
import java.util.Properties;

import java.io.IOException;

import javax.management.ObjectName;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.AttributeChangeNotification;
import javax.management.MBeanException;

import com.sun.messaging.jms.management.server.*;
import com.sun.messaging.jmq.jmsserver.Globals;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;
import com.sun.messaging.jmq.jmsserver.management.util.ServiceUtil;
import com.sun.messaging.jmq.jmsserver.config.ConfigListener;
import com.sun.messaging.jmq.jmsserver.config.PropertyUpdateException;
import com.sun.messaging.jmq.jmsserver.service.Service;
import com.sun.messaging.jmq.jmsserver.service.ServiceManager;
import com.sun.messaging.jmq.jmsserver.service.imq.IMQService;
import com.sun.messaging.jmq.jmsserver.management.agent.Agent;
import com.sun.messaging.jmq.util.admin.ServiceInfo;
import com.sun.messaging.jmq.util.ServiceType;
import com.sun.messaging.jmq.util.log.Logger;
import com.sun.messaging.jmq.Version;

public class ServiceConfig extends MQMBeanReadWrite implements ConfigListener  {
    private String service = null;

    private Properties brokerProps = null;
    private boolean propsStale = true;

    private static MBeanAttributeInfo[] attrs = {
	    new MBeanAttributeInfo(ServiceAttributes.MAX_THREADS,
					Integer.class.getName(),
					mbr.getString(mbr.I_SVC_ATTR_MAX_THREADS),
					true,
					true,
					false),

	    new MBeanAttributeInfo(ServiceAttributes.MIN_THREADS,
					Integer.class.getName(),
					mbr.getString(mbr.I_SVC_ATTR_MIN_THREADS),
					true,
					true,
					false),

	    new MBeanAttributeInfo(ServiceAttributes.NAME,
					String.class.getName(),
					mbr.getString(mbr.I_SVC_ATTR_NAME),
					true,
					false,
					false),

	    new MBeanAttributeInfo(ServiceAttributes.PORT,
					Integer.class.getName(),
					mbr.getString(mbr.I_SVC_CFG_ATTR_PORT),
					true,
					true,
					false),

	    new MBeanAttributeInfo(ServiceAttributes.THREAD_POOL_MODEL,
					String.class.getName(),
					mbr.getString(mbr.I_SVC_ATTR_THREAD_POOL_MODEL),
					true,
					false,
					false)
			};

    private static MBeanOperationInfo[] ops = {
	    new MBeanOperationInfo(ServiceOperations.PAUSE,
		mbr.getString(mbr.I_SVC_OP_PAUSE),
		    null, 
		    Void.TYPE.getName(),
		    MBeanOperationInfo.ACTION),

	    new MBeanOperationInfo(ServiceOperations.RESUME,
		mbr.getString(mbr.I_SVC_OP_RESUME),
		    null, 
		    Void.TYPE.getName(),
		    MBeanOperationInfo.ACTION)
		};

    private static String[] attrChangeTypes = {
		    AttributeChangeNotification.ATTRIBUTE_CHANGE
		};

    private static MBeanNotificationInfo[] notifs = {
	    new MBeanNotificationInfo(
		    attrChangeTypes,
		    AttributeChangeNotification.class.getName(),
		    mbr.getString(mbr.I_ATTR_CHANGE_NOTIFICATION)
		    )
		};

    public ServiceConfig(String service)  {
	super();
	this.service = service;

	initProps();

	com.sun.messaging.jmq.jmsserver.config.BrokerConfig cfg = Globals.getConfig();
	cfg.addListener(getThreadModelPropName(), this);
	cfg.addListener(getMaxThreadsPropName(), this);
	cfg.addListener(getMinThreadsPropName(), this);
	cfg.addListener(getPortPropName(), this);
    }

    public void setMaxThreads(Integer i) throws MBeanException  {
	try  {
            updateService(-1, -1, i.intValue());
	}  catch(Exception e)  {
	    handleSetterException(ServiceAttributes.MAX_THREADS, e);
	}
    }
    public Integer getMaxThreads() throws MBeanException  {
        initProps();

	String s = brokerProps.getProperty(getMaxThreadsPropName());
	Integer i = null;

	try  {
	    if (s != null)  {
	        i = new Integer(s);
	    }
	} catch (Exception e)  {
	    handleGetterException(ServiceAttributes.MAX_THREADS, e);
	}

	return (i);
    }
    private String getMaxThreadsPropName()  {
	return (Globals.IMQ + "." + getName() + ".max_threads");
    }

    public void setMinThreads(Integer i) throws MBeanException  {
	try  {
            updateService(-1, i.intValue(), -1);
	}  catch(Exception e)  {
	    handleSetterException(ServiceAttributes.MIN_THREADS, e);
	}
    }
    public Integer getMinThreads() throws MBeanException {
        initProps();

	String s = brokerProps.getProperty(getMinThreadsPropName());
	Integer i = null;

	try  {
	    if (s != null)  {
	        i = new Integer(s);
	    }
	} catch (Exception e)  {
	    handleGetterException(ServiceAttributes.MIN_THREADS, e);
	}

	return (i);
    }
    private String getMinThreadsPropName()  {
	return (Globals.IMQ + "." + getName() + ".min_threads");
    }

    public String getName()  {
	return (service);
    }

    public void setPort(Integer i) throws MBeanException  {
	try  {
            updateService(i.intValue(), -1, -1);
	}  catch(Exception e)  {
	    handleSetterException(ServiceAttributes.PORT, e);
	}
    }
    public Integer getPort() throws MBeanException  {
        initProps();

	String s = brokerProps.getProperty(getPortPropName());
	Integer i = null;

	try  {
	    if (s != null)  {
	        i = new Integer(s);
	    }
	} catch (Exception e)  {
	    handleGetterException(ServiceAttributes.PORT, e);
	}

	return (i);

    }
    private String getPortPropName()  {
	String proto = brokerProps.getProperty(Globals.IMQ + "."
			+ getName()
			+ ".protocoltype");
	return (Globals.IMQ + "." + getName() + "." + proto + ".port");
    }

    public String getThreadPoolModel()  {
	return (brokerProps.getProperty(getThreadModelPropName()));
    }
    private String getThreadModelPropName()  {
	return (Globals.IMQ + "." + getName() + ".threadpool_model");
    }

    public void pause() throws MBeanException  {
	try  {
	    if (isAdminService())  {
		throw (new BrokerException("Cannot pause admin service: " + service));
	    }

	    logger.log(Logger.INFO, rb.I_PAUSING_SVC, service);
	    ServiceUtil.pauseService(service);
	} catch(BrokerException e)  {
	    handleOperationException(ServiceOperations.PAUSE, e);
	}

    }

    public void resume() throws MBeanException  {
	try  {
	    if (isAdminService())  {
		throw (new BrokerException("Cannot resume admin service: " + service));
	    }

	    logger.log(Logger.INFO, rb.I_RESUMING_SVC, service);
	    ServiceUtil.resumeService(service);
	} catch(BrokerException e)  {
	    handleOperationException(ServiceOperations.RESUME, e);
	}
    }

    private boolean isAdminService()  {
	ServiceInfo si = ServiceUtil.getServiceInfo(service);

	if (si == null)  {
	    return (false);
	}

	if (si.type == ServiceType.ADMIN)  {
	    return (true);
	}

	return (false);
    }

    public String getMBeanName()  {
	return ("ServiceConfig");
    }

    public String getMBeanDescription()  {
	return (mbr.getString(mbr.I_SVC_CFG_DESC));
    }

    public MBeanAttributeInfo[] getMBeanAttributeInfo()  {
	return (attrs);
    }

    public MBeanOperationInfo[] getMBeanOperationInfo()  {
	return (ops);
    }

    public MBeanNotificationInfo[] getMBeanNotificationInfo()  {
	return (notifs);
    }

    public void validate(String name, String value)
            throws PropertyUpdateException {
    }
            
    public boolean update(String name, String value) {
	Object newVal = null;
        Object oldVal = null;

	/*
        System.err.println("### update called: "
            + name
            + "="
            + value);
	*/

	if (name.equals(getMaxThreadsPropName()))  {
	    try  {
	        newVal = Integer.valueOf(value);
	    } catch (NumberFormatException nfe)  {
	        logger.log(Logger.ERROR,
		    getMBeanName()
		    + ": cannot parse internal value of "
		    + ServiceAttributes.MAX_THREADS
		    + ": " 
		    + nfe);
                newVal = null;
	    }

	    try  {
	        oldVal = getMaxThreads();
	    } catch(Exception e)  {
                logProblemGettingOldVal(ServiceAttributes.MAX_THREADS, e);
	        oldVal = null;
	    }
            notifyAttrChange(ServiceAttributes.MAX_THREADS, 
			newVal, oldVal);
	    propsStale = true;
	} else if (name.equals(getMinThreadsPropName()))  {
	    try  {
	        newVal = Integer.valueOf(value);
	    } catch (NumberFormatException nfe)  {
	        logger.log(Logger.ERROR,
		    getMBeanName()
		    + ": cannot parse internal value of "
		    + ServiceAttributes.MIN_THREADS
		    + ": " 
		    + nfe);
                newVal = null;
	    }

	    try  {
	        oldVal = getMinThreads();
	    } catch(Exception e)  {
                logProblemGettingOldVal(ServiceAttributes.MIN_THREADS, e);
	        oldVal = null;
	    }

            notifyAttrChange(ServiceAttributes.MIN_THREADS, 
			newVal, oldVal);
	    propsStale = true;
	} else if (name.equals(getPortPropName()))  {
	    try  {
	        newVal = Integer.valueOf(value);
	    } catch (NumberFormatException nfe)  {
	        logger.log(Logger.ERROR,
		    getMBeanName()
		    + ": cannot parse internal value of "
		    + ServiceAttributes.PORT
		    + ": " 
		    + nfe);
                newVal = null;
	    }

	    try  {
	        oldVal = getPort();
	    } catch(Exception e)  {
                logProblemGettingOldVal(ServiceAttributes.PORT, e);
	        oldVal = null;
	    }

            notifyAttrChange(ServiceAttributes.PORT, 
			newVal, oldVal);
	    propsStale = true;
	}

        return true;
    }

    public void notifyAttrChange(String attrName, Object newVal, Object oldVal)  {
	sendNotification(
	    new AttributeChangeNotification(this, sequenceNumber++, new Date().getTime(),
	        "Attribute change", attrName, 
                 newVal == null ? "" : newVal.getClass().getName(),
	        oldVal, newVal));
	
	Agent agent = Globals.getAgent();

	if (agent != null)  {
	    agent.notifyServiceAttrUpdated(getName(), attrName, oldVal, newVal);
	}
    }

    private void updateService(int port, int min, int max) 
			throws IOException, PropertyUpdateException, 
			BrokerException  {
	ServiceManager sm = Globals.getServiceManager();
	Service svc = sm.getService(getName());
	IMQService stsvc;

	if (svc == null)  {
	    throw new BrokerException(rb.getString(rb.X_NO_SUCH_SERVICE, getName()));
	}

	if (!(svc instanceof IMQService))  {
	    throw new BrokerException("Internal Error: can updated non-standard Service");
	}

	stsvc = (IMQService)svc;

	stsvc.updateService(port, min, max);
    }

    private void initProps() {
	if (!propsStale)  {
	    return;
	}

	brokerProps = Globals.getConfig().toProperties();
	Version version = Globals.getVersion();
	brokerProps.putAll(version.getProps());

	propsStale = false;
    }
}