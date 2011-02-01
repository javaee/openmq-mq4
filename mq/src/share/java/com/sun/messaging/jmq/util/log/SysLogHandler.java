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
 * @(#)SysLogHandler.java	1.5 06/29/07
 */ 

package com.sun.messaging.jmq.util.log;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import com.sun.messaging.jmq.resources.SharedResources;
import com.sun.messaging.jmq.util.StringUtil;

/**
 * A LogHandler that logs to Unix syslog
 */
public class SysLogHandler extends LogHandler {

    /*
     * Apply to all instances of a SysLogHandler. This is due to
     * the way syslog works. It is designed for a process to have
     * one connection to syslogd.
     */
    static String ident = "SysLogHandler";
    static int     facility = SysLog.LOG_DAEMON;
    static int     logopt = (SysLog.LOG_PID | SysLog.LOG_CONS);

    static boolean open = false;

    public SysLogHandler() {
    	
    	// this handler always ignores FORCE messages
    	setAllowForceMessage(false);
    }

    public static void setFacility(int f) {
        facility = f;
    }

    /**
     * Configure SysLogHandler with the values contained in
     * the passed Properties object. Note that all SysLogHandlers
     * will be affected, not just this instance. The handler's properties are
     * prefixed with the specified prefix.
     * <P>
     * An example of valid properties are:
     * <PRE>
     * imq.log.syslog.facility=LOG_DAEMON
     * imq.log.syslog.logpid=true
     * imq.log.syslog.logconsole=false
     * imq.log.syslog.identity=imqbrokerd_imqbroker
     * </PRE>
     * In this case prefix would be "jmq.log.file"
     *
     * @param props	Properties to get configuration information from
     * @param prefix	String that this handler's properties are prefixed with
     *
     * @throws IllegalArgumentException if one or more property values are
     *                                  invalid. All valid properties will
     *					still be set.
     * @throws UnsatisfiedLinkError     if native code can't be loaded
     */
    public void configure(Properties props, String prefix)
	throws IllegalArgumentException, UnsatisfiedLinkError {

	String value = null;
	String property = null;
	String error_msg = null;

	prefix = prefix + ".";

        synchronized (getClass()) {

	property = prefix + "facility";
	if ((value = props.getProperty(property)) != null) {
	    try {
		//facility = parseFacility(value);
		setFacility(SysLog.LOG_DAEMON);
            } catch (Exception e) {
	        //error_msg = rb.getString(rb.W_BAD_NFORMAT, property, value);
	        error_msg = "Bad syslog facility " + property + "=" + value;
            }
	}

	property = prefix + "logpid";
	if ((value = props.getProperty(property)) != null) {
            if (value.equalsIgnoreCase("true")) {
                logopt = logopt | SysLog.LOG_PID;
            }
	}

	property = prefix + "logconsole";
	if ((value = props.getProperty(property)) != null) {
            if (value.equalsIgnoreCase("true")) {
                logopt = logopt | SysLog.LOG_CONS;
            }
	}

	property = prefix + "identity";
	if ((value = props.getProperty(property)) != null) {
            ident = StringUtil.expandVariables(value, props);
	}

	property = prefix + "output"; 
	if ((value = props.getProperty(property)) != null) {
	    try {
	        setLevels(value);
	    } catch (IllegalArgumentException e) {
	        error_msg = (error_msg != null ? error_msg + "\n" : "") +
			property + ": " + e.getMessage();
	    }
        } 

        if (open) {
            this.close();
        }
        // Causes prop changes to take effect and forces link error
        this.open();

        }
    }

    /**
     * Publish string to log
     *
     * @param level	Log level to use
     * @param message	Message to write to log file
     *
     */
    public void publish(int level, String message) throws IOException {

        int priority = SysLog.LOG_INFO;
        
		// ignore FORCE messages if we have explicitly been asked to ignore them
        // (note that isAllowForceMessage() will always return false for this class)
		if (level == Logger.FORCE && !isAllowForceMessage()) {
			return;
		}

        /* Syslog adds its own timestamp, strip ours */
        if (message.indexOf("[") == 0) {
            message = message.substring(message.indexOf("]") + 1);
        }

        // Map level to a syslog level
        switch (level) {
            case Logger.ERROR:   priority = SysLog.LOG_ERR; break;
            case Logger.WARNING: priority = SysLog.LOG_WARNING; break;
            case Logger.INFO:    priority = SysLog.LOG_INFO; break;
            case Logger.DEBUG:   
            case Logger.DEBUGMED:
            case Logger.DEBUGHIGH: priority = SysLog.LOG_DEBUG; break;
        }

        SysLog.syslog(priority, message);
    }

    /**
     * Open handler
     */
    public void open() {
       
        synchronized(getClass()) {
            if (!open) {
                SysLog.openlog(ident, logopt, facility);
                open = true;
            }
        }
    }

    /**
     * Close handler
     */
    public void close() {
        synchronized(getClass()) {
            if (open) {
                SysLog.closelog();
                open = false;
            }
        }
    }

    /**
     * Return a string description of this FileHandler. The descirption
     * is the class name followed by the path of the file we are logging to.
     */
    public String toString() {
	return this.getClass().getName() + ":" + ident;
    }
}
