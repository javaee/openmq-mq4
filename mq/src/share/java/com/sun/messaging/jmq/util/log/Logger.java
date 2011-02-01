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
 * @(#)Logger.java	1.37 06/29/07
 */ 

package com.sun.messaging.jmq.util.log;

import java.util.Date;
import java.util.Properties;
import java.util.MissingResourceException;
import java.util.Vector;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.File;
import java.text.SimpleDateFormat;
import java.text.MessageFormat;

import com.sun.messaging.jmq.resources.SharedResources;
import com.sun.messaging.jmq.util.MQResourceBundle;

/**
 * Logger is an interface to a set of LogHandlers (logging devices).
 * LogHandlers are added using setLogHandlers(). Once the log handlers
 * are set you log messages to them using the Logger's various
 * log() and logStack() methods. You may specify a resource bundle for
 * Logger to use via the setResourceBundle() method. If a resource
 * bundle is set Logger will attempt to lookup up messages in it using
 * the string passed to the log() method as a key. If the lookup fails
 * then the string is assumed to be the text to log, and not a key.
 *
 * Logger defines six log levels in the following descending order:
 * ERROR, WARNING, INFO, DEBUG, DEBUGMED, DEBUGHIGH. By default
 * Logger will only display INFO messages and above. Logger's 'level'
 * field is public so that code can efficiently compare it
 * if they want to avoid the overhead of a method call. Note that
 * in addition to Logger's log level, each LogHandler may selectively
 * choose which messages to log by specifying a bit-mask for the
 * log levels they want messages for.
 *
 * When a Logger is in a closed() state, any messages logged to it
 * are deferred in a buffer. When the logger is opened the deferred
 * messages are logged.
 */
public class Logger {

    // Resource bundle to look up logged messages
    private MQResourceBundle rb = null;


    // List of logging handlers to send messages to
    private LogHandler[] handlers = null;

    // Buffer to hold messages that are logged when Logger is closed
    private Vector deferBuffer = null;
    private boolean closed = true;

    // Full path to "home" directory for Logger. Any relative paths
    // used by logger will be relative to this directory.
    String logHome = null;

    String propPrefix = "";

    // Current logging level
    public int level = INFO;

    /**
     * Log levels. We use bit values so that LogHandlers can selectively
     * choose which level of messages to log.
     */
    public static final int FORCE         = 0x0040; // Should always be greatest
    public static final int ERROR         = 0x0020;
    public static final int WARNING       = 0x0010;
    public static final int INFO          = 0x0008;
    public static final int DEBUG         = 0x0004;
    public static final int DEBUGMED      = 0x0002;
    public static final int DEBUGHIGH     = 0x0001;

    public static final int OFF           = Integer.MAX_VALUE;

    /**
     * A hack. These are the properties that may be dynamically
     * updated. Logger really shouldn't know about handler
     * specific properties.
     */
    private static final String LOGLEVEL_PROP      = "log.level";
    private static final String TIMEZONE_PROP      = "log.timezone";
    private static final String ROLLOVERSECS_PROP  = "log.file.rolloversecs";
    private static final String ROLLOVERBYTES_PROP = "log.file.rolloverbytes";

    // Resource bundle for the Logging code to use to display it's error
    // messages.
    private static SharedResources myrb = SharedResources.getResources();

    // Format of date string prepended to each message
    private SimpleDateFormat df =
	new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z");

    /**
     * Constructor for Logger.
     *
     * @param logHome	Home directory for this Logger. Any relative paths
     * 		  	used by this Logger will be relative to logHome.
     */
    public Logger(String logHome) {
	this.logHome = logHome;
	this.handlers = new LogHandler[1];
    }

    public void useMilliseconds() {
        df = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss:SS z");
    }

    public int getLevel() {
        return level;
    }

    /**
     * Get the name of the properties that can be dynamically updated.
     * These are prefixed with whatever prefix string was passed to configure()
     */
    public String[] getUpdateableProperties() {

        String[] v = new String[4];

        v[0] = propPrefix + "." + LOGLEVEL_PROP;
        v[1] = propPrefix + "." + ROLLOVERSECS_PROP;
        v[2] = propPrefix + "." + ROLLOVERBYTES_PROP;
        v[3] = propPrefix + "." + TIMEZONE_PROP;

        return v;
    }

    /**
     * Dynamically update a property. 
     */
    public synchronized void updateProperty(String name, String value)
        throws IllegalArgumentException {

        if (name == null || value == null || value.equals("")) {
            return;
        }

        // Skip over <prefix>.
        name = name.substring(propPrefix.length() + 1);

        long n = -1;

        if (name.equals(LOGLEVEL_PROP)) {
	    this.level = levelStrToInt(value);
        } else if (name.equals(TIMEZONE_PROP)) {
            if (value.length() > 0) {
                df.setTimeZone(TimeZone.getTimeZone(value));
            }
        } else if (name.equals(ROLLOVERSECS_PROP) ||
                   name.equals(ROLLOVERBYTES_PROP)) {
            // Hack! Logger should not know about these properties
            try {
                n = Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    myrb.getString(myrb.W_BAD_NFORMAT, name, value));
            }
            FileLogHandler flh = (FileLogHandler)findHandler("file");
            if (flh == null) {
                return;
            }

	    /*
             * As of MQ 3.5, the value that is used to mean 'unlimited'
             * is -1 (although 0 is still supported). To support -1 as
             * an additional way of specifying unlimited, this method will
             * detect if -1 was set for rolloverbytes or rolloversecs
             * (these are the properties that are relevant here), and
             * pass on the value '0' to the method setRolloverLimits().
             *
	     */
	    if (n == -1)  {
	        n = 0L;
	    }

            if (name.equals(ROLLOVERSECS_PROP)) {
                flh.setRolloverLimits(-1, n);
            } else {
                flh.setRolloverLimits(n, -1);
            }
        } else {
            throw new IllegalArgumentException(
                myrb.getString(myrb.X_BAD_PROPERTY, name));
        }
    }

    /**
     * Find the handler with the specified name
     */
    private synchronized LogHandler findHandler(String name) {

        if (handlers == null) {
            return null;
        }

        // Return the handler that matches the passed name
	for (int n = 0; n < handlers.length; n++) {
	    if (handlers[n] != null && name.equals(handlers[n].getName())) {
                return handlers[n];
            }
	}
        return null;
    }

    /**
     * Configure the logger based on the configuration in the 
     * passed properties. The properties and their syntax is TBD
     */
    public synchronized void configure(Properties props, String prefix) {

        propPrefix = prefix;

	if (props == null) {
	    return;
        }

	close();

	String property, value;

	prefix = prefix + ".log.";

	// Get the log level
	property = prefix + "level";
	value = props.getProperty(property);
	if (value != null && !value.equals("")) {
	    try {
		this.level = levelStrToInt(value);
	    } catch (IllegalArgumentException e) {
		this.log(WARNING,
		    myrb.getKString(myrb.W_BAD_LOGLEVELSTR, property, value));
	    }
	}

	property = prefix + "timezone";
	value = props.getProperty(property);
        if (value != null && !value.equals("")) {
            df.setTimeZone(TimeZone.getTimeZone(value));
        }

	// Get list of LogHandlers from the properties
	property = prefix + "handlers";
	value = props.getProperty(property);
	if (value == null || value.equals("")) {
	    this.log(ERROR, myrb.getKString(myrb.E_NO_LOGHANDLERLIST,
		     property));
	    return;
	}

	String handlerName;
	String handlerClass;

	StringTokenizer token = new StringTokenizer(value, ",", false);
	int nHandlers = token.countTokens();
	LogHandler[] handlers = new LogHandler[nHandlers];

	// Loop through each LogHandler and instantiate the class
	for (int n = 0; n < nHandlers; n++) {
	    handlerName  = token.nextToken();
	    property = prefix +  handlerName + ".class";
	    handlerClass = props.getProperty(property);
            if (handlerClass == null || handlerClass.equals("")) {
		this.log(ERROR, myrb.getKString(myrb.E_NO_LOGHANDLER,
			 property));
		continue;
	    }

	    Class t;
	    try {
		t = Class.forName(handlerClass);
	        handlers[n] = (LogHandler)t.newInstance();
	    } catch (ClassNotFoundException e) {
		this.log(ERROR, myrb.getKString(myrb.E_BAD_LOGHANDLERCLASS,
					    e.toString()));
		continue;
            } catch (IllegalAccessException e) {
		this.log(ERROR, myrb.getKString(myrb.E_BAD_LOGHANDLERCLASS,
					    e.toString()));
		continue;
            } catch (InstantiationException e) {
		this.log(ERROR, myrb.getKString(myrb.E_BAD_LOGHANDLERCLASS,
					    e.toString()));
		continue;
	    }
	    handlers[n].init(this);
	    handlers[n].setName(handlerName);

	    try {
	        // Configure LogHandler class based on properties
	        handlers[n].configure(props, prefix + handlerName);
	    } catch (IllegalArgumentException e) {
		this.log(WARNING, myrb.getKString(myrb.W_BAD_LOGCONFIG,
					e.toString()));
	    } catch (UnsatisfiedLinkError e) {
		this.log(WARNING, myrb.getKString(myrb.W_LOGCHANNEL_DISABLED,
                                          handlerName, e.getMessage()));
                handlers[n] = null;
            
        
	    } catch (java.lang.NoClassDefFoundError err) {
	    	this.log(WARNING, myrb.getKString(myrb.W_LOGCHANNEL_DISABLED,
                    handlerName, err.getMessage()));
	    	handlers[n] = null;
	    }
	}

	try {
            setLogHandlers(handlers);
        } catch (IOException e) {
	    // Not I18N because this is a programming error
            System.err.println("Could not set handlers: " + e);
        }
    }


    /**
     * Convert a string representation of a log level to its integer value.
     *
     * @param levelStr	Log level string. One of: ERROR, WARNING, INFO, DEBUG
     *                  DEBUGMED, DEBUGHIGH
     */
    public static int levelStrToInt(String levelStr)
	throws IllegalArgumentException {

	if (levelStr.equals("FORCE")) {
	    return Logger.FORCE;
	} else if (levelStr.equals("ERROR")) {
	    return Logger.ERROR;
	} else if (levelStr.equals("WARNING")) {
	    return Logger.WARNING;
	} else if (levelStr.equals("INFO")) {
	    return Logger.INFO;
	} else if (levelStr.equals("DEBUG")) {
	    return Logger.DEBUG;
	} else if (levelStr.equals("DEBUGMED")) {
	    return Logger.DEBUGMED;
	} else if (levelStr.equals("DEBUGHIGH")) {
	    return Logger.DEBUGHIGH;
	} else if (levelStr.equals("NONE")) {
	    return Logger.OFF;
	} else {
	    throw new IllegalArgumentException(
                myrb.getString(myrb.W_BAD_LOGLEVELSTR, levelStr));
	}
    }

    /**
     * Convert an int representation of a log level to its string value.
     *
     * @param level	Log level int. One of: ERROR, WARNING, INFO, DEBUG
     *                  DEBUGMED, DEBUGHIGH
     */
    public static String levelIntToStr(int level)
	throws IllegalArgumentException {

        switch (level) {
        case FORCE:
            return "FORCE";
        case ERROR:
            return "ERROR";
        case WARNING:
            return "WARNING";
        case INFO:
            return "INFO";
        case DEBUG:
            return "DEBUG";
        case DEBUGMED:
            return "DEBUGMED";
        case DEBUGHIGH:
            return "DEBUGHIGH";
        default:
	    throw new IllegalArgumentException();
	}
    }


    /**
     * Set the resource bundle used to lookup strings logged via
     * the log() and logStack() methods
     *
     * @param rb	The resource bundle used to lookup logged strings.
     */
    public void setResourceBundle(MQResourceBundle rb) {
	this.rb = rb;
    }

    /**
     * Specify the set of LogHandlers to use. The Logger must be in 
     * the closed state to perform this operation. If it isn't this
     * method throws an IOException.
     *
     * @param newhandlers	Array of LogHandlers to use
     *
     * @throws IOException
     */
    private synchronized void setLogHandlers(LogHandler[] newhandlers)
	   throws IOException {

	if (!closed) {
	    // This is a programming error and is therefore not I18N'd
	    throw new IOException(
		"Logger must be closed before setting handlers");
        }

	handlers = newhandlers;
    }

    /**
     * Close all LogHandlers
     */
    public synchronized void close() {
	// Close handlers
	if (handlers != null) {
	    for (int n = 0; n < handlers.length; n++) {
		if (handlers[n] != null) {
		    handlers[n].close();
		}
            }
	}

	closed = true;
    }

    /**
     * Open all LogHandlers. If a handler fails to open and message
     * is written to System.err and the handler is removed from the 
     * list of LogHandlers.
     *
     * If there are any messages in the defer buffer they are flushed
     * out after the handlers are opened.
     */
    public synchronized void open() {

	// Open handlers
	for (int n = 0; n < handlers.length; n++) {
	    if (handlers[n] != null) {
		try {
	            handlers[n].open();
		} catch (IOException e) {
		    this.log(ERROR, myrb.getKString(myrb.E_BAD_LOGDEVICE,
				       handlers[n].toString(),
				       e));
		    handlers[n] = null;
		}
	    }
        }

	closed = false;

	// Flush any defered messages to handlers
	flushDeferBuffer();
    }

    /**
     * Flush all LogHandlers.
     */
    public void flush() {
	// Flush handlers
	if (handlers != null) {
	    for (int n = 0; n < handlers.length; n++) {
		if (handlers[n] != null) {
		    handlers[n].flush();
		}
            }
	}
    }

    /**
     * Publish a formatted message to all LogHandlers. If the Logger is
     * closed, then the message is held in a buffer until the Logger is
     * opened.
     *
     * @param level	Log level. Message will be logged to a LogHandler
     *			if the handler accepts this level of message. If
     *                  level is FORCE then message will be published
     *                  to all LogHandlers unless a handlers has its log
     *                  level set to NONE.
     *
     * @param message	The string to log. It is assumed that this 
     *                  string is already formatted
     *
     */
    public void publish(int level, String message) {

	// If the Logger is closed we defer the message into a buffer
	// until the Logger is opened again
	if (closed) {
	    defer(level, message);
	    return;
        }

	boolean loggedOnce = false;
        LogHandler handler = null;

	// The message has already been formatted. We just need to
	// send it to each LogHandler
	for (int n = 0; n < handlers.length; n++) {
            try {
                handler = handlers[n];
            } catch (Exception e) {
                // If list was changed out from under us (unlikely)
                // just skip handler
                handler = null;
            }
	    // Check if handler wants this level of message
	    if (handler != null &&
                handler.levels != 0 &&
                ((level == FORCE && handler.isAllowForceMessage()) || (level != FORCE && (level & handler.levels) != 0))) {
		try {
		    handler.publish(level, message);
		    loggedOnce = true;
		} catch (IOException e) {
		    System.err.println(myrb.getKString(myrb.E_LOGMESSAGE,
				handler.toString(), e));
		}
            }
	}

	// Don't let message go into black hole.
	if (!loggedOnce) {
	    System.err.println(message);
	}
    }

    /**
     * Defers a message into a buffer until the buffer is flushed
     * to the handlers. When a Logger is closed, any messages logged
     * are defered until the buffer is opened
     */
    private synchronized void defer(int level, String message) {

	if (deferBuffer == null) {
	    deferBuffer = new Vector(32);
        }

	// Add message to buffer. We must preserve the level too.
	LogRecord dr = new LogRecord(level, message);
	deferBuffer.addElement((Object)dr);
    }

    /**
     * Flushes deferred messages out of buffer and releases the buffer
     */
    private synchronized void flushDeferBuffer() {

	if (deferBuffer == null) {
            return;
        }

	// Publish all messages in buffer
	LogRecord df = null;
        for (Enumeration e = deferBuffer.elements(); e.hasMoreElements(); ) {
	    df = (LogRecord)e.nextElement();
	    publish(df.level, df.message);
        }

	// Clear buffer and discard it
	deferBuffer.clear();
	deferBuffer = null;
    }

    /**
     * Format a log message.
     *
     * @param level	Log level. Used to generate a possible prefix
     *			like "ERROR" or "WARNING"
     *
     * @param key	Key to use to find message in resource bundle.
     *                  If message is not localized then the key is just
     *                  the raw message to display.
     *
     * @param args	Optional args to message. Null if no args
     *
     * @param ex	Optional throwable to display description of. Null
     *			if no throwable.
     *
     * @param printStack True to print stack trace of throwable. False to
     *			 not print a stack trace. If True "ex" must not be
     *			 null.
     *
     */
    public String format(
	int level,
	String key,
	Object[] args,
	Throwable ex,
	boolean printStack) {

	StringBuffer sb = new StringBuffer(80);

	// Append date and time
	sb.append("[");
	sb.append(df.format(new Date()));
	sb.append("] ");

	// Append prefix if any
	switch (level) {
	case ERROR:   sb.append(myrb.getString(myrb.M_ERROR)); break;
	case WARNING: sb.append(myrb.getString(myrb.M_WARNING)); break;
	default: break;
	}

	// Get message from resource bundle.
	String message = null;

	if (key == null) key = "";

	if (rb != null) {
	    try {
	        if (args == null) {
		    message = rb.getKString(key);
	        } else {
		    message = rb.getKString(key, args);
	        }
            } catch (MissingResourceException e) {
	        // Message is not localized.
	        message = null;
            }
        }

	// If message was not localized use key as message
	if (message == null) {
	    if (args == null) {
	        message = key;
	    } else {
		message = MessageFormat.format(key, args);
	    }
        }

	sb.append(message);

	// If there is a throwable, append its description
	if (ex != null) {

	    sb.append(":" + myrb.NL);

	    // If requested, append full stack trace
	    if (printStack) {
		// No way to get trace it as a String so
		// we use a ByteArrayOutputStream to capture it.
		ByteArrayOutputStream bos = new ByteArrayOutputStream(512);
		PrintWriter pw = new PrintWriter(bos);
		ex.printStackTrace(pw);
		pw.flush();
		pw.close();
		sb.append(bos.toString());
		try {
		    bos.close();
		} catch (IOException e) {
		}
	    } else {
		// Just append description
	        sb.append(ex + myrb.NL);
	    }
        } else {
	    sb.append(myrb.NL);
        }

	return sb.toString();
    }

    /**
     ************************************************************************
     * The following are variations of the base log() and logStack() methods.
     * log() logs a message, logStack() includes the stack backtrace of a
     * passed Throwable. Since these routines may perform the resource bundle
     * lookup, we provide variations to pass arguments for the message
     * string. That gives use 12 variations: 4 each of log,
     * log with Throwable, and logStack with Throwable.
     ***********************************************************************/

    /**
     * Log a message
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     */
    public void log(int level, String msg) {
	if (level < this.level) {
            return;
        }
	publish(level, format(level, msg, null, null, false));
    }

    /**
     * Log a message with one argument
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param arg	The argument to substitute into msg.
     */
    public void log(int level, String msg, Object arg) {
	if (level < this.level) {
            return;
        }

	// the following check is needed because when the
	// third argument is of type Object[], this method
	// is called instead of log(level, String, Object[])
	if (arg instanceof Object[]) {
	    publish(level, format(level, msg, (Object[])arg, null, false));
	} else {
	    Object[] args = {arg};
	    publish(level, format(level, msg, args, null, false));
        }
    }

    /**
     * Log a message with two arguments
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param arg1	The first argument to substitute into msg.
     *
     * @param arg2	The second argument to substitute into msg.
     */
    public void log(int level, String msg, Object arg1, Object arg2) {
	if (level < this.level) {
            return;
        }
	Object[] args = {arg1, arg2};
	publish(level, format(level, msg, args, null, false));
    }

    /**
     * Log a message with an array of arguments
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param args	Array of arguments to substitute into msg.
     */
    public void log(int level, String msg, Object[] args) {
	if (level < this.level) {
            return;
        }
	publish(level, format(level, msg, args, null, false));
    }

    /**
     * Log a message and a throwable
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param ex	Throwable to log description of.
     */
    public void log(int level, String msg, Throwable ex) {
	if (level < this.level) {
            return;
        }

        // If we are at a DEBUG level, log throwable stack
        boolean logStack = (this.level <= DEBUG);
        publish(level, format(level, msg, null, ex, logStack));
    }

    /**
     * Log a message with one argument and a throwable
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param arg	The argument to substitute into msg.
     *
     * @param ex	Throwable to log description of.
     */
    public void log(int level, String msg, Object arg, Throwable ex) {
	if (level < this.level) {
            return;
        }

        // If we are at a DEBUG level, log throwable stack
        boolean logStack = (this.level <= DEBUG);

	// the following check is needed because when the
	// third argument is of type Object[], this method
	// is called instead of log(level, String, Object[])
	if (arg instanceof Object[]) {
	    publish(level, format(level, msg, (Object[])arg, ex, logStack));
	} else {
	    Object[] args = {arg};
	    publish(level, format(level, msg, args, ex, logStack));
        }
    }

    /**
     * Log a message with two arguments and a throwable
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param arg1	The first argument to substitute into msg.
     *
     * @param arg2	The second argument to substitute into msg.
     *
     * @param ex	Throwable to log description of.
     */
    public void log(int level, String msg, Object arg1, Object arg2, 
		    Throwable ex) {
	if (level < this.level) {
            return;
        }
        // If we are at a DEBUG level, log throwable stack
        boolean logStack = (this.level <= DEBUG);
	Object[] args = {arg1, arg2};
	publish(level, format(level, msg, args, ex, logStack));
    }

    /**
     * Log a message with an array of arguments and a throwable
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param args	Array of arguments to substitute into msg.
     */
    public void log(int level, String msg, Object[] args, Throwable ex) {
	if (level < this.level) {
            return;
        }
        // If we are at a DEBUG level, log throwable stack
        boolean logStack = (this.level <= DEBUG);
	publish(level, format(level, msg, args, ex, logStack));
    }

    /**
     * Log a message and a throwable. Include stack backtrace.
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param ex	Throwable to log description of.
     */
    public void logStack(int level, String msg, Throwable ex) {
	if (level < this.level) {
            return;
        }
	publish(level, format(level, msg, null, ex, true));
    }

    /**
     * Log a message with one argument and a throwable.
     * Include stack backtrace.
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param arg	The argument to substitute into msg.
     *
     * @param ex	Throwable to log description of.
     */
    public void logStack(int level, String msg, Object arg, Throwable ex) {
	if (level < this.level) {
            return;
        }

	// the following check is needed because when the
	// third argument is of type Object[], this method
	// is called instead of logStack(level, String, Object[])
	if (arg instanceof Object[]) {
	    publish(level, format(level, msg, (Object[])arg, ex, true));
	} else {
	    Object[] args = {arg};
	    publish(level, format(level, msg, args, ex, true));
        }
    }

    /**
     * Log a message with two arguments and a throwable
     * Include stack backtrace.
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param arg1	The first argument to substitute into msg.
     *
     * @param arg2	The second argument to substitute into msg.
     *
     * @param ex	Throwable to log description of.
     */
    public void logStack(int level, String msg, Object arg1, Object arg2, 
		    Throwable ex) {
	if (level < this.level) {
            return;
        }
	Object[] args = {arg1, arg2};
	publish(level, format(level, msg, args, ex, true));
    }

    /**
     * Log a message with an array of arguments and a throwable
     * Include stack backtrace.
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param args	Array of arguments to substitute into msg.
     */
    public void logStack(int level, String msg, Object[] args, Throwable ex) {
	if (level < this.level) {
            return;
        }
	publish(level, format(level, msg, args, ex, true));
    }


    /**
     * Log a message with arguments and force it to appear to all handlers
     * that do not have their log level set to NONE.
     *
     * @param level	Log level. Used to format the message (ie put
     *                  "ERROR" in front of errors). Message will go to
     *                  all log handlers not matter what this level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param args	Array of arguments to substitute into msg.
     */
    public void logToAll(int level, String msg, Object[] args) {
	publish(FORCE, format(level, msg, args, null, false));
    }

    /**
     * Log a message with arguments and force it to appear to all handlers
     * that do not have their log level set to NONE.
     *
     * @param level	Log level. Used to format the message (ie put
     *                  "ERROR" in front of errors). Message will go to
     *                  all log handlers not matter what this level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     */
    public void logToAll(int level, String msg) {
	publish(FORCE, format(level, msg, null, null, false));
    }

    /**
     * Log a message with arguments and force it to appear to all handlers
     * that do not have their log level set to NONE.
     *
     * @param level	Log level. Used to format the message (ie put
     *                  "ERROR" in front of errors). Message will go to
     *                  all log handlers not matter what this level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param arg1	The first argument to substitute into msg.
     *
     * @param arg2	The second argument to substitute into msg.
     */
    public void logToAll(int level, String msg, Object arg1, Object arg2) {
	Object[] args = {arg1, arg2};
	publish(FORCE, format(level, msg, args, null, false));
    }

    /**
     * Log a message with one argument and force it to appear to all handlers
     * that do not have their log level set to NONE.
     *
     * @param level	Log level. Message will be logged if this level
     *			is greater or equal to the current log level.
     *
     * @param msg	The message to log. This will first be tried
     *			as a key to look up the localized string in the
     *			current resource bundle. If that fails then the
     *			msg String will be used directly.
     *
     * @param arg	The argument to substitute into msg.
     */
    public void logToAll(int level, String msg, Object arg) {
	// the following check is needed because when the
	// third argument is of type Object[], this method
	// is called instead of log(level, String, Object[])
	if (arg instanceof Object[]) {
	    publish(FORCE, format(level, msg, (Object[])arg, null, false));
	} else {
	    Object[] args = {arg};
	    publish(FORCE, format(level, msg, args, null, false));
        }
    }

}

class LogRecord {
    public int level;
    public String message;

    public LogRecord(int level, String message) {
	this.level = level;
	this.message = message;
    }
}

