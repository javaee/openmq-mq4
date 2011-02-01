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

package com.sun.messaging.jmq.util.timer;

public class WakeupableTimer implements Runnable 
{
    private static boolean DEBUG = false;

    private String name = null;
    private long nexttime = 0;
    private long repeatinterval = 0;
    private Thread thr = null;
    private Runnable runner = null;
    private boolean valid = true;
    private boolean wakeup = false;
    private String startString = null;
    private String exitString = null;
    private TimerEventHandler handler = null;


    /**
     * @param delaytime in millisecs
     * @param repeatinterval in millisecs, if 0 run runner only once
     */
    public WakeupableTimer(String name, Runnable runner, 
                           long delaytime, long repeatInterval,
                           String startString, String exitString,
                           TimerEventHandler handler) {
        this.name = name;
        this.nexttime = delaytime + System.currentTimeMillis();
        this.repeatinterval = repeatInterval;
        this.runner = runner;
        this.startString = startString;
        this.exitString = exitString;
        this.handler = handler;

        thr = new Thread(this, name);
        thr.start();
    }

    public synchronized void wakeup() {
        wakeup = true;
        notify();
    }

    public void cancel() {
        valid = false;
        wakeup();
        thr.interrupt();
    }

    public void run() {
        try {

        handler.handleLogInfo(startString);

        long time = System.currentTimeMillis();
        while (valid) {
            try {

            synchronized(this) {
                while (!wakeup && time < nexttime) {
                    try {
                        this.wait(nexttime  - time);
                    } catch (InterruptedException ex) {}
                    time = System.currentTimeMillis();
                }
                if (!valid) break; 
                wakeup =  false;
            }

            if (DEBUG) {
                handler.handleLogInfo(name+" run "+runner.getClass().getName());
            }

            runner.run();

            if (DEBUG) {
                handler.handleLogInfo(name+" completed run "+runner.getClass().getName());
            }

            if (!wakeup && repeatinterval == 0) break;
            time = System.currentTimeMillis();
            nexttime = time + repeatinterval;

            } catch (Throwable e) {
            handler.handleLogWarn(e.getMessage(), e);
            if (e instanceof OutOfMemoryError) {
                handler.handleOOMError(e);
            }
            }
        } //while

        handler.handleLogInfo(exitString);

        } catch (Throwable t) {
        handler.handleLogError(exitString, t);
        handler.handleTimerExit(t);
        }
    }

}
