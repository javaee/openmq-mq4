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
 * @(#)Destination.java	1.47 06/28/07
 */ 

package com.sun.messaging.jmq.jmsserver.data;

import com.sun.messaging.jmq.jmsserver.service.ConnectionUID;
import com.sun.messaging.jmq.util.SizeString;
import com.sun.messaging.jmq.jmsserver.util.BrokerException;

import java.util.*;
import java.io.*;
import java.lang.*;

/**
 * This class represents a destination (topic or queue name)
 *
 * @deprecated since 3.5
 * @see com.sun.messaging.jmq.jmsserver.core.Destination
 *
 */
public class Destination implements java.io.Serializable
{
    static final long serialVersionUID = -5305744890489268213L;

    private String name = null;
    protected int type = -1;
    protected ConnectionUID id = null;
    private boolean autocreate = false;
    protected SizeString msgSizeLimit = null;
    protected int countLimit = 0;
    protected SizeString memoryLimit = null;

    protected String dest_string = null;
    protected String unique_id = null;

    public Object readResolve() throws ObjectStreamException {
        try {
            com.sun.messaging.jmq.jmsserver.core.Destination 
                 obj = com.sun.messaging.jmq.jmsserver.
                    core.Destination.createDestination(name, type,
                       false, autocreate, id);
            obj.setMaxByteSize(msgSizeLimit);
            if (countLimit > 0)
                obj.setCapacity(countLimit);
            if (memoryLimit != null && memoryLimit.getBytes() > 0)
                obj.setByteCapacity(memoryLimit);
            obj.initializeOldDestination();
            return obj;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }

    } 

}
