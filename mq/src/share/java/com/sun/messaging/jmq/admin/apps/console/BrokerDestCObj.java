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
 * @(#)BrokerDestCObj.java	1.15 06/27/07
 */ 

package com.sun.messaging.jmq.admin.apps.console;

import java.util.Vector;

import javax.swing.ImageIcon;
import javax.swing.JPopupMenu;
import javax.swing.JMenuItem;

import com.sun.messaging.jmq.util.admin.DestinationInfo;
import com.sun.messaging.jmq.util.DestState;

import com.sun.messaging.jmq.admin.bkrutil.BrokerAdmin;
import com.sun.messaging.jmq.admin.util.Globals;
import com.sun.messaging.jmq.admin.resources.AdminConsoleResources;

/** 
 * This class is used in the JMQ Administration console
 * to store information related to a particular broker
 * destination.
 *
 * @see ConsoleObj
 * @see BrokerAdminCObj
 *
 */
public class BrokerDestCObj extends BrokerAdminCObj  {

    private BrokerCObj bCObj;
    private DestinationInfo destInfo = null;
    private Vector durables = null;
    private static AdminConsoleResources acr = Globals.getAdminConsoleResources();

    /*
     * It is okay not to get the latest durable subscriptions in the constructor, 
     * since we always retrieve the latest durable subscriptions right before we 
     * display them (because of their dynamic nature.)
     */
    public BrokerDestCObj(BrokerCObj bCObj, DestinationInfo destInfo) {
        this.bCObj = bCObj;
        this.destInfo = destInfo;
    }

    public BrokerDestCObj(BrokerCObj bCObj, DestinationInfo destInfo, Vector durables) {
        this.bCObj = bCObj;
        this.destInfo = destInfo;
        this.durables = durables;
    }

    public BrokerAdmin getBrokerAdmin() {
	return (bCObj.getBrokerAdmin());
    }

    public BrokerCObj getBrokerCObj() {
	return (bCObj);
    }

    public DestinationInfo getDestinationInfo() {
	return (destInfo);
    }

    public void setDestinationInfo(DestinationInfo destInfo) {
	this.destInfo = destInfo;
    }

    public Vector getDurables() {
	return (durables);
    }

    public void setDurables(Vector durables) {
	this.durables = durables;
    }

    public String getExplorerLabel()  {
        if (destInfo != null)
            return destInfo.name;
        else
	    return (acr.getString(acr.I_BROKER_DEST));
    }

    public String getExplorerToolTip()  {
	return (null);
    }

    public ImageIcon getExplorerIcon()  {
	return (null);
    }

    public String getActionLabel(int actionFlag, boolean forMenu)  {
	if (forMenu)  {
	    switch (actionFlag)  {
	    case ActionManager.PAUSE:
	        return (acr.getString(acr.I_MENU_PAUSE_DEST));

	    case ActionManager.RESUME:
	        return (acr.getString(acr.I_MENU_RESUME_DEST));
	    }
	} else  {
	    switch (actionFlag)  {
	    case ActionManager.PAUSE:
	        return (acr.getString(acr.I_PAUSE_DEST));

	    case ActionManager.RESUME:
	        return (acr.getString(acr.I_RESUME_DEST));
	    }
	}

	return (null);
    }

    public int getExplorerPopupMenuItemMask()  {
	return (ActionManager.DELETE | ActionManager.PROPERTIES 
		| ActionManager.PURGE | ActionManager.PAUSE | ActionManager.RESUME);
    }


    public int getActiveActions()  {
	BrokerAdmin ba = getBrokerAdmin();
	int mask;

        // REVISIT: for now, no operation is allowed if we are not connected.
        // This should be taken out, as we should disallow selecting a dest
        // when it is not connected.
	if (!ba.isConnected())
	    mask = 0;
	else
	    if (destInfo.destState == DestState.RUNNING)  {
	        mask = ActionManager.DELETE | ActionManager.PROPERTIES
		     | ActionManager.PURGE | ActionManager.REFRESH 
		     | ActionManager.PAUSE;
	    } else if ((destInfo.destState == DestState.CONSUMERS_PAUSED)
		    || (destInfo.destState == DestState.PRODUCERS_PAUSED)
		    || (destInfo.destState == DestState.PAUSED))  {
	        mask = ActionManager.DELETE | ActionManager.PROPERTIES
		     | ActionManager.PURGE | ActionManager.REFRESH 
		     | ActionManager.RESUME;
	    } else {
	        mask = ActionManager.DELETE | ActionManager.PROPERTIES
		     | ActionManager.PURGE | ActionManager.REFRESH;
	    }

	return (mask);
    }



    public String getInspectorPanelClassName()  {
	return (null);
    }

    public String getInspectorPanelId()  {
	return (null);
    }

    public String getInspectorPanelHeader()  {
	return (null);
    }
}
