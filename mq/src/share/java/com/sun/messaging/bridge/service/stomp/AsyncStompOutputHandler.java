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

package com.sun.messaging.bridge.service.stomp;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import javax.net.ssl.SSLEngine;
import com.sun.grizzly.Context;
import com.sun.grizzly.SelectorHandler;
import com.sun.grizzly.util.SSLOutputWriter;
import com.sun.grizzly.util.WorkerThread;
import com.sun.grizzly.async.AsyncWriteCallbackHandler;
import com.sun.grizzly.async.AsyncQueueWriteUnit;
import com.sun.messaging.bridge.service.BridgeContext;
import com.sun.messaging.bridge.service.stomp.resources.StompBridgeResources;

/**
 *
 * @author amyk
 */
public class AsyncStompOutputHandler implements StompOutputHandler {

    private  Logger _logger = StompServer.getLogger();

    private SelectionKey _selectionkey = null;
    private SelectorHandler _selectorhandler = null;
    private SSLEngine _sslengine = null;
    private StompProtocolHandler _sph = null;
    private BridgeContext _bc = null;
	private StompBridgeResources _sbr = null;
     
    public AsyncStompOutputHandler(SelectionKey selkey, SelectorHandler selhdle, 
                                   SSLEngine engine, StompProtocolHandler sph,
                                   BridgeContext bc) { 
        //ctx.setKeyRegistrationState(Context.KeyRegistrationState.NONE);
        _selectionkey = selkey;
        _selectorhandler = selhdle;
        _sslengine = engine;
        _sbr = StompServer.getStompBridgeResources();
        _sph = sph;
        _bc = bc;
    }

    public void sendToClient(StompFrameMessage msg, Context ctx, 
                             StompProtocolHandler sph) throws Exception {

        throw new UnsupportedOperationException("sendToClient(msg, ctx)");
    }

    public void sendToClient(final StompFrameMessage msg) throws Exception {
        boolean closechannel = false;
        if (msg.getCommand() == StompFrameMessage.Command.ERROR) {
            if (msg.isFatalERROR()) {
                closechannel = true;
            }
        }
 
        try {
        ByteBuffer bb = msg.marshall();
        if (_sslengine == null) {
             if (!closechannel) {
                 _selectorhandler.getAsyncQueueWriter().write(_selectionkey, bb);
             } else {

                 AsyncWriteCallbackHandler awcb = new AsyncWriteCallbackHandler() {

                     public void onWriteCompleted(SelectionKey key,
                                                  AsyncQueueWriteUnit writtenRecord) {
                           _logger.log(Level.FINE,  "Completed sending "+msg+", canceling key "+key);
                           _selectorhandler.getSelectionKeyHandler().cancel(key);
                     }
                     public void onException(Exception exception, SelectionKey key,
                                             ByteBuffer buffer,
                                             java.util.Queue<AsyncQueueWriteUnit> remainingQueue) {
                     }
                 };
                 _selectorhandler.getAsyncQueueWriter().write(_selectionkey, bb, awcb);

             }
        } else { 
           int osize = Math.max(8192,  _sslengine.getSession().getPacketBufferSize());
           ByteBuffer outputBB = ByteBuffer.allocate(osize);
           outputBB.position(0);

           SelectableChannel sc = _selectionkey.channel();
           synchronized(sc) {

           SSLOutputWriter.flushChannel(sc, bb, outputBB, _sslengine);

           }

           if (closechannel) {
               _logger.log(Level.INFO,  _sbr.getKString(_sbr.I_SENT_MSG_CANCEL_SELECTIONKEY, msg.toString(), _selectionkey));
               _selectorhandler.getSelectionKeyHandler().cancel(_selectionkey);
           }

        }

        } catch (java.nio.channels.ClosedChannelException e) {
            _logger.log(Level.WARNING, StompServer.getStompBridgeResources().getKString(
                    StompServer.getStompBridgeResources().W_SEND_MSG_TO_CLIENT_FAILED, msg.toString(), e.toString()));
            _sph.close(true);
            throw e;
        }

    }
}
