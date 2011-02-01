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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import javax.net.ssl.SSLEngine;
import java.util.logging.Logger;
import java.util.logging.Level;
import javax.jms.ConnectionFactory;
import com.sun.grizzly.ProtocolFilter;
import com.sun.grizzly.Context;
import com.sun.grizzly.ProtocolParser;
import com.sun.grizzly.util.AttributeHolder;
import com.sun.grizzly.util.WorkerThread;
import com.sun.grizzly.util.WorkerThread;
import com.sun.grizzly.async.AsyncWriteCallbackHandler;
import com.sun.grizzly.async.AsyncQueueWriteUnit;
import com.sun.grizzly.util.SSLOutputWriter;
import com.sun.messaging.bridge.service.BridgeContext;
import com.sun.messaging.bridge.service.stomp.resources.StompBridgeResources;

/**
 *
 * @author amyk
 */
public class StompProtocolFilter implements ProtocolFilter, StompOutputHandler {
     
     protected static final String STOMP_PROTOCOL_HANDLER_ATTR = "stomp-protocol-handler";
     private Logger _logger = null;

     private BridgeContext _bc = null;
     private Properties _jmsprop = null;
     private StompBridgeResources _sbr = null;

     public StompProtocolFilter(BridgeContext bc, Properties jmsprop) {
         _logger = StompServer.logger();
         _bc = bc;
         _jmsprop = jmsprop;
         _sbr = StompServer.getStompBridgeResources();
     }
    
    /**
     * Execute a unit of processing work to be performed. This ProtocolFilter
     * may either complete the required processing and return false, 
     * or delegate remaining processing to the next ProtocolFilter in a 
     * ProtocolChain containing this ProtocolFilter by returning true.
     * @param ctx {@link Context}
     * @return 
     * @throws java.io.IOException 
     */
    public boolean execute(Context ctx) throws IOException {

        StompProtocolHandler sph = null;
        try {
        sph = getStompProtocolHandler(ctx);

        final StompFrameMessage msg = (StompFrameMessage)ctx.removeAttribute(
                                                       ProtocolParser.MESSAGE);

        switch (msg.getCommand()) { 
            case CONNECT:
                sph.onCONNECT(msg, this, ctx);
                break; 
            case SEND:
                sph.onSEND(msg, this, ctx);
                break; 
            case SUBSCRIBE:
                StompOutputHandler soh = new AsyncStompOutputHandler(
                                                 ctx.getSelectionKey(),
                                                 ctx.getSelectorHandler(),
                ((WorkerThread)Thread.currentThread()).getSSLEngine(), sph, _bc);

                sph.onSUBSCRIBE(msg, this, ctx, soh);
                break; 
            case UNSUBSCRIBE:
                sph.onUNSUBSCRIBE(msg, this, ctx);
                break; 
            case BEGIN:
                sph.onBEGIN(msg, this, ctx);
                break; 
            case COMMIT:
                sph.onCOMMIT(msg, this, ctx);
                break; 
            case ABORT:
                sph.onABORT(msg, this, ctx);
                break; 
            case ACK:
                sph.onACK(msg, this, ctx);
                break; 
            case DISCONNECT:
                sph.onDISCONNECT(msg, this, ctx);
                break; 
            case ERROR:
                sendToClient(msg, ctx, sph);
                break; 
            default: 
                throw new IOException(
                "Internal Error: unexpected STOMP frame "+msg.getCommand());
        }
 
        } catch (Throwable t) {
            _logger.log(Level.SEVERE,  t.getMessage(), t);
            try {

            StompFrameMessage err = StompProtocolHandler.toStompErrorMessage("StompProtocolFilter", t);
            sendToClient(err, ctx, sph);

            } catch (Exception e) {
            _logger.log(Level.SEVERE, _sbr.getKString(_sbr.E_UNABLE_SEND_ERROR_MSG, t.toString(), e.toString()), e);
            }
        }
        return false;
    }

    public void sendToClient(StompFrameMessage msg) throws Exception {
        throw new UnsupportedOperationException("sendToclient(msg)");
    }
    
    public void sendToClient(final StompFrameMessage msg, 
                             final Context ctx,
                             StompProtocolHandler sph) throws Exception {
        try {
            boolean closechannel = false;
            if (msg.getCommand() == StompFrameMessage.Command.ERROR) {
                if (msg.isFatalERROR()) {
                    closechannel = true;
                }
            }

            SSLEngine sslengine = ((WorkerThread)Thread.currentThread()).getSSLEngine();
            ByteBuffer bb = msg.marshall();
            if (sslengine == null) {
                if (!closechannel) {
                    ctx.getAsyncQueueWritable().writeToAsyncQueue(bb);
                } else {
                    AsyncWriteCallbackHandler awcb = new AsyncWriteCallbackHandler() {

                        public void onWriteCompleted(SelectionKey key, 
                                                     AsyncQueueWriteUnit writtenRecord) {
                            _logger.log(Level.FINE,  "Completed sending "+msg+", canceling key "+key);
                           ctx.getSelectorHandler().getSelectionKeyHandler().cancel(key);
                        }
                        public void onException(Exception exception, SelectionKey key,
                                                ByteBuffer buffer, 
                                                java.util.Queue<AsyncQueueWriteUnit> remainingQueue) {
                        }
                    };
                    ctx.getAsyncQueueWritable().writeToAsyncQueue(bb, awcb);
                }
            } else {
                SelectableChannel sc = ctx.getSelectionKey().channel();

                synchronized(sc) {
                    SSLOutputWriter.flushChannel(sc, bb);
                }

                if (closechannel) {
                    SelectionKey key = ctx.getSelectionKey();
                    _logger.log(Level.INFO,  _sbr.getKString(_sbr.I_SENT_MSG_CANCEL_SELECTIONKEY, msg.toString(), key));
                    ctx.getSelectorHandler().getSelectionKeyHandler().cancel(key);
                }
            } 

        } catch (java.nio.channels.ClosedChannelException e) {
            _logger.log(Level.WARNING, _sbr.getKString(_sbr.W_EXCEPTION_ON_SEND_MSG, msg.toString(), e.toString()));
            if (sph != null) sph.close(false);
            throw e;
        }
    }
    
    /**
     * Execute any cleanup activities, such as releasing resources that were 
     * acquired during the execute() method of this ProtocolFilter instance.
     * @param ctx {@link Context}
     * @return 
     * @throws java.io.IOException 
     */
    public boolean postExecute(Context ctx) throws IOException {
        return true;
    }

    private StompProtocolHandler getStompProtocolHandler(Context ctx) {

        StompProtocolHandler sph = null;
        AttributeHolder attr = ctx.getAttributeHolderByScope(Context.AttributeScope.CONNECTION);
        if (attr == null) {
            WorkerThread workerThread = (WorkerThread)Thread.currentThread();
            attr = workerThread.getAttachment();
            ctx.getSelectionKey().attach(attr);
        }
        sph = (StompProtocolHandler)attr.getAttribute(STOMP_PROTOCOL_HANDLER_ATTR);
        if (sph == null) {
            sph = new StompProtocolHandler(_bc, _jmsprop);
            attr.setAttribute(STOMP_PROTOCOL_HANDLER_ATTR, sph);
        }
        return sph;
    }

}
