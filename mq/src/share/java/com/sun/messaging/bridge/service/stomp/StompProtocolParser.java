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
import java.util.concurrent.atomic.*;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.sun.grizzly.ProtocolParser;
import com.sun.grizzly.util.ByteBufferFactory;
import com.sun.grizzly.util.WorkerThread;
import com.sun.messaging.bridge.service.BridgeContext;
import com.sun.messaging.bridge.service.stomp.resources.StompBridgeResources;

/**
 * Parse bytes into a STOMP protocol frame message.
 *
 * @author amyk
 */

public class StompProtocolParser implements ProtocolParser<StompFrameMessage> {
    
	private Logger _logger = null;

    //current byte buffer
    private ByteBuffer _buffer = null;

    private StompFrameMessage _message = null;

    //current position in _buffer to start parse
    private int _position = 0;

    private boolean _expectingMoreData = true;
	private boolean _hasMoreBytesToParse = false;
    private BridgeContext _bc = null;
    private final String _OOMMSG = "Running low on memory while parsing stomp incoming data"; 
	 

    protected StompProtocolParser(BridgeContext bc) {
        _logger = StompServer.logger();
        _bc = bc;
    }

    /**
     * Is this ProtocolParser expecting more data ?
     * 
     * This method is typically called after a call to <code>parseBytes()</code>
     * to determine if the {@link ByteBuffer} which has been parsed
     * contains a partial message
     * 
     * @return - <tt>true</tt> if more bytes are needed to construct a
     *           message;  <tt>false</tt>, if no 
     *           additional bytes remain to be parsed into a <code>T</code>.
     *	 	 Note that if no partial message exists, this method should
     *		 return false.
     */
    public boolean isExpectingMoreData() {
        if (_logger.isLoggable(Level.FINEST)) {
        _logger.log(Level.FINEST, "expectingMoreData="+_expectingMoreData);
        }
        return _expectingMoreData;
    }
    
    /**
     * Are there more bytes to be parsed in the {@link ByteBuffer} given
     * to this ProtocolParser's <code>setBuffer</code> ?
     * 
     * This method is typically called after a call to <code>parseBytes()</code>
     * to determine if the {@link ByteBuffer} has more bytes which need to
     * parsed into a message.
     * 
     * @return <tt>true</tt> if there are more bytes to be parsed.
     *         Otherwise <tt>false</tt>.
     */
    public boolean hasMoreBytesToParse() {
        if (_logger.isLoggable(Level.FINEST)) {
        _logger.log(Level.FINEST, "hasMoreBytesToParse="+_hasMoreBytesToParse);
        }

        return _hasMoreBytesToParse;
    }

    
    /**
     * Get the next complete message from the buffer, which can then be
     * processed by the next filter in the protocol chain. Because not all
     * filters will understand protocol messages, this method should also
     * set the position and limit of the buffer at the start and end
     * boundaries of the message. Filters in the protocol chain can
     * retrieve this message via context.getAttribute(MESSAGE)
     *
     * @return The next message in the buffer. If there isn't such a message,
     *	return <code>null.</code>
     *
     */
    public StompFrameMessage getNextMessage() {
        if (_logger.isLoggable(Level.FINEST)) {
        _logger.log(Level.FINEST, "entering: hasmorebytestoparse="+_hasMoreBytesToParse+", expectmoredata="+_expectingMoreData+", _position="+_position+", _buffer="+_buffer+", msgcmd="+_message.getCommand());
        }

        StompFrameMessage msg = _message;

        if ((_buffer.position() - _position) > 0) {
            _hasMoreBytesToParse = true;
        } else {
            _hasMoreBytesToParse = false;
        }
        _expectingMoreData = false;
        _message = null;

        if (_logger.isLoggable(Level.FINEST)) {
        _logger.log(Level.FINEST, "leaving: hasmorebytestoparse="+_hasMoreBytesToParse+", expectmoredata="+_expectingMoreData);
        }

        return msg;
    }

    /**
     * Indicates whether the buffer has a complete message that can be
     * returned from <code>getNextMessage</code>. Smart implementations of
     * this will set up all the information so that an actual call to
     * <code>getNextMessage</code> doesn't need to re-parse the data.
     */
    public boolean hasNextMessage() {
        try {

        if (_logger.isLoggable(Level.FINEST)) {
        _logger.log(Level.FINEST, "this: "+this+", _position="+_position+", _buffer="+_buffer);
        }

        if (_buffer == null) return false;

        if (_logger.isLoggable(Level.FINEST)) {
        _logger.log(Level.FINEST, "_position="+_position+", _buffer_position=:"+_buffer.position());

        _logger.log(Level.FINEST, "_buffer=" +
                new String(_buffer.array(), _buffer.arrayOffset(), 
                                   _buffer.remaining(), "UTF-8"));
        }

        _hasMoreBytesToParse = false;

        if (_message == null) {

            if ((_buffer.position() - _position) >= StompFrameMessage.MIN_COMMAND_LEN) {
                _message = StompFrameMessage.parseCommand(_buffer, _position);

                if (_logger.isLoggable(Level.FINEST)) {
                _logger.log(Level.FINEST, "returned from parseCommand");
                }
            }


            if (_message == null) {
                if ((_buffer.capacity() - _buffer.position()) < StompFrameMessage.MAX_COMMAND_LEN) {
                    _logger.log(Level.FINEST, 
                    "_buffer: capacity="+_buffer.capacity()+
                    ", position="+_buffer.position()+
                    ", extend _buffer for < max-command-len="+
                    StompFrameMessage.MAX_COMMAND_LEN);

                    extendByteBuffer();
                }
                _expectingMoreData = true;
                return false;
            }
            _position = _message.getByteBufferPosition();
        }

        if (_message.getNextParseStage() == StompFrameMessage.ParseStage.HEADER) { 
            _message.parseHeader(_buffer, _position);

            if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "returned from parseHeader");
            }

            _position = _message.getByteBufferPosition();
        }
        if (_message.getNextParseStage() == StompFrameMessage.ParseStage.BODY) { 
            _message.readBody(_buffer, _position);
            _position = _message.getByteBufferPosition();
        }
        if (_message.getNextParseStage() == StompFrameMessage.ParseStage.NULL) { 
            _message.readNULL(_buffer, _position);
            _position = _message.getByteBufferPosition();
        }
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST,
            "_position="+_position+", _buffer="+_buffer+", nextParseState="+_message.getNextParseStage());
        }

        if (_message.getNextParseStage() != StompFrameMessage.ParseStage.DONE) { 
            if (_buffer.capacity() == _buffer.position()) {
                if(_position < _buffer.position()) {
                   extendByteBuffer();
                } else {
                   long clen = _message.getContentLength();
                   allocateNewByteBuffer((clen == -1 ? _buffer.capacity():clen));
                }
            } else if (_position == _buffer.position()) {//although only needed by SSL
                _position = 0;
               _buffer.clear();
            }
            _expectingMoreData = true;
            return false;
        } 
        _expectingMoreData = false;
        Exception pex = _message.getParseException();
        if (pex != null) {
            if (pex instanceof FrameParseException) {
                _message = ((FrameParseException)pex).getStompMessageERROR();
            } else {
                _message = (new FrameParseException(pex.getMessage(), pex)).getStompMessageERROR();
            }
        }
        return true;

        } catch (Throwable t) {
            if (t instanceof OutOfMemoryError) { 
                _logger.log(Level.SEVERE, _OOMMSG);
                _bc.handleGlobalError(t, _OOMMSG);
            } else {
                _logger.log(Level.SEVERE, StompServer.getStompBridgeResources().getKString(
                            StompBridgeResources.E_PARSE_INCOMING_DATA_FAILED, t.getMessage()), t); 
            }
            try {

            if (t instanceof FrameParseException) {
                _message = ((FrameParseException)t).getStompMessageERROR();
                _message.setFatalERROR();
            } else {
                _message = (new FrameParseException(t.getMessage(), t, true)).getStompMessageERROR();
            }

            } catch (Throwable tt) {

            if (t instanceof OutOfMemoryError) {
                _message = FrameParseException.OOMMSG;
            } else {
                _logger.log(Level.SEVERE, StompServer.getStompBridgeResources().getKString(
                            StompBridgeResources.E_UNABLE_CREATE_ERROR_MSG, t.getMessage()), tt);
                _expectingMoreData = false;
                RuntimeException re = new RuntimeException(tt.getMessage());
                re.initCause(tt);
                throw re;
            }

            }
            //_position = _buffer.position();
            _buffer.clear();
            _position = 0;
            _expectingMoreData = false;
            return true;
        }
    }


    private void extendByteBuffer() throws FrameParseException {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "entering: _position="+_position+", _buffer="+_buffer);
        }

        if (_position > _buffer.position()) {
            throw new IllegalStateException(StompServer.getStompBridgeResources().getKString(
                  StompBridgeResources.X_UNEXPECTED_PARSER_POSITION_EXT, String.valueOf(_position), String.valueOf(_buffer.position())));
        }

        ByteBuffer newbuf = ByteBufferFactory.allocateView(
                   _buffer.capacity()*2, _buffer.isDirect());

        int savepos = _buffer.position();
        _buffer.position(_position);
        newbuf.put(_buffer);
        _buffer = newbuf;
        _buffer.position(savepos-_position);
        _position = 0;

        WorkerThread workerThread = (WorkerThread) Thread.currentThread();
        workerThread.setByteBuffer(_buffer);

        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "leaving: _position="+_position+", _buffer="+_buffer);
        }
    }

    private void allocateNewByteBuffer(long size) throws FrameParseException {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "entering: _position="+_position+", _buffer="+_buffer+", size="+size);
        }
        if (_position != _buffer.position()) {
            throw new IllegalStateException(StompServer.getStompBridgeResources().getKString(
                  StompBridgeResources.X_UNEXPECTED_PARSER_POSITION, _position, _buffer.position()));
        }

        ByteBuffer newbuf = ByteBuffer.allocate(_buffer.capacity());
        WorkerThread workerThread = (WorkerThread) Thread.currentThread();
        workerThread.setByteBuffer(newbuf);
        _buffer = newbuf;
        _position = 0;

        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "leaving: _position="+_position+", _buffer="+_buffer);
        }
    }

    /**
     * Set the buffer to be parsed. This method should store the buffer and
     * its state so that subsequent calls to <code>getNextMessage</code>
     * will return distinct messages, and the buffer can be restored after
     * parsing when the <code>releaseBuffer</code> method is called.
     */

    AtomicInteger ccc = new AtomicInteger();

    public void startBuffer(ByteBuffer bb) {

        if (_logger.isLoggable(Level.FINEST)) {
        _logger.log(Level.FINEST, "this:"+this + " total: " + ccc.addAndGet(bb.position()));
        }

        _buffer = bb;
    }
    
    /**
     * No more parsing will be done on the buffer passed to
     * <code>startBuffer.</code>
     * Set up the buffer so that its position is the first byte that was
     * not part of a full message, and its limit is the original limit of
     * the buffer.
     *
     * @return -- true if the parser has saved some state (e.g. information
     * data in the buffer that hasn't been returned in a full message);
     * otherwise false. If this method returns true, the framework will
     * make sure that the same parser is used to process the buffer after
     * more data has been read.
     */
    public boolean releaseBuffer() {
        if (_logger.isLoggable(Level.FINEST)) {
        _logger.log(Level.FINEST, "expectingMoreData="+_expectingMoreData+
                ", hasMoreBytesToParse="+_hasMoreBytesToParse+", _position="+_position);
        }

        if (!_hasMoreBytesToParse) {
             if (_buffer.position() == _position) {
                 _buffer.clear();
                 _position = 0;
             } else {
                 _buffer.limit(_buffer.position());
                 _buffer.position(_position);
                 _buffer.compact();
                 _position = 0;
             }
        }

        return _expectingMoreData;
    }

}
