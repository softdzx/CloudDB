package common.messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

import org.apache.log4j.Logger;

public class MessageHandler {

	private OutputStream output;
 	private InputStream input;
 	private Socket socket;
	private Logger logger;;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

	public MessageHandler(Socket socket, Logger logger) throws IOException{
		this.input = socket.getInputStream();
		this.output = socket.getOutputStream();
		this.socket = socket;
		this.logger = logger;
	}

	/**
	 * Receives message from the server and converts it to a TextMessage.
	 * @return A TextMessage representing server response.
	 * @throws IOException Some exception while reading the input stream occured
	 */
	
	public byte [] receiveMessage() {
		
		logger.info("start to wait to receive message, socket.localport: " + socket.getLocalPort() + ", port:  " +socket.getPort());
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		byte read = 0;
		boolean reading = false;

		/* read first char from stream */
		try {
			read = (byte) input.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("the error caused by starting receiveMessage(): " + e.getMessage() + "socket.localport: " + socket.getLocalPort() + ", port:  " +socket.getPort());
			return null;
		}
		
		reading = true;
		
		while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			try {
				read = (byte) input.read();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("the error caused by looping receiveMessage(): " + e.getMessage());
				reading = false;
			}
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = addCtrChars(tmp);
		String msg = new String(tmp).trim();
		logger.info("Received message:" + msg);
		
		return tmp;
    }

	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(String msg) {
		logger.info("start to send message:" + " socket.localport: " + socket.getLocalPort() + ", port:  " +socket.getPort());
		byte[] msgBytes = toByteArray(msg);
		try {
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Error during sendMessage(): " + e.getMessage() + "; socket.localport: " + socket.getLocalPort() + ", port:  " +socket.getPort());
		}
		logger.info("finish send message:" + msg + "; socket.localport: " + socket.getLocalPort() + ", port:  " +socket.getPort());
    }

	public void close(){
		try {
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("The messagehandler cannot be closed");
		}
	}
	
	private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
	
	private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}


	
}
