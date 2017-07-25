package app_kvClient;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvClient.ClientSocketListener.SocketStatus;
import app_kvEcs.NodeInfo;
import common.messages.CommonMessage;
import common.messages.BrokerMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import logger.LogSetup;
import common.messages.MessageHandler;
import common.messages.Metadata;

public class KVClient implements KVCommInterface {	
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private MessageHandler messageHandler;
	
	private boolean running;
	
	private Socket clientSocket; 	
	private Metadata metadata;
 	private Socket subscribeSocket;
 	private MessageHandler subscriptionHandler;

 	private String address;
 	private int port;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public KVClient(String address, int port){
		this.address = address;
		this.port = port;
		
		listeners = new HashSet<ClientSocketListener>();
		setRunning(false);

	}
	
	public void run() {
		
		setRunning(true);
		byte [] msg = messageHandler.receiveMessage();
		CommonMessage receivedMessage = new CommonMessage(msg);
		for(ClientSocketListener listener : listeners) {
			listener.handleNewMessage(receivedMessage);
		}
		
	}
	
	private synchronized void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("Tearing down the connection ...");
		if (clientSocket != null) {
			clientSocket.close();
			clientSocket = null;
			logger.info("Connection closed!");
		}
	}
	
	public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}
	
	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}
	
	
 	/**
 	 * Opens the socket for connection to the server.
 	 * @throws IOException 
 	 */
	@Override
	public void connect() throws IOException, UnknownHostException {
		try {
			
			clientSocket = new Socket(address, port);
			messageHandler = new MessageHandler(clientSocket, logger);
			
			run();
			
			logger.info("Connection established");

		} catch (UnknownHostException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			throw new UnknownHostException();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			throw new IOException();
		}
	
	}
/**
 * Disconnects the client from the server and sets the SocketStatus to
 * DISCONNECTED.
 */
	@Override
	public void disconnect() {
		logger.info("Trying to close connection ...");
		
		try {
			tearDownConnection();
			messageHandler.close();
			messageHandler = null;
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	/**
	 * Creates a TextMessage that is then filled with required values for the
	 * put command and send to the responsible server.
	 * If SERVER_NOT_RESPONSIBLE is received, update the metadata, reestablish the
	 * connection with the correct server and retry the put operation.
	 */
	@Override
	public CommonMessage put(String key, String value) throws Exception {
		
		CommonMessage sentMessage = new CommonMessage();
		sentMessage.setStatusType(StatusType.PUT);
		sentMessage.setKey(key);
		sentMessage.setValue(value);
		
		if(metadata != null){
			NodeInfo correctServer = metadata.getServerForKey(key);
			NodeInfo successor = metadata.getSuccessor(correctServer.hashedkey);
			NodeInfo sesuccessor = null;
			if(successor != null){
				sesuccessor = metadata.getSuccessor(successor.hashedkey);
			}

			if((!correctServer.ip.equals(this.address) || !correctServer.port.equals("" + this.port)) && (!sesuccessor.ip.equals(this.address) || !sesuccessor.port.equals("" + this.port)) && (!successor.ip.equals(this.address) || !successor.port.equals("" + this.port))){
				disconnect();
				this.address = correctServer.ip;
				this.port = Integer.parseInt(correctServer.port);
				connect();
			}
		}

		messageHandler.sendMessage(sentMessage.serialize().getMsg());

		byte [] msg = messageHandler.receiveMessage();
		
		CommonMessage receivedMessage = new CommonMessage(msg);
		receivedMessage = receivedMessage.deserialize();
				
		if(receivedMessage.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)){
			
			if(receivedMessage.getMetadata() != null){
				this.metadata = receivedMessage.getMetadata();
				logger.warn("Metadata stale! Metadata was updated!");
				reestablishConnection(key);
				return put(key, value);
			}
			else{
				logger.error("Metadata stale, but no metadata update was received!");
				return new CommonMessage("Error while contacting the server.");
			}
		}
		else{
			for(ClientSocketListener listener : listeners) {
				listener.handleNewMessage(receivedMessage);
			}

			return receivedMessage;
		}
	
	}

	/**
	 * Creates a TextMessage that is then filled with required values for the
	 * put command and send to the responsible server.
	 * If SERVER_NOT_RESPONSIBLE is received, update the metadata, reestablish the
	 * connection with the correct server and retry the put operation.
	 */
	@Override
	public CommonMessage merge(String key, String value) throws Exception {
		
		CommonMessage sentMessage = new CommonMessage();
		sentMessage.setStatusType(StatusType.MERGE_UPDATE);
		sentMessage.setKey(key);
		sentMessage.setValue(value);
		
		if(metadata != null){
			NodeInfo correctServer = metadata.getServerForKey(key);
			NodeInfo successor = metadata.getSuccessor(correctServer.hashedkey);
			NodeInfo sesuccessor = null;
			if(successor != null){
				sesuccessor = metadata.getSuccessor(successor.hashedkey);
			}

			if((!correctServer.ip.equals(this.address) || !correctServer.port.equals("" + this.port)) && (!sesuccessor.ip.equals(this.address) || !sesuccessor.port.equals("" + this.port)) && (!successor.ip.equals(this.address) || !successor.port.equals("" + this.port))){
				disconnect();
				this.address = correctServer.ip;
				this.port = Integer.parseInt(correctServer.port);
				connect();
			}
		}

		messageHandler.sendMessage(sentMessage.serialize().getMsg());

		byte [] msg = messageHandler.receiveMessage();
		
		CommonMessage receivedMessage = new CommonMessage(msg);
		receivedMessage = receivedMessage.deserialize();
				
		if(receivedMessage.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)){
			
			if(receivedMessage.getMetadata() != null){
				this.metadata = receivedMessage.getMetadata();
				logger.warn("Metadata stale! Metadata was updated!");
				reestablishConnection(key);
				return merge(key, value);
			}
			else{
				logger.error("Metadata stale, but no metadata update was received!");
				return new CommonMessage("Error while contacting the server.");
			}
		}
		else{
			for(ClientSocketListener listener : listeners) {
				listener.handleNewMessage(receivedMessage);
			}

			return receivedMessage;
		}
	
	}
	
	/**
	 * Creates a TextMessage that is then filled with required values for the
	 * get command and send to the responsible server.
	 * If SERVER_NOT_RESPONSIBLE is received, update the metadata, reestablish 
	 * the connection with the correct server and retry the get operation.
	 */
	@Override
	public CommonMessage get(String key) throws Exception {
		
		CommonMessage sentMessage = new CommonMessage();
		sentMessage.setStatusType(StatusType.GET);
		sentMessage.setKey(key);
		
		if(metadata != null){
			NodeInfo correctServer = metadata.getServerForKey(key);
			NodeInfo successor = metadata.getSuccessor(correctServer.hashedkey);
			NodeInfo sesuccessor = null;
			if(successor != null){
				sesuccessor = metadata.getSuccessor(successor.hashedkey);
			}
			
//			if(correctServer != null && (!correctServer.ip.equals(this.address) || !correctServer.port.equals(""+this.port))){
			if((!correctServer.ip.equals(this.address) || !correctServer.port.equals("" + this.port)) && (!sesuccessor.ip.equals(this.address) || !sesuccessor.port.equals("" + this.port)) && (!successor.ip.equals(this.address) || !successor.port.equals("" + this.port))){
				disconnect();
				this.address = correctServer.ip;
				this.port = Integer.parseInt(correctServer.port);
				connect();
			}
		}
		
		logger.info("the message to be sent is: " + sentMessage.getMsg());
		logger.info("the message to be sent is: " + sentMessage.getMsg() + messageHandler);
		
		messageHandler.sendMessage(sentMessage.serialize().getMsg());
		
		byte [] msg = messageHandler.receiveMessage();
		
		CommonMessage receivedMessage = new CommonMessage(msg);
		receivedMessage = receivedMessage.deserialize();

		
		if(receivedMessage.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)){
			
			if(receivedMessage.getMetadata() != null){
				this.metadata = receivedMessage.getMetadata();
				logger.warn("Metadata stale! Metadata was updated!");
				reestablishConnection(key);
				return get(key);
			} else{
				logger.error("Metadata stale, but no metadata update was received!");
				return new CommonMessage("Error while contacting the server.");
			}
			
		} else {
			
			for(ClientSocketListener listener : listeners) {
				listener.handleNewMessage(receivedMessage);
			}
			return receivedMessage;
			
		}
	}
	
	/**
	 * Creates a TextMessage that is then filled with required values for the
	 * subscribe command and send to the responsible server.
	 * @param type	The subscription type, DELETE or CHANGE.
	 * @param key	The key that the subscription targets.
	 */
	public KVMessage subscribe(String type, String key) throws Exception {
		BrokerMessage sentMessage = new BrokerMessage();
		
		if(type.equals("DELETE")){
			sentMessage.setStatus(common.messages.BrokerMessage.StatusType.SUBSCRIBE_DELETE);
		}
		else{
			sentMessage.setStatus(common.messages.BrokerMessage.StatusType.SUBSCRIBE_CHANGE);
		}
		sentMessage.setKey(key);
		
		if(subscribeSocket == null){
			try {
				if(metadata != null){
					subscribeSocket = new Socket(metadata.getBrokerIP(), metadata.getBrokerPort());
				} else{
					subscribeSocket = new Socket("127.0.0.1", 49999);	
				}
				subscriptionHandler = new MessageHandler(subscribeSocket, logger);
				
				logger.info("Subscription connection established");

			} catch (UnknownHostException e) {
				logger.error(e.getMessage());
				throw new UnknownHostException();
			} catch (IOException e) {
				logger.error(e.getMessage());
				throw new IOException();
			}
		}

		subscriptionHandler.sendMessage(sentMessage.serialize().getMsg());

		byte [] msg = subscriptionHandler.receiveMessage();
		
		CommonMessage receivedMessage = new CommonMessage(msg);
		receivedMessage = receivedMessage.deserialize();
		for(ClientSocketListener listener : listeners) {
			listener.handleNewMessage(receivedMessage);
		}

		return (KVMessage)receivedMessage.deserialize();
	
	}
	
	/**
	 * Creates a TextMessage that is then filled with required values for the
	 * unsubscribe command and send to the responsible server.
	 * @param type	The subscription type, DELETE or CHANGE.
	 * @param key	The key that the subscription targets.
	 */
	public KVMessage unsubscribe(String type, String key) throws Exception {
		BrokerMessage sentMessage = new BrokerMessage();
		
		if(type.equals("DELETE")){
			sentMessage.setStatus(common.messages.BrokerMessage.StatusType.UNSUBSCRIBE_DELETE);
		}
		else{
			sentMessage.setStatus(common.messages.BrokerMessage.StatusType.UNSUBSCRIBE_CHANGE);
		}
		sentMessage.setKey(key);
		
		if(subscribeSocket == null){
			throw new Exception();
		}

		subscriptionHandler.sendMessage(sentMessage.serialize().getMsg());

		byte [] msg = subscriptionHandler.receiveMessage();
		
		CommonMessage receivedMessage = new CommonMessage(msg);
		receivedMessage = receivedMessage.deserialize();
		for(ClientSocketListener listener : listeners) {
			listener.handleNewMessage(receivedMessage);
		}

		return (KVMessage)receivedMessage.deserialize();
	
	}
	
	/**
	 * Looks up the correct server for the provided key in the
	 * updated metadata and establishes a new connection to
	 * this server after closing the old connection.
	 * @param key			The key of the original get/put request 
	 * @throws Exception	
	 */
	private void reestablishConnection(String key) throws Exception{
		NodeInfo correctserver = metadata.getServerForKey(key);
		
		if(correctserver != null)
		{
			disconnect();
			this.address = correctserver.ip;
			this.port = Integer.parseInt(correctserver.port);
			connect();
		}
	}
	
}
