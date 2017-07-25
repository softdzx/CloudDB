package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import common.messages.EcsMessage;
import common.messages.MessageHandler;
import common.messages.Metadata;
import common.messages.EcsMessage.StatusType;

public class ServerConnection{
	private MessageHandler messageHandler;
	private int cacheSize;
	private String displacementStrategy;
	private Metadata metadata;

	/**
	 * This class handles the connection to the server.
	 * @param input		Input stream of the connected kvserver socket.
	 * @param output	Output stream of the connected kvserver socket.
	 * @param cacheSize	cacheSize of the KVServer.
	 * @param displacementStrategy	displacementStrategy of the KVServer.
	 * @param metadata	Metadata of the ECS,
	 * @param logger	Logger of the ECS.
	 * @throws IOException 
	 */

	public ServerConnection(final Socket socket, final int cacheSize, final String displacementStrategy, final Metadata metadata, final Logger logger) throws IOException{
	
		this.cacheSize = cacheSize;
		this.displacementStrategy = displacementStrategy;
		this.metadata = metadata;
		messageHandler = new MessageHandler(socket, logger);
	}
	
	/**
	 * Sends initialization message to the KVServer.
	 * @return 
	 */

	public int init(){
		EcsMessage initMessage = new EcsMessage();
		initMessage.setCacheSize(cacheSize);
		initMessage.setDisplacementStrategy(displacementStrategy);
		initMessage.setMetadata(metadata);
		initMessage.setStatusType(StatusType.INIT);

		messageHandler.sendMessage(initMessage.serialize().getMsg());
		
		byte [] b =messageHandler.receiveMessage();
		EcsMessage portMessage = new EcsMessage();
		portMessage = portMessage.deserialize(new String(b));
		return portMessage.getPort();
	}
	
	public void startServer(){
		
		EcsMessage startMessage = new EcsMessage();
		startMessage.setStatusType(StatusType.START);

		messageHandler.sendMessage(startMessage.serialize().getMsg());
	}
	
	public void stopServer(){
		EcsMessage stopMessage = new EcsMessage();
		stopMessage.setStatusType(EcsMessage.StatusType.STOP);

		messageHandler.sendMessage(stopMessage.serialize().getMsg());
	}

	public void shutDown(){
		EcsMessage shutDownMessage = new EcsMessage();
		shutDownMessage.setStatusType(EcsMessage.StatusType.SHUTDOWN);

		messageHandler.sendMessage(shutDownMessage.serialize().getMsg());
	}
		
	
	public void setWriteLock(){
		EcsMessage writeLockMessage = new EcsMessage();
		writeLockMessage.setStatusType(EcsMessage.StatusType.WRITELOCK);

		messageHandler.sendMessage(writeLockMessage.serialize().getMsg());
	}
	
	public void releaseWriteLock(){
		EcsMessage writeLockMessage = new EcsMessage();
		writeLockMessage.setStatusType(EcsMessage.StatusType.WRITELOCK);

		messageHandler.sendMessage(writeLockMessage.serialize().getMsg());
		
	}
	
	public void receiveData(){
		
		EcsMessage receiveMessage = new EcsMessage();
		receiveMessage.setStatusType(EcsMessage.StatusType.RECEIVE);

		messageHandler.sendMessage(receiveMessage.serialize().getMsg());
		
		byte [] b =messageHandler.receiveMessage();
		EcsMessage preparedMessage = new EcsMessage();
		preparedMessage = preparedMessage.deserialize(new String(b));
		if(preparedMessage.getStatusType() == StatusType.PREPARED){
			
		}		
	}
	
	public void moveData(String from, String to, String ip, int port){
		EcsMessage moveMessage = new EcsMessage();
		moveMessage.setStatusType(StatusType.MOVE);
		moveMessage.setFrom(from);
		moveMessage.setTo(to);
		moveMessage.setIp(ip);
		moveMessage.setPort(port);
		
		messageHandler.sendMessage(moveMessage.serialize().getMsg());

		byte [] msg = messageHandler.receiveMessage();
		EcsMessage moveFinished = new EcsMessage();
		moveFinished = moveFinished.deserialize(new String(msg));
		
		if(moveFinished.getStatusType() == StatusType.MOVEFINISH){

		}
	}

	public void removeData(String from, String to){
		EcsMessage moveMessage = new EcsMessage();
		moveMessage.setStatusType(StatusType.REMOVE);
		moveMessage.setFrom(from);
		moveMessage.setTo(to);
		
		messageHandler.sendMessage(moveMessage.serialize().getMsg());
	}

	
	public void update(Metadata metadata){
		EcsMessage updateMessage = new EcsMessage();
		updateMessage.setStatusType(StatusType.UPDATE);
		updateMessage.setMetadata(metadata);
		messageHandler.sendMessage(updateMessage.serialize().getMsg());
	}
	
	public byte [] getInput(){
		return messageHandler.receiveMessage();
	}
}
