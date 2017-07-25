package replication;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.messages.MessageHandler;
import common.messages.CommonMessage;
import storage.StorageManager;

public class RepConnection implements Runnable {
	private ServerSocket replicaSocket;
	private Logger logger;
	private StorageManager storageManager;
	
	
	public RepConnection(int port, StorageManager storageManager, Logger logger){
		try {
			replicaSocket = new ServerSocket(port+20);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.storageManager = storageManager;
		this.logger = logger;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		Socket server = null;
		try {
			while((server = replicaSocket.accept()) != null){
				final Socket newserver = server;
				new Thread(){
					public void run(){
						byte [] message = null;
						MessageHandler serverMsgHandler = null;
						try {
							serverMsgHandler = new MessageHandler(newserver, logger);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							logger.error(e.getMessage());
						}
	
						while((message = serverMsgHandler.receiveMessage()) != null){							
							CommonMessage receivedMessage = new CommonMessage(message);
							receivedMessage = receivedMessage.deserialize();
							//storageManager.update_replica(receivedMessage.getKey(), receivedMessage.getValue());
							storageManager.update_replica(receivedMessage.getKey(), receivedMessage.getValue());
						}
					}
				}.start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void close(){
		try {
			replicaSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
