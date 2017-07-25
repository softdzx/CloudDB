package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.ServerSocket;

import org.apache.log4j.*;

import app_kvEcs.NodeInfo;
import app_kvServer.ServerState.State;
import common.messages.KVMessage.StatusType;
import reconciliation.VectorClock;
import replication.LazyReplication;
import replication.RepConnection;
import replication.ReplicationManager;
import common.messages.MessageHandler;
import common.messages.Metadata;
import common.messages.CommonMessage;
import common.messages.BrokerMessage;
import storage.StorageManager;

/**
 * Represents a connection end point for a particular client that is connected
 * to the server. This class is responsible for message reception and sending.
 * The class also implements the echo functionality. Thus whenever a message is
 * received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;

	private Socket clientSocket;
	private ServerSocket serverSocket;

	private Metadata metadata;

	private MessageHandler messageHandler;
	private StorageManager storageManager;

	private ReplicationManager replicationManager;
//	private MessageHandler brokerMsgHandler;

	/**
	 * 
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket
	 *            the Socket object for the client connection.
	 * @param keyvalue
	 *            The cache of the server.
	 * @param cacheSize
	 *            The cache size of the server cache.
	 * @param strategy
	 *            Persistence strategy of the server: LRU | LFU | FIFO.
	 * @param persistance
	 *            Instance of Persictance class, which handles the reading and
	 *            writing to the storage file.
	 * @param metadata
	 *            Metadata set of the server.
	 */
	public ClientConnection(Socket clientSocket, ServerSocket serverSocket, Metadata metadata,
			MessageHandler brokerMessageHandler) {
		this.clientSocket = clientSocket;
		this.serverSocket = serverSocket;
		this.isOpen = true;
		this.metadata = metadata;

		storageManager = StorageManager.getInstance();
		replicationManager = ReplicationManager.getInstance();
//		this.brokerMsgHandler = brokerMessageHandler;

	}

	/**
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client.
	 */
	public void run() {
		try {
			messageHandler = new MessageHandler(clientSocket, logger);
			CommonMessage welcomemsg = new CommonMessage("Connection to MSRG Echo server established: "
					+ clientSocket.getLocalAddress() + " / " + clientSocket.getLocalPort());
			messageHandler.sendMessage(welcomemsg.getMsg());

			CommonMessage receivedMessage = null;

			while (isOpen) {

				if (!clientSocket.getInetAddress().isReachable(10000)) {
					logger.info("Client " + clientSocket.getInetAddress() + " has been disconnected.");
					isOpen = false;
				} else {
					logger.info("Client " + clientSocket.getInetAddress() + " is still connected.");
					byte[] latestMsg = messageHandler.receiveMessage();
					receivedMessage = new CommonMessage(latestMsg);
					receivedMessage = receivedMessage.deserialize();
					action(receivedMessage);
				}
			}
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
		} finally {
			try {
				if (clientSocket != null) {
					clientSocket.close();
				}
				logger.info("Client " + clientSocket.getInetAddress() + " has been disconnected.");
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	/**
	 * Performs a certain action depending on the receivedMessage from the
	 * client. Differentiates messages via the status of the client message: put
	 * or get. It is synchronized since other clients should not change the
	 * cache or storage simultaneously.
	 * 
	 * @param receivedMessage
	 *            The message received from the client
	 */
	private void action(CommonMessage receivedMessage) {
		logger.info("start to do action");
		switch (receivedMessage.getStatus()) {
		case PUT:
			put(receivedMessage.getKey(), receivedMessage.getValue());
			break;
		case GET:
			get(receivedMessage.getKey());
			break;
		case MERGE_UPDATE:
			merge(receivedMessage.getKey(), receivedMessage.getValue());
			break;
		default:
			logger.error("No valid status.");
			break;
		}
		logger.info("finish to do action");
	}

	private void put(String key, String value) {

		CommonMessage sentMessage_temp = new CommonMessage();
		StatusType type = StatusType.PUT_ERROR;

		if (KVServer.state.getState() == State.STOP) {
			sentMessage_temp.setStatusType(StatusType.SERVER_STOPPED);
		} else if (KVServer.state.getState() == State.LOCK) {
			sentMessage_temp.setStatusType(StatusType.SERVER_WRITE_LOCK);
		} else {
			if (isCoordinator("127.0.0.1", "" + serverSocket.getLocalPort(), key)
					|| isReplica("127.0.0.1", "" + serverSocket.getLocalPort(), key)) {
				type = storageManager.put(key, value, serverSocket.getLocalPort());
				// StatusType type = storageManager.put(key, value);
				sentMessage_temp.setStatusType(type);
				logger.info("put finished: " + key + value + type);
			} else {
				sentMessage_temp.setStatusType(StatusType.SERVER_NOT_RESPONSIBLE);
				sentMessage_temp.setMetadata(metadata);
			}
			sentMessage_temp.setKey(key);
			sentMessage_temp.setValue(value);
		}
		CommonMessage sentMessage = sentMessage_temp.serialize();
		messageHandler.sendMessage(sentMessage.getMsg());
		replicationManager.replicate(type, key, value);
		publish(key, value);		
		
	}

	private void merge(String key, String value) {

		CommonMessage sentMessage_temp = new CommonMessage();
		StatusType type = StatusType.PUT_ERROR;

		if (KVServer.state.getState() == State.STOP) {
			sentMessage_temp.setStatusType(StatusType.SERVER_STOPPED);
		} else if (KVServer.state.getState() == State.LOCK) {
			sentMessage_temp.setStatusType(StatusType.SERVER_WRITE_LOCK);
		} else {
			if (isCoordinator("127.0.0.1", "" + serverSocket.getLocalPort(), key)
					|| isReplica("127.0.0.1", "" + serverSocket.getLocalPort(), key)) {
				type = storageManager.updateMerge(key, value, serverSocket.getLocalPort());
				// StatusType type = storageManager.put(key, value);
				sentMessage_temp.setStatusType(type);
			} else {
				sentMessage_temp.setStatusType(StatusType.SERVER_NOT_RESPONSIBLE);
				sentMessage_temp.setMetadata(metadata);
			}
			sentMessage_temp.setKey(key);
			sentMessage_temp.setValue(value);
		}
		CommonMessage sentMessage = sentMessage_temp.serialize();
		messageHandler.sendMessage(sentMessage.getMsg());
		replicationManager.replicate(type, key, value);
		publish(key, value);

	}

	private void get(String key) {
		CommonMessage sentMessage_temp = new CommonMessage();
		String value = null;
		if (KVServer.state.getState() == State.STOP) {
			sentMessage_temp.setStatusType(StatusType.SERVER_STOPPED);
		} else {
			if (isCoordinator("127.0.0.1", "" + serverSocket.getLocalPort(), key)
					|| isReplica("127.0.0.1", "" + serverSocket.getLocalPort(), key)) {
				value = storageManager.get(key);
				if (value == null) {
					sentMessage_temp.setStatusType(StatusType.GET_ERROR);
				} else {
					sentMessage_temp.setStatusType(StatusType.GET_SUCCESS);
				}
			} else {
				sentMessage_temp.setStatusType(StatusType.SERVER_NOT_RESPONSIBLE);
				sentMessage_temp.setMetadata(metadata);
			}
		}

		sentMessage_temp.setKey(key);
		sentMessage_temp.setValue(value);

		CommonMessage sentMessage = sentMessage_temp.serialize();
		messageHandler.sendMessage(sentMessage.getMsg());
	}

	private boolean isCoordinator(String ip, String port, String key) {
		NodeInfo server = metadata.getServerForKey(key);
		return (server.ip).equals(ip) && (server.port).equals(port);
	}

	private boolean isReplica(String ip, String port, String key) {
		NodeInfo server = metadata.getServerForKey(key);
		NodeInfo successor = metadata.getSuccessor(server.hashedkey);
		NodeInfo sesuccessor = metadata.getSuccessor(successor.hashedkey);
		return successor != null && ((sesuccessor.ip.equals(ip) && sesuccessor.port.equals(port))
				|| (successor.ip.equals(ip) && successor.port.equals(port)));
	}

	private void publish(String key, String value) {
//		if (!value.equals("")) {
//			publish("UPDATE", key, value);
//		} else {
//			publish("DELETE", key, value);
//		}

	}

//	private void publish(String type, String key, String value) {
//		
//		if (type.equals("UPDATE")) {
//			KVBrokerMessage sentMessage = new KVBrokerMessage();
//			sentMessage.setStatus(KVBrokerMessage.StatusType.SUBSCRIBTION_UPDATE);
//			sentMessage.setKey(key);
//			sentMessage.setValue(value);
//			brokerMsgHandler.sendMessage(sentMessage.serialize().getMsg());
//			
//		} else if (type.equals("DELETE")) {
//			KVBrokerMessage sentMessage = new KVBrokerMessage();
//			sentMessage.setStatus(KVBrokerMessage.StatusType.SUBSCRIBTION_UPDATE);
//			sentMessage.setKey(key);
//			brokerMsgHandler.sendMessage(sentMessage.serialize().getMsg());
//		}
//		
//	}
}