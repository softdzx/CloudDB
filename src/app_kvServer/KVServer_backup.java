//package app_kvServer;
//
//import java.io.IOException;
//import java.net.BindException;
//import java.net.ServerSocket;
//import java.net.Socket;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//
//import app_kvEcs.Metadata;
//import app_kvEcs.NodeInfo;
//import app_kvServer.ServerState.State;
//import common.messages.EcsMessage;
//import common.messages.EcsMessage.StatusType;
//import common.messages.MessageHandler;
//import logger.LogSetup;
//import replication.LazyReplication;
//import replication.RepConnection;
//import replication.ReplicationManager;
//import storage.KeyValue;
//import storage.StorageManager;
//
//class ServerState{
//	enum State {
//		RUNNING,
//		SHUTDOWN,
//		LOCK,
//		STOP
//	}
//	private State state;
//	public ServerState(){
//		state = State.STOP;
//	}
//	public void setState(State state){
//		this.state = state;
//	}
//	public State getState(){
//		return this.state;
//	}
//}
//
//public class KVServer_backup{
//	
//	private static Logger logger = Logger.getRootLogger();
//	private int port;
//	
//	//serve clients
//    private ServerSocket serverSocket;
//    public static volatile ServerState state = new ServerState();
//    
//    //connect to ECS
//    private Socket clientSocket;
//	private MessageHandler ecsMsgHandler;
//    private Metadata metadata;
//    private RepConnection repConnection;
//	private StorageManager storageManager;
//	private ReplicationManager replicationManager;
//	private FailureDetector failureDetector;
//	
//    /**
//	 * Start KV Server at given port
//	 * @param port given port for storage server to operate
//	 * @param cacheSize specifies how many key-value pairs the server is allowed 
//	 *           to keep in-memory
//	 * @param strategy specifies the cache replacement strategy in case the cache 
//	 *           is full and there is a GET- or PUT-request on a key that is 
//	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
//	 *           and "LFU".
//	 */
//
//	
//    /**
//     * Initializes and starts the server. 
//     * Loops until the the server should be closed.
//     */
//    public void run(int port) {
//    	
//    	this.port = port;
//    	try {
//			initializeServer();
//		} catch (Exception e2) {
//			// TODO Auto-generated catch block
//			return;
//		}
//    	
//		Thread messageReceiver= new Thread(){
//			public void run(){
//				communicateECS();
//			}
//		};
//		
//		messageReceiver.start();
//
//        while(state.getState() != State.SHUTDOWN){
//			try {
//				synchronized(state){
//					while(state.getState() != State.RUNNING)
//						state.wait();
//	            	logger.info("start to serve");
//				}
//			}catch (InterruptedException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//
//			try {
//				Socket client = serverSocket.accept();
//               	ClientConnection connection = 
//                		new ClientConnection(client, serverSocket, metadata);
//               (new Thread(connection)).start();
//                
//                logger.info("Connected to " 
//                		+ client.getInetAddress().getHostName() 
//                		+  " on port " + client.getPort());
//            } catch (IOException e) {
//            	logger.error("Error! " +
//            			"Unable to establish connection. \n", e);
//            }
//        }
//                
//    }
//    	
//	private void communicateECS(){
//		EcsMessage message = null;
//		while(state.getState() != State.SHUTDOWN){
//			try {
//				byte [] b =ecsMsgHandler.receiveMessage();
//				
//				message = new EcsMessage(b);
//				message = message.deserialize(new String(b, "UTF-8"));
//			}catch (IOException e) {
//				// TODO Auto-generated catch block
//				shutDown();
//				logger.error(e);
//
//				return;
//			}
//
//			switch(message.getStatusType()){
//			case INIT:{
//				Metadata meta = message.getMetadata();
//				int size = message.getCacheSize();
//				String strategyName = message.getDisplacementStrategy();
//				initKVServer(meta, size, strategyName);
//				break;
//			}
//			case MOVE:{
//				String from = message.getFrom();
//				String to = message.getTo();
//				String ip = message.getIp();
//				int port = message.getPort();
//				
//				try {
//					moveData(from, to, ip, port);
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				break;
//			}
//			case RECEIVE:
//				try {
//					receiveData();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				break;
//			case REMOVE:{
//				String from = message.getFrom();
//				String to = message.getTo();
//				storageManager.removeData(from, to);
//				break;
//			}
//			case SHUTDOWN:
//				shutDown();
//				break;
//			case START:
//				start();
//				break;
//			case STOP:
//				stop();
//				break;
//			case UPDATE:{
//				Metadata meta = message.getMetadata();
//				update(meta);
//				break;
//			}
//			case WRITELOCK:{
//				if(state.getState() != State.LOCK){
//					state.setState(State.LOCK);
//				}else{
//					state.setState(State.RUNNING);
//				}
//				break;
//			}
//			default:
//				break;
//			}								
//		}
//	}
//    
//    private void initializeServer() throws Exception {
//    	logger.info("Initialize server ...");
//    	
//    	try {
//    		while(clientSocket == null){
//	    		try{
//	    			clientSocket = new Socket("127.0.0.1", 40000);
//	    		}catch(Exception e){
//
//	    		}
//    		}
//    		
//			ecsMsgHandler = new MessageHandler(clientSocket, logger);
//			
//			EcsMessage msg = new EcsMessage();
//			msg.setStatusType(StatusType.RECEIVED);
//			msg.setPort(port);
//			
//			ecsMsgHandler.sendMessage(msg.serialize().getMsg());
//						
//		} catch (Exception e) {
//			logger.error(e.getMessage());
//			throw new Exception();
//		}
//    }
//    
//    /**
//     * Initialize the KVServer with the meta­data, it’s local cache size, 
//		and the cache displacement strategy, and block it for client 
//		requests, i.e., all client requests are rejected with an 
//		SERVER_STOPPED error message; ECS requests have to be 
//		processed. 
//     */
//    
//    public void initKVServer(Metadata metadata, int cacheSize, String displacementStrategy){
//    	setMetadata(metadata);
//
//		try {
//			failureDetector = new FailureDetector("127.0.0.1", port);
//			for(NodeInfo server :metadata.getServers().values()){
//				if(server.ip.equals("127.0.0.1") && server.port.equals(""+port))
//					continue;
//				failureDetector.add(server.ip, Integer.parseInt(server.port));
//			}
//
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			logger.error("failure detector cannot be initialized");
//		}
//				
//		storageManager = StorageManager.getInstance("" + port, displacementStrategy, cacheSize);
//		replicationManager = ReplicationManager.getInstance("127.0.0.1"+port, metadata, logger);
//		
//		repConnection = new RepConnection(port, storageManager, logger);
//		Thread thread = new Thread(repConnection);
//		thread.start();
//
//    }
//
//    /**
//     *Starts the KVServer, all client requests and all ECS requests are 
//	 *processed.  
//     */
//
//    public void start(){
//    	
//    	try {
//            serverSocket = new ServerSocket(port);
//            logger.info("Server listening on port: " 
//            		+ serverSocket.getLocalPort());
//        } catch (IOException e) {
//        	logger.error("Error! Cannot open server socket:");
//            if(e instanceof BindException){
//            	logger.error("Port " + port + " is already bound!");
//            }
//        }
//
//    	replicationManager.update(this.metadata);
//    	 
//    	synchronized(state){
//			state.setState(State.RUNNING);
//    		state.notifyAll();
//    	}
//
//		if(!failureDetector.isAlive())
//			failureDetector.start();
//
//    	logger.info("The server is started");
//    }
//
//    /**
//     * Stops the KVServer, all client requests are rejected and only 
//		ECS requests are processed.
//     */
//    
//    public void stop(){
//		state.setState(State.STOP);
//    	logger.info("The server is stopped");
//    	try {
//			serverSocket.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    }
//    
//    /**
//     * Exits the KVServer application. 
//     */
//    
//    public void shutDown(){
//		state.setState(State.SHUTDOWN);
//    	try {
//			serverSocket.close();
//			clientSocket.close();
//			repConnection.close();
//			failureDetector.terminate();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    	logger.info("The server is shutdown");
//    }
//
//    /**
//     *Lock the KVServer for write operations. 
//     */
//    public void lockWrite(){
//		state.setState(State.LOCK);
//    	logger.info("The server is locked");
//    }
//    
//    /**
//     * Unlock the KVServer for write operations. 
//     */
//    
//    public void unLockWrite(){
//		state.setState(State.RUNNING);
//    	logger.info("The server is unlocked");
//    }
//    
///*    /**
//     * Transfer a subset (range) of the KVServer's data to another 
//		KVServer (reallocation before removing this server or adding a 
//		new KVServer to the ring); send a notification to the ECS, if data 
//		transfer is completed. 
//     */
//
//    
//    private void moveData(String from, String to, String ip, int port) throws UnknownHostException, IOException{    	
//
//    	EcsMessage dataMessage = new EcsMessage();
//		dataMessage.setStatusType(StatusType.DATA);
//		
//		Socket moveSender = new Socket(ip, port);
//				
//		MessageHandler senderHandler = new MessageHandler(moveSender, logger);
//		
//		String data = "";
//
//		int count = 0;
//		
//		ArrayList<KeyValue> toMove = storageManager.get(from, to);
//		for(KeyValue pair : toMove){
//			data += (pair.getKey() + " " + pair.getValue() + ":");
//			count++;
//		}
//
//		dataMessage.setData(data);
//
//		senderHandler.sendMessage(dataMessage.serialize().getMsg());
//							
//		moveSender.close();
//
//		EcsMessage moveFinished = new EcsMessage();
//		moveFinished.setStatusType(StatusType.MOVEFINISH);
//		ecsMsgHandler.sendMessage(moveFinished.serialize().getMsg());
//		
//    	logger.info(count + " keyvalue pair are transferred");
//    }
//    
//    private void receiveData() throws IOException{
//    	
//    	ServerSocket serverMove = new ServerSocket(port-20);
//
//		EcsMessage preparedMessage = new EcsMessage();
//		preparedMessage.setStatusType(StatusType.PREPARED);
//		ecsMsgHandler.sendMessage(preparedMessage.serialize().getMsg());
//		
//		Socket clientMove = serverMove.accept();
//		
//		MessageHandler receiverHandler = new MessageHandler(clientMove, logger);
//		byte [] datab = receiverHandler.receiveMessage();
//
//		EcsMessage receivedData = new EcsMessage(datab);
//		receivedData = receivedData.deserialize(receivedData.getMsg());
//				
//		if(receivedData.getStatusType() == StatusType.DATA){
//			String datamsg = receivedData.getData();
//
//			if(datamsg.contains(":")){
//				String [] pairs = datamsg.split(":");
//	
//				for(String pair : pairs){
//					String[] kvpair = pair.split(" ");
//					if(kvpair.length == 2){
//						storageManager.put(kvpair[0], kvpair[1], this.port);
//					}
//				}
//			   	logger.info(pairs.length + " key value pairs are received");
//			}else{
//			   	logger.info("0 key value pairs are received");
//				
//			}
//		}else{
//			logger.error("Format of received data is not correct");
//		}
//
//		serverMove.close();
//		clientMove.close();		
//    }
//        
//    /**
//     * Update the metadata repository of this server 
//     */
//    
//    public void update(Metadata metadata){
//    	this.setMetadata(metadata);
//    	replicationManager.update(this.metadata);
//    	
//    	for(NodeInfo server : this.metadata.getServers().values()){
//    		if(!metadata.getServers().values().contains(server)){
//    			failureDetector.remove(server.ip, Integer.parseInt(server.port));
//    		}
//    	}
//    	
//    	
//		logger.info("metadata is updated: " + metadata);
//    }
//            
//	public void setMetadata(Metadata metadata) {
//		this.metadata = metadata;
//	}
//	
//	public int getID() {
//		return this.port;
//	}
//	
//}
