package replication;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import app_kvEcs.ConsistentHashing;
import app_kvEcs.NodeInfo;
import common.messages.MessageHandler;
import common.messages.Metadata;
import common.messages.KVMessage.StatusType;
import common.messages.CommonMessage;
import common.messages.KVMessage;
import storage.KeyValue;
import storage.StorageManager;

public class LazyReplication implements Runnable {
	
	private String name;
    private Metadata metadata;
    private Logger logger;
	private StorageManager storageManager;
    private static ReplicationManager replicationManager = null;
	private MessageHandler [] successors = new MessageHandler[2];

	
	public LazyReplication(ReplicationManager repMan){
	}
	
	@Override
	public void run() {
        Runnable runnable = new Runnable() {
            public void run() {
            	logger.info(">>>REPLICA>>>start to do replica");
            	NodeInfo self = metadata.getServer(ConsistentHashing.getHashedKey(name));
            	String from = self.from;
            	String to = self.to;
            	logger.info(">>>REPLICA>>>from: " + from + ", to: " + to);
            	ArrayList<KeyValue> kvs = storageManager.get(from, to);
            	logger.info(">>>REPLICA>>>kvs: " + kvs);
            	if(kvs != null) {
                	for(KeyValue kv : kvs) {
            			for(MessageHandler successor : successors)
            				sendReplica(successor, kv.getKey(), kv.getValue());
                	}
            	}
            }
          };
    	
        ScheduledExecutorService service = Executors
                .newSingleThreadScheduledExecutor();
        
        //time period
        service.scheduleAtFixedRate(runnable, 0, 10, TimeUnit.SECONDS);
		}
	

    /**
     * update replicas
     */
    public synchronized void sendReplica(final MessageHandler successor, String key, String value){
    		if(successor == null)
    			return;
    		
    		CommonMessage sentMessage_temp = new CommonMessage();
    		sentMessage_temp.setStatusType(StatusType.PUT);
    		sentMessage_temp.setKey(key);
    		sentMessage_temp.setValue(value);    	
    		final CommonMessage sentMessage = sentMessage_temp.serialize();
    		
			Thread messageSender = new Thread() {
				public void run() {
					try {
						Thread.sleep(60000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					successor.sendMessage(sentMessage.getMsg());
				}
			};
			messageSender.start();
    }
    
    /**
     * delete replicas
     */
    public void deletes(MessageHandler successor, String key){
    		if(successor == null)
    			return;

    		CommonMessage sentMessage = new CommonMessage();
    		sentMessage.setStatusType(KVMessage.StatusType.PUT);
    		sentMessage.setKey(key);
    		sentMessage.setValue("null");
    		sentMessage = sentMessage.serialize();
    		successor.sendMessage(sentMessage.getMsg());
    }
    
    /**
     * get two successors
     * @return two successor server, can be null
     */
    private void updateSuccessors(){
    	
    	NodeInfo successor = metadata.getSuccessor(ConsistentHashing.getHashedKey(name));
    	if(successor != null){
    		try {
				Socket successorSocket= new Socket(successor.ip, Integer.parseInt(successor.port)+20);
				successors[0] = new MessageHandler(successorSocket, logger);
//				System.out.println(successor.port);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			}
    		NodeInfo sesuccessor = metadata.getSuccessor(successor.hashedkey);
    		if(!sesuccessor.hashedkey.equals(ConsistentHashing.getHashedKey(name))){
    			try{
    				Socket sesuccessorSocket= new Socket(sesuccessor.ip, Integer.parseInt(sesuccessor.port)+20);
    				successors[1] = new MessageHandler(sesuccessorSocket, logger);
//    				System.out.println(sesuccessor.port);

    			}catch(Exception e){

    			}
    		}

    	}
    }

    public void update(Metadata metadata){
    	this.metadata = metadata;
    	updateSuccessors();
    }
    
    private void updateAll() {
    	
    	//updateSuccessors();
    	
    	NodeInfo self = metadata.getServer(ConsistentHashing.getHashedKey(name));
    	String from = self.from;
    	String to = self.to;
    			
    	ArrayList<KeyValue> kvs = storageManager.get(from, to);
    	for(KeyValue kv : kvs) {
			for(MessageHandler successor : successors)
				sendReplica(successor, kv.getKey(), kv.getValue());
    	}
    	
    }
    
    private void updatePUT(String key) {
		if(successors == null)
			return;

    	//updateSuccessors();
		
    	String kvs = storageManager.get(key);
		for(MessageHandler successor : successors) {
			sendReplica(successor, key, kvs);
		}

    }

	public void close(){
		
	}
}
