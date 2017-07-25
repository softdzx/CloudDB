package testing;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import app_kvEcs.ConsistentHashing;
import app_kvEcs.NodeInfo;
import common.messages.MessageHandler;
import common.messages.KVMessage.StatusType;
import replication.ReplicationManager;
import storage.KeyValue;
import storage.StorageManager;

public class test_rep {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String key = "key";
		String value = "value";
		StorageManager storageManager = StorageManager.getInstance("name", "FIFO", 2);
		StatusType type = null;
		type = storageManager.updateMerge(key, "value", 666);
		type = storageManager.put(key+"1", value, 666);
		type = storageManager.put(key+"2", value, 666);
		type = storageManager.put(key+"3", value, 666);
		type = storageManager.put(key+"4", value, 666);
		
		storageManager.update_replica(key, value + " VECTORCLOCK: [ 667 <> 1 ]");
		String OUT = storageManager.get(key);
		System.out.println(">>>OUT " + OUT);
		
		System.out.println(">>>merge>>> ");

		type = storageManager.updateMerge(key, "value_new", 666);
		String OUT2 = storageManager.get(key);
		System.out.println(">>>OUT2 " + OUT2 + type);

		type = storageManager.put(key+"1", value, 666);
		type = storageManager.put(key+"2", value, 666);
		type = storageManager.put(key+"3", value, 666);
		type = storageManager.put(key+"4", value, 666);
		
//		ReplicationManager Instance = ReplicationManager.getInstance("name", storageManager);
//		Instance.start2();
//		Instance.start3();
	}
	
	private void print(StorageManager storageManager, String from, String to) {
    	ArrayList<KeyValue> kvs = storageManager.get(from, to);
    	for(KeyValue kv : kvs) {
			System.out.println(kv.getKey()+kv.getValue());
    	}
		
	}
	

}
