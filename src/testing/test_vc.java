package testing;

import java.lang.String;
import java.util.ArrayList;

import common.messages.MessageHandler;
import common.messages.KVMessage.StatusType;
import reconciliation.*;
import storage.KeyValue;
import storage.StorageManager;

public class test_vc {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
//		int[] a = {11, 22};
//		
//		VectorClock vClock = new VectorClock(a);
//		vClock.increment(11);
//		vClock.increment(11);
//		vClock.increment(11);
//
//		String temp = vClock.toString();

//		VectorClock vClock1 = new VectorClock();
//		vClock1.increment(839102);
//		vClock1.increment(839102);
//		vClock1.increment(99);
//		String value1 = "ojddsds" + vClock1.toString();
//
//		VectorClock vClock2 = new VectorClock();
//		vClock2.increment(839102);
//		String value2 = "vsiuddnds" + vClock2.toString();
//		
//		VectorClock vClock3 = vClock1.mergeIn(vClock2);
//		String value3 = "fjoidsnjc" + vClock3.toString();
//		
//		
//		VectorClock vClock4 = new VectorClock();
//		vClock4.toVectorClock(value3);
//		String value4 = "fjoidsnjc" + vClock4.toString();
		
//		System.out.println(value1);
//		System.out.println(value2);
//		System.out.println(value3);
//		System.out.println(value4);
//
//		int ans = vClock1.compareTo(vClock2);
//		System.out.println(String.valueOf(ans));
		
//		String str = "123, 456";
//		String [] str_tep = str.split(",");
//		System.out.println(String.valueOf(str_tep[0].length()));
		
		String key = "key";
		String value = "value";
		StorageManager storageManager = StorageManager.getInstance("name", "FIFO", 2);
		StatusType type = null;

		storageManager.put(key, value, 666);
		storageManager.update_replica(key, value + " VECTORCLOCK: [ 667 <> 1 ]");
		storageManager.update_replica(key, value + " VECTORCLOCK: [ 666 <> 2; 667 <> 2 ]");
//		type = storageManager.updateMerge(key, "value", 666);
//		type = storageManager.put(key+"2", value, 666);
//		type = storageManager.put(key+"3", value, 666);
//		type = storageManager.put(key+"4", value, 666);
//		
//		storageManager.update_replica(key, value + " VECTORCLOCK: [ 667 <> 1 ]");
		String OUT = storageManager.get(key);
		System.out.println(">>>OUT " + OUT);
//		
//		System.out.println(">>>merge>>> ");
//
//		type = storageManager.updateMerge(key, "value_new", 666);
//		String OUT2 = storageManager.get(key);
//		System.out.println(">>>OUT2 " + OUT2 + type);
//
//		type = storageManager.put(key+"1", value, 666);
//		type = storageManager.put(key+"2", value, 666);
//		type = storageManager.put(key+"3", value, 666);
//		type = storageManager.put(key+"4", value, 666);
//		
//		String from = "00000000000000000000000000000000";
//		String to = "ffffffffffffffffffffffffffffffff";
//		
//    	ArrayList<KeyValue> kvs = storageManager.get(from, to);
//    	for(KeyValue kv : kvs) {
//			System.out.println(kv.getKey()+kv.getValue());
//    	}



	}

}
