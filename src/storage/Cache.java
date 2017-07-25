package storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import strategy.Strategy;
import strategy.StrategyFactory;

class CacheFullException extends Exception{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public CacheFullException(){
		super();
	}
	public CacheFullException(String message){
		 super(message); 
	}
}

public class Cache extends Storage{
	private LinkedHashMap<String, String> keyvalue = new LinkedHashMap<String, String>();
	private Strategy strategy;
	private int size;

	public Cache(int size, String strategyName){
		this.size = size;
		strategy = StrategyFactory.getStrategy(strategyName);
	}
	
	public synchronized boolean isFull(){
		boolean full = true;
		if (keyvalue.size() < size)
			full = false;
		return full;
	}

	public synchronized String remove(String key){
		return keyvalue.remove(key);
	}
	
	public synchronized void put(String key, String value) throws CacheFullException{
		if(isFull()){
			throw new CacheFullException();			
		} else {
			keyvalue.put(key, value);
			strategy.add(key);			
		}
	}
	
	public synchronized String get(String key){
		strategy.update(key);
		return keyvalue.get(key);
	}

	public synchronized ArrayList<KeyValue> get(String from, String to){
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		for(Entry<String, String> entry : keyvalue.entrySet()){
			if(inRange(entry.getKey(), from, to))
				results.add(new KeyValue(entry.getKey(), entry.getValue()));
		}
		return results;
	}
	
	public synchronized void remove(String from, String to){
		Iterator<String> it = keyvalue.keySet().iterator();
		while(it.hasNext()){
			String key = it.next();
			if(inRange(key, from, to))
				it.remove();
		}
	}
	
	public synchronized KeyValue remove(){
		String key = keyvalue.keySet().iterator().next();
		String value = keyvalue.remove(key);
		return new KeyValue(key, value);
	}
}