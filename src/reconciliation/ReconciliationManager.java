package reconciliation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import app_kvEcs.ConsistentHashing;
import storage.KeyValue;
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

/**
 * @author Han
 *
 */
public class ReconciliationManager extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5933454920471948932L;
	private HashMap<String, ArrayList<String>> keyvalue = new HashMap<String, ArrayList<String>>();

	public ReconciliationManager(){
		
	}
	
	public ArrayList<String> remove(String key){
		return keyvalue.remove(key);
	}
	
	public void put(String key, ArrayList<String> value) {
		if (keyvalue.containsKey(key)) {
			keyvalue.remove(key);
		}
		keyvalue.put(key, value);
	}
	
	public ArrayList<String> get(String key){
		if (keyvalue.containsKey(key))
			return keyvalue.get(key);
		else return null;
	}
	
	public boolean containKey(String key) {
		return keyvalue.containsKey(key);
	}
	
	public void putNewValue(String key, String value) {
		if(!keyvalue.get(key).contains(value)){
			keyvalue.get(key).add(value);
		}
	}
	
	public synchronized ArrayList<KeyValue> get(String from, String to){
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		String value = "";
		for(Entry<String, ArrayList<String>> entry : keyvalue.entrySet()){
			if(inRange(entry.getKey(), from, to)){
				ArrayList<String> values = get(entry.getKey());
				if (values != null) {
					value = "";
					for (String v : values) {
						value = value + v + " || ";
					}
					value = value.substring(0, value.length() - 3);
				}
			}
				results.add(new KeyValue(entry.getKey(), value));
		}
		return results;
	}

	
	
	/**
	 * This is the method to handle the version conflict
	 * within the reconciliationManager
	 */
	public void reconciliation(String key) {

		ArrayList<String> values = keyvalue.get(key);
		
		ArrayList<String> al = new ArrayList<String>();
		al.addAll(values);
		HashSet<String> hs = new HashSet<String>();
		hs.addAll(al);
		al.clear();
		al.addAll(hs);
		
		values = al;
		keyvalue.remove(key);
		keyvalue.put(key, values);
		
		values = keyvalue.get(key);
		String largest = "";
		boolean biggest = false;
		for (String s_choose : values) {
			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(values);
			temp.remove(s_choose);
			biggest = true;
			for (String s_other : temp) {
				VectorClock vClock_choose = new VectorClock();
				VectorClock vClock_other = new VectorClock();
				vClock_choose.toVectorClock(s_choose);
				vClock_other.toVectorClock(s_other);
				if (vClock_choose.compareTo(vClock_other) != 1) {
					biggest = false;
				}
			}
			if (biggest == true) {
				largest = s_choose;
				break;				
			}
		}
		
		if(biggest == true) {
			ArrayList<String> newlist = new ArrayList<String>();
			newlist.add(largest);
			keyvalue.remove(key);
			keyvalue.put(key, newlist);
		}
		
	}
		
	protected boolean inRange(String key, String from, String to){
		if(from.compareTo(to) > 0){
			if(ConsistentHashing.getHashedKey(key).compareTo(from) > 0 || 
					ConsistentHashing.getHashedKey(key).compareTo(to) < 0){
				return true;
			}
		}else{
			if(ConsistentHashing.getHashedKey(key).compareTo(from) > 0 && 
					ConsistentHashing.getHashedKey(key).compareTo(to) < 0){
				return true;
			}
		}
		return false;
	}
	

}
