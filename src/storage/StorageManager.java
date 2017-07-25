package storage;

import java.util.ArrayList;

import app_kvEcs.ConsistentHashing;
import common.messages.KVMessage.StatusType;
import reconciliation.ReconciliationManager;
import reconciliation.VectorClock;

public class StorageManager {
	private Cache cache;
	private Persistance persistance;
	private ReconciliationManager recon;

	private static StorageManager storageManager = null;

	public static StorageManager getInstance() {
		return storageManager;
	}

	public static StorageManager getInstance(String name, String strategyName, int cacheSize) {
		if (storageManager == null) {
			synchronized (StorageManager.class) {
				if (storageManager == null) {
					storageManager = new StorageManager(name, strategyName, cacheSize);
				}
			}
		}
		return storageManager;
	}

	private StorageManager(String name, String strategyName, int cacheSize) {
		cache = new Cache(cacheSize, strategyName);
		persistance = new Persistance(name);
		recon = new ReconciliationManager();
	}

	public synchronized ArrayList<KeyValue> get(String from, String to) {
		ArrayList<KeyValue> result = new ArrayList<KeyValue>();
		
		result.addAll(recon.get(from, to));
		result.addAll(cache.get(from, to));
		result.addAll(persistance.get(from, to));

		return result;
	}
	
	public synchronized String get(String key) {
		
		String value = null;
//		System.out.println(">>>value>>> " + value);

		if (recon.containKey(key)) {
			ArrayList<String> values = recon.get(key);
			if (values != null) {
				value = "";
				for (String v : values) {
					value = value + v + " || ";
//					System.out.println(">>>value>>> " + value);
				}
				value = value.substring(0, value.length() - 3);
			}
		} else if (cache.get(key) != null) {
			value = cache.get(key);
		} else if (persistance.get(key) != null) {
			value = persistance.get(key);
		}
		
//		System.out.println(">>>value>>> " + value);
		return value;
	}

	public synchronized void removeData(String from, String to) {
		cache.remove(from, to);
		persistance.remove(from, to);
	}
	
	public synchronized StatusType put(String key, String value, int serverPort) {
//		System.out.println(">>>put>>> ");

		StatusType type = StatusType.PUT_ERROR;

		if (key == null || value == null) {
			return type;
		}

//		if (value.equals("null")) {
//			// delete
//			String toremove = cache.remove(key);
//			if (toremove == null) {
//				toremove = persistance.remove(key);
//			}
//			if (toremove != null) {
//				type = StatusType.DELETE_SUCCESS;
//			} else {
//				type = StatusType.DELETE_ERROR;
//			}
//		} else {
			// put
			if (get(key) != null) {
				// update
				type = update(key, value, serverPort);
			} else {
				// insert
				VectorClock vClock = new VectorClock();
				vClock.increment(serverPort);
				value = value + vClock.toString();
				type = insert(key, value);
			}
//		}
		return type;
	}

	private synchronized StatusType update(String key, String value, int serverPort) {
//		System.out.println(">>>update>>> ");

		StatusType type = StatusType.PUT_ERROR;

		if (this.recon.containKey(key)) {
			// key in recon, conflict
			// put error
			return type;
		} else {
			// key not in recon, no conflict
			String value_pre = get(key);

			VectorClock vClock_pre = new VectorClock();
			vClock_pre.toVectorClock(value_pre);
			VectorClock vClock_new = vClock_pre.getCopy();
			vClock_new.increment(serverPort);

			value = value + vClock_new.toString();

//			System.out.println(">>>value_current>>> " + value);
//			System.out.println(">>>value_pre>>> " + value_pre);
			
			int compare = vClock_new.compareTo(vClock_pre);
			if (compare == 1) {
//				System.out.println(">>>compare == 1>>> ");
				// delete old value
				String toremove = cache.remove(key);
				if (toremove == null) {
					toremove = persistance.remove(key);
				}
//				System.out.println(">>>compare == 1 && delete success>>> ");
				// insert new value
				insert(key, value);
//				 System.out.println("value_current");
				type = StatusType.PUT_SUCCESS;
			} else if (compare == -1||(compare == -0)) {
//				System.out.println(">>>compare == -1>>> ");
				// do nothing
			} else {
//				System.out.println(">>>compare == 100>>> ");
				// delete old value
				String toremove = cache.remove(key);
				if (toremove == null) {
					toremove = persistance.remove(key);
				}
				// conflict, put into recon
				ArrayList<String> values = new ArrayList<String>();
				values.add(value_pre);
				values.add(value);
				recon.put(key, values);
			}
		}
		return type;
	}

	public void update_replica(String key, String value) {

//		System.out.println(">>>update_replica>>>");

		if (get(key) != null) {
//			System.out.println(">>>update_replica>>>key exist>>>");
			// key exist
			// TO DO: reconciliation
			if (recon.containKey(key)) {
//				System.out.println(">>>update_replica>>>key exist>>>in recon>>>");
				// key exist && in recon
				putRecon(key, value);
				System.out.println(">>>update_replica>>>key exist>>>in recon>>>" + recon.get(key).size());
				if(recon.get(key).size() == 1) {
					// delete recon, put the value in database
					recon.remove(key);
					insert(key, value);
				}
			} else {
//				System.out.println(">>>update_replica>>>key exist>>>not in recon>>>");
				// key exist && not in recon
				// need to compare and merge
				String value_new = value;
				String value_pre = get(key);
				VectorClock vClock_pre = new VectorClock();
				vClock_pre.toVectorClock(value_pre);
				VectorClock vClock_new = new VectorClock();
				vClock_new.toVectorClock(value_new);
				/*
				 * isEqual: 0 Equal 1 Greater -1 Smaller 100 Conflict
				 */
				int compare = vClock_new.compareTo(vClock_pre);
				if (compare == 1) {
					// delete old value
//					System.out.println(">>>update_replica>>>key exist>>>not in recon>>>larger>>>");
					String toremove = cache.remove(key);
					if (toremove == null) {
						toremove = persistance.remove(key);
					}
					// insert new value
					insert(key, value_new);
//					 System.out.println("value_current");
				} else if ((compare == -1)||(compare == 0)) {
//					System.out.println(">>>update_replica>>>key exist>>>not in recon>>>smaller or equal>>>");
					// do nothing
				} else {
//					System.out.println(">>>update_replica>>>key exist>>>not in recon>>>conflict>>>");
					// delete old value
					String toremove = cache.remove(key);
					if (toremove == null) {
						toremove = persistance.remove(key);
					}
					// conflict, put into recon
					ArrayList<String> values = new ArrayList<String>();
					values.add(value_pre);
					values.add(value_new);
					recon.put(key, values);
				}
			}
		} else {
			// key not exsit
//			System.out.println(">>>update_replica>>>key not exist>>>insert>>>");
			insert(key, value);
		}
	}

	public synchronized StatusType updateMerge(String key, String value, int serverPort) {
		
		VectorClock vClock = new VectorClock();
		StatusType type = StatusType.PUT_ERROR;
		
		if (get(key) == null) {
			type = put(key, value, serverPort);
		} else {
			if (recon.containKey(key)) {
				// if key in recon
				// recon need to be removed to clean conflict
				ArrayList<String> values = recon.get(key);
				recon.remove(key);

				// merge all things in recon
				for (String v : values) {
//					System.out.println("+++" + v);
					VectorClock vClock_temp = new VectorClock();
					vClock_temp.toVectorClock(v);
					vClock = vClock.mergeIn(vClock_temp);
//					System.out.println("|||" + vClock.toString());
				}
				// increment to ensure it's biggest
				vClock.incrementAll();
				vClock.increment(serverPort);

				value = value + vClock.toString();
//				System.out.println("===" + value);

				// insert new value
				type = insert(key, value);

			} else {
				
//				System.out.println(">>>not in recon>>> ");
				
				// if key not in Recon
				String value_pre = get(key);
				vClock.toVectorClock(value_pre);
				vClock.increment(serverPort);
				// now to delete old things in cache and persistence
				// and insert new thing

				// delete
				String toremove = cache.remove(key);
				if (toremove == null) {
					toremove = persistance.remove(key);
				}

				// insert new value
				value = value + vClock.toString();
				type = insert(key, value);
			}
		}
//		if (type == StatusType.PUT_SUCCESS) type = StatusType.MERGE_SUCCESS;
		return type;
	}

	private synchronized StatusType insert(String key, String value) {
//		System.out.println(">>>insert>>> ");
		StatusType type = StatusType.PUT_ERROR;
		try {
			if (cache.isFull()) {
//				System.out.println(">>>CACHE>>>ISFULL>>>");
				KeyValue toremove = cache.remove();
//				System.out.println(">>>CACHE>>>DELETE>>>" + toremove.getKey() + toremove.getValue());
//				System.out.println(">>>PERSISTENCE>>>PUT>>>" + toremove.getKey() + toremove.getValue());				
				persistance.put(toremove.getKey(), toremove.getValue());
				cache.put(key, value);
			} else {
//				System.out.println(">>>CACHE>>>NOTFULL>>>" + key + value);
				cache.put(key, value);
			}
		} catch (Exception e) {
			type = StatusType.PUT_ERROR;
		}
		type = StatusType.PUT_SUCCESS;
		return type;
	}

	/**
	 * Method call from update_replica
	 * 
	 * @param key
	 * @param value:
	 *            contains vectorClock
	 */
	private synchronized void putRecon(String key, String value) {

		recon.putNewValue(key, value);
		recon.reconciliation(key);

	}

}
