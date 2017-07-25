package reconciliation;

import java.util.HashMap;

public class VectorClock implements Comparable<VectorClock> {

	private HashMap<Integer, Integer> clock = new HashMap<Integer, Integer>();
	
	public VectorClock() {
	}
	
	public VectorClock(int[] a) {
		int NumElems = a.length;
		for (int i = 0; i < NumElems; i++) {
			increment(a[i]);
		}
	}
	
	public void increment(int ID) {
		if (clock.containsKey(ID))
			clock.put(ID, clock.get(ID) + 1);
		else
			clock.put(ID, 1);
	}
	
	public void incrementAll() {
		for (Integer i : clock.keySet()) {
			increment(i);
		}
	}
	
	public void setValue(int ID, int value) {
		clock.put(ID, value);
	}
	
	public int getValue(int ID) {
		int temp = 0;
		if(clock.containsKey(ID)) {
			temp = clock.get(ID);
		}
		return temp;
	}

	public VectorClock mergeIn(VectorClock v) {
		
		VectorClock tmp = this.getCopy();
		
		for (Integer i : v.clock.keySet()) {
			if (!tmp.clock.containsKey(i) || tmp.clock.get(i) < v.clock.get(i)) {
				tmp.clock.put(i, v.clock.get(i));
			}
		}
		
		return tmp;
	}
	
	public VectorClock getCopy() {
		VectorClock retval = new VectorClock();
		for (int i : clock.keySet()) {
			retval.setValue(i, clock.get(i).intValue());
		}
		return retval;
	}
	
	public String toString() {
		StringBuffer retval = new StringBuffer();
		retval.append(" VECTORCLOCK : [ ");
		for (int i : clock.keySet()) {
			retval.append( i + " <> " + clock.get(i) + "; ");
		}
		retval.delete(retval.length()-2, retval.length()-1);
		retval.append(" ]");
		return new String(retval);
	}
	
	/**
	 * @param s:  value + VECTORCLOCK [ PORT : VC, ... ]
	 * @return
	 */
	public void toVectorClock(String s) {

		String temp = s.trim().split(":")[1].trim();	
		//delete "[" and "]"
		String [] clocks = temp.substring(1, temp.length()-1).split(";");
		
		for( String clock : clocks) {
			String s_port = clock.split("<>")[0].trim();
			String s_vc = clock.split("<>")[1].trim();

			int port = Integer.valueOf(s_port);
			int vc = Integer.valueOf(s_vc);
			this.clock.put(port, vc);
		}
		
	}

	public int compareTo(VectorClock v) {

		boolean isEqual = true;
		boolean isGreater = true;
		boolean isSmaller = true;

		for (Integer i : clock.keySet()) {
			if (v.clock.containsKey(i)) {
				if (clock.get(i) < v.clock.get(i)) {
					isEqual = false;
					isGreater = false;
				} else if (clock.get(i) > v.clock.get(i)) {
					isEqual = false;
					isSmaller = false;
				}
			} else {
				isEqual = false;
				isSmaller = false;
			}
		}

		for (Integer i : v.clock.keySet()) {
			if (!clock.containsKey(i)) {
				isEqual = false;
				isGreater = false;
			}
		}

		// Return based on determined information.
		if (isEqual) {
			return 0;
		} else if (isGreater && !isSmaller && !isEqual) {
			return 1;
		} else if (isSmaller && !isGreater && !isEqual) {
			return -1;
		} else {
//			System.out.println("Conflict");
			return 100;
		}
	}
}