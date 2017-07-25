package app_kvEcs;

import java.util.Comparator;

public class NodeInfo implements Comparator<NodeInfo>{
	public String ip;
	public String port;
	public String hashedkey;
	public String from;
	public String to;
		
	public NodeInfo(String ip, String port){
		this.ip = ip;
		this.port = port;
	}
	
	public NodeInfo(String ip, String port, String hashedkey, String from, String to){
		this.ip = ip;
		this.port = port;
		this.hashedkey = hashedkey;
		this.from = from;
		this.to = to;
	}
	
	@Override
	public int compare(NodeInfo o1, NodeInfo o2) {
		// TODO Auto-generated method stub
		if(o1.ip.equals(o2.ip) && o1.port.equals(o2.port))
			return 0;
		return o1.hashedkey.compareTo(o2.hashedkey);
	}	
	@Override
	public String toString(){
		return "server:{"+ip +","+ port+"," + from+"," + to+"}";
	}
}
