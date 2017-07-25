package app_kvClient;

import common.messages.CommonMessage;

public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewMessage(CommonMessage msg);
	
	public void handleStatus(SocketStatus status);
}
