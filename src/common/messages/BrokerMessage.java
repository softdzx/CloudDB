package common.messages;

import java.io.UnsupportedEncodingException;

/**
 * Message format that is used for interaction between the BrokerService and
 * the publishers/subscribers. 
 */

public class BrokerMessage extends Message {

	public enum StatusType {
    	SUBSCRIBE_CHANGE,       		/* Clients wants to subscribe to change of <key> */
    	UNSUBSCRIBE_CHANGE,     		/* Client's subscription for the change of <key> is cancelled */
    	SUBSCRIBE_DELETE,       		/* Clients wants to subscribe to deletion of <key> */
    	UNSUBSCRIBE_DELETE,     		/* Client's subscription for the deletion of <key> is cancelled, e.g. <key> was deleted */
    	
    	SUBSCRIBE_CHANGE_CONFIRM,      /* Server informs client about successful subscribing */
    	UNSUBSCRIBE_CHANGE_CONFIRM,    /* Server informs client about successful unsubscribing */
    	SUBSCRIBE_DELETE_CONFIRM,      /* Server informs client about successful subscribing */
    	UNSUBSCRIBE_DELETE_CONFIRM,    /* Server informs client about successful unsubscribing */
    	
    	SUBSCRIBTION_ALREADY_EXISTS,    /* Subscription already exists and could not be created */
    	SUBSCRIBTION_DOES_NOT_EXIST,    /* Delete failed because the subscription does not exist */
    	INVALID_SUBSCRIPTION_TARGET,	/* Key does not exist in the system */
    	
    	SUBSCRIBTION_UPDATE,    		/* Server send's the updated subscribed item to the client */
    	SUBSCRIBTION_DELETE     		/* Server informs client about deleted item */

  }
	
	private String msg;
	private byte[] msgBytes;
	private String key;
	private String value;
	private StatusType statusType;
	/**
	 * Constructs a KVPubSubMessage with no initial values
	 */
	public BrokerMessage(){
		
	}
	
    /**
     * Constructs a KVPubSubMessage object with a given array of bytes that 
     * forms the message.
     * 
     * @param bytes the bytes that form the message in ASCII coding.
     */
	public BrokerMessage(byte[] bytes) {
		if (bytes != null) {
			this.msgBytes = bytes;
			try {
				this.msg = new String(msgBytes, "UTF-8").trim();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
	}
	
	/**
     * Constructs a KVPubSubMessage object with a given String that
     * forms the message. 
     * 
     * @param msg the String that forms the message.
     */
	public BrokerMessage(String msg) {
		if(msg != null) {
			this.msg = msg.trim();
			try {
				this.msgBytes = msg.getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}


	/**
	 * Returns the content of this KVPubSubMessage as a String.
	 * 
	 * @return the content of this message in String format.
	 */
	public String getMsg() {
		return msg;
	}

	/**
	 * Returns an array of bytes that represent the ASCII coded message content.
	 * 
	 * @return the content of this message as an array of bytes 
	 * 		in ASCII coding.
	 */
	public byte[] getMsgBytes() {
		return msgBytes;
	}
	
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public StatusType getStatus() {
		return statusType;
	}
	
	public void setStatus(StatusType type) {
		this.statusType = type;
	}

	/**
	 * Serializes the data of the current TextMessage into a String with format
	 * {StatusType: <STATUS_TYPE>, key: <KEY>, username: <USER>}
	 * and returns the result in a new TextMessage
	 * @return Serialized String ready for sending to the server.
	 */
	public BrokerMessage serialize(){

		switch(statusType){
		case SUBSCRIBE_CHANGE:
	    	return new BrokerMessage("{StatusType: SUBSCRIBE_CHANGE, key:" + key +"}");
	    case SUBSCRIBE_DELETE:
	    	return new BrokerMessage("{StatusType: SUBSCRIBE_DELETE, key:" +"}");
	    case UNSUBSCRIBE_CHANGE:
	    	return new BrokerMessage("{StatusType: UNSUBSCRIBE_CHANGE, key:" + key +"}");
	    case UNSUBSCRIBE_DELETE:
	    	return new BrokerMessage("{StatusType: UNSUBSCRIBE_DELETE, key:" + key +"}");
	    case SUBSCRIBE_CHANGE_CONFIRM:
	    	return new BrokerMessage("{StatusType: SUBSCRIBE_CHANGE_CONFIRM, key:" + key +"}");
	    case UNSUBSCRIBE_CHANGE_CONFIRM:
	    	return new BrokerMessage("{StatusType: UNSUBSCRIBE_CHANGE_CONFIRM, key:" + key +"}");
	    case SUBSCRIBE_DELETE_CONFIRM:
	    	return new BrokerMessage("{StatusType: SUBSCRIBE_DELETE_CONFIRM, key:" + key +"}");
	    case UNSUBSCRIBE_DELETE_CONFIRM:
	    	return new BrokerMessage("{StatusType: UNSUBSCRIBE_DELETE_CONFIRM, key:" + key +"}");
	    case SUBSCRIBTION_ALREADY_EXISTS:
	    	return new BrokerMessage("{StatusType: SUBSCRIBTION_ALREADY_EXISTS, key:" + key +"}");
	    case SUBSCRIBTION_DOES_NOT_EXIST:
	    	return new BrokerMessage("{StatusType: SUBSCRIBTION_DOES_NOT_EXIST, key:" + key +"}");
	    case INVALID_SUBSCRIPTION_TARGET:
	    	return new BrokerMessage("{StatusType: INVALID_SUBSCRIPTION_TARGET, key:" + key +"}");
	    case SUBSCRIBTION_UPDATE:
	    	return new BrokerMessage("{StatusType: SUBSCRIBTION_UPDATE, key:" + key + ", value:" + value +"}");
	    case SUBSCRIBTION_DELETE:
	    	return new BrokerMessage("{StatusType: SUBSCRIBTION_DELETE, key:" + key +"}");
	    default:
			return new BrokerMessage("{StatusType: FAILED}");
		}
	}

	/**
	 * Deserializes a String received from the server. The message
	 * {StatusType: <STATUS_TYPE>, key: <KEY>, username: <USER>}
	 * is split into its values and a new TextMessage with these
	 * values is returned.
	 * @return TextMessage with received Status, Key
	 */
	public BrokerMessage deserialize(){
		BrokerMessage demessage = new BrokerMessage(msgBytes);

		if(msg.charAt(0) != '{' || msg.charAt(msg.length()-1) != '}'){
			return null;
		}
		msg = msg.substring(1, msg.length()-1);
		String [] pairs = msg.split(",", 3);
		for(String pair : pairs){
			
			String [] keyvalue= pair.split(":");
			
			if(keyvalue[0].trim().equals("StatusType")){
				demessage.setStatus((StatusType.valueOf(keyvalue[1].trim().toUpperCase())));
			}
			else if(keyvalue[0].trim().equals("key")){
				demessage.setKey(keyvalue[1].trim());
			}
			else if(keyvalue[0].trim().equals("value")){
				demessage.setValue(keyvalue[1].trim());
			}
		}
		return demessage;

	}


}
