package common.messages;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import app_kvEcs.NodeInfo;

/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */

public class CommonMessage extends Message implements Serializable, KVMessage {
	
	private static final long serialVersionUID = 5549512212003782618L;
	private String msg;
	private byte[] msgBytes;
	
	private String key;
	private String value;
	private StatusType statusType;
	private Metadata metadata;
	
	/**
	 * Constructs a TextMessage with no initial values
	 */
	public  CommonMessage(){
		
	}
	
    /**
     * Constructs a TextMessage object with a given array of bytes that 
     * forms the message.
     * 
     * @param bytes the bytes that form the message in ASCII coding.
     */
	public CommonMessage(byte[] bytes) {
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
     * Constructs a TextMessage object with a given String that
     * forms the message. 
     * 
     * @param msg the String that forms the message.
     */
	public CommonMessage(String msg) {
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
	 * Returns the content of this TextMessage as a String.
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
	
	@Override
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public String getValue() {
		return value;
	}
	
	public void setValue(String value) {
		this.value = value;
	}
	
	public void appendValue(String append) {
		this.value += append;
	}
	
	@Override
	public StatusType getStatus() {
		return statusType;
	}
	
	public void setStatusType(StatusType type) {
		this.statusType = type;
	}

	/**
	 * Serializes the data of the current TextMessage into a String with format
	 * {StatusType: <STATUS_TYPE>, key: <KEY>, value: <VALUE>}
	 * and returns the result in a new TextMessage
	 * @return Serialized String ready for sending to the server.
	 */
	public CommonMessage serialize(){

		switch(statusType){
		case GET:
			return new CommonMessage("{StatusType: GET, key:" + key +"}");
		case GET_ERROR:
			return new CommonMessage("{StatusType: GET_ERROR, key:" + key +"}");
		case GET_SUCCESS:
			return new CommonMessage("{StatusType: GET_SUCCESS, key:" + key + ", value:" + value +"}");
		case PUT:
			return new CommonMessage("{StatusType: PUT, key:" + key + ", value:" + value + "}");
		case PUT_SUCCESS:
			return new CommonMessage("{StatusType: PUT_SUCCESS, key:" + key + ", value:" + value +"}");
		case MERGE_SUCCESS:
			return new CommonMessage("{StatusType: MERGE_SUCCESS, key:" + key + ", value:" + value +"}");
		case PUT_UPDATE:
			return new CommonMessage("{StatusType: PUT_UPDATE, key:" + key + ", value:" + value +"}");
		case PUT_ERROR:
			return new CommonMessage("{StatusType: PUT_ERROR, key:" + key + ", value:" + value +"}");
		case DELETE_SUCCESS:
			return new CommonMessage("{StatusType: DELETE_SUCCESS, key:" + key +"}");
		case DELETE_ERROR:
			return new CommonMessage("{StatusType: DELETE_ERROR, key:" + key +"}");
		case SERVER_STOPPED:
			return new CommonMessage("{StatusType: SERVER_STOPPED, key:" + key + "}");
		case SERVER_WRITE_LOCK:
			return new CommonMessage("{StatusType: SERVER_WRITE_LOCK, key:" + key +"}");
	    case SERVER_NOT_RESPONSIBLE:
			return new CommonMessage("{StatusType: SERVER_NOT_RESPONSIBLE, key:" + key + ", metadata:" + metadata.toString() +"}");
	    case MERGE_UPDATE:
			return new CommonMessage("{StatusType: MERGE_UPDATE, key:" + key + ", value:" + value + "}");    	
		default:
			return new CommonMessage("{StatusType: FAILED}");
		}
	}

	/**
	 * Deserializes a String received from the server. The message
	 * {StatusType: <STATUS_TYPE>, key: <KEY>, value: <VALUE>}
	 * is split into its values and a new TextMessage with these
	 * values is returned.
	 * @return TextMessage with received Status, Key and Value
	 */
	public CommonMessage deserialize(){
		
		CommonMessage demessage = new CommonMessage(msgBytes);
		//here need to be modified
		
		if(msg.charAt(0) != '{' || msg.charAt(msg.length()-1) != '}'){
			return null;
		}
		
		msg = msg.substring(1, msg.length()-1);
		
		String [] pairs = msg.split(",");
		if(pairs.length > 3) {
			pairs = msg.split(",", 3);			
		}
		
		for(String pair : pairs) {
			
			String [] keyvalue= pair.split(":");
			
			if(keyvalue[0].trim().equals("StatusType")) {
				demessage.setStatusType((StatusType.valueOf(keyvalue[1].trim().toUpperCase())));
			} else if(keyvalue[0].trim().equals("key")) {
				demessage.setKey(keyvalue[1].trim());
			} else if(keyvalue[0].trim().equals("value")) {
				if(keyvalue.length > 1) {
					keyvalue= pair.split(":", 2);
					demessage.setValue(keyvalue[1].trim());
				} else {
					demessage.setValue("");
				}
			} else if(keyvalue[0].trim().equals("VECTORCLOCK")) { 
				//demessage.appendValue(keyvalue[1]);
			} else if(keyvalue[0].trim().equals("metadata")) {
				Metadata tempdata = new Metadata();
				if(keyvalue.length > 1){
					String[] datasets = keyvalue[1].split("<");
					String[] serverset;
					for(String dataset: datasets){
						serverset = dataset.substring(1, dataset.length()-1).split(" ");
						//server.ip + server.port + server.hashedkey + server.from + server.to
						if(serverset.length == 5)
						{
							tempdata.add(new NodeInfo(serverset[0], serverset[1], serverset[2], serverset[3], serverset[4]));
						}
					}
					demessage.setMetadata(tempdata);
				}
				
			} else {
				break;
			}
		}
		return demessage;

	}

	public Metadata getMetadata() {
		return metadata;
	}

	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
	}
		
}
