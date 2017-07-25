package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.CommonMessage;
import common.messages.KVMessage.StatusType;
import logger.LogSetup;

public class app_KVClient implements ClientSocketListener{
	
	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "EchoClient> ";
	private BufferedReader stdin;
	private KVClient client = null;
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;
	
	public void run() {
		while(!stop) {
			
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				if(cmdLine != null)
					this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
				logger.error(e.getMessage());
			}
		}
	}
	
	/**
	 * The handleCommand methods handles all possible commands entered into the
	 * command line. The put and get request is passed to the KVStore-Library, which
	 * handles serialization and sending of the message.	
	 * @param cmdLine The latest command line input
	 */
	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+", 3);

		if(tokens[0].equals("quit")) {	
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");
		
		} 
		else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverAddress, serverPort);
				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				}
				System.out.print(PROMPT);
				System.out.println("Connected to: " + serverAddress + ":" + serverPort);
			} else {
				printError("Invalid number of parameters!");
			}
			
		} 
		else  if (tokens[0].equals("put")) {
			if(tokens.length >= 2) {
				if(client != null && client.isRunning()){
					CommonMessage msg = new CommonMessage();
					try {
						if(tokens.length == 2){
							msg = client.put(tokens[1], "");
							if (msg.getStatus()==StatusType.PUT_SUCCESS) {
								msg.setStatusType(StatusType.DELETE_SUCCESS);
							} else {
								msg.setStatusType(StatusType.DELETE_ERROR);								
							}
						} else {
							msg = client.put(tokens[1], tokens[2]);
						}
					} catch (Exception e) {
						printError("Error while sending put request to server!");
						logger.error(e.getMessage());
					}
					System.out.print(PROMPT);
					System.out.println(msg.getStatus());
					
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No message passed!");
			}
			
		} else  if (tokens[0].equals("merge")) {
			if(tokens.length >= 2) {
				if(client != null && client.isRunning()){
					CommonMessage msg = new CommonMessage();
					try {
						if(tokens.length == 2){
							msg = client.merge(tokens[1], "");
							if (msg.getStatus()==StatusType.PUT_SUCCESS) {
								msg.setStatusType(StatusType.DELETE_SUCCESS);
							} else {
								msg.setStatusType(StatusType.DELETE_ERROR);								
							}
						} else {
							msg = client.merge(tokens[1], tokens[2]);
						}
					} catch (Exception e) {
						printError("Error while sending put request to server!");
						logger.error(e.getMessage());
					}
					if (msg.getStatus() == StatusType.PUT_SUCCESS) msg.setStatusType(StatusType.MERGE_SUCCESS);
					System.out.print(PROMPT);
					System.out.println(msg.getStatus());					
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No message passed!");
			}
			
		}else if(tokens[0].equals("get")){
			if(tokens.length >= 2){
				if(client != null && client.isRunning()){
					CommonMessage msg = new CommonMessage();
					try {
						msg = client.get(tokens[1]);
					} catch (Exception e) {
						printError("Error while sending get request to server!");
						logger.error(e.getMessage());
					}
					if (msg.getStatus()==StatusType.GET_SUCCESS) {
						String[] vcss = msg.getValue().split("\\|\\|");
						for (String vcs : vcss) {
							String[] vc = vcs.split("VECTORCLOCK :");
							System.out.print(PROMPT);
							System.out.print("value: " + vc[0].trim() + "; vectorClock: " + vc[1].trim() + "\n");
						}
					} else {
						System.out.print(PROMPT);
						System.out.println(msg.getStatus());						
					}
				}
			} else {
				printError("no key passed!");
			}
			
		} else if(tokens[0].equals("disconnect")) {
			disconnect();
			
		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("subscribe")) {
			if(tokens.length == 3){
				if(tokens[1].equals("CHANGE") || tokens[1].equals("DELETE")){
					try {
						if(client != null){
							client.subscribe(tokens[1], tokens[2]);
						} else {
							printError("You're not connected to the system! Use 'connect <IP> <PORT>'");
						}
					} catch (Exception e) {
						printError("Error while subscribing!");
						logger.error(e.getMessage());
					}
				}
				else{
					printError("Invalid parameter! Usage: subscribe CHANGE|DELETE <key>");
				}
			} else {
				printError("Invalid number of parameters!");
			}
		} else if(tokens[0].equals("unsubscribe")) {
			if(tokens.length == 3){
				if(tokens[1].equals("CHANGE") || tokens[1].equals("DELETE")){
					try {
						client.unsubscribe(tokens[1], tokens[2]);
					} catch (Exception e) {
						printError("Error while unsubscribing!");
						logger.error(e.getMessage());
					}
				} else{
					printError("Invalid parameter! Usage: unsubscribe CHANGE|DELETE <key>");
				}
			}
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}
	
	/**
	 * Calls the connect method of KVStore
	 * @param address IPV4 address of the server
	 * @param port port that the server listens on
	 * @throws UnknownHostException IP address not valid/reachable
	 * @throws IOException
	 */
	private void connect(String address, int port) {
		client = new KVClient(address, port);
		client.addListener(this);

		try {
			client.connect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(e.getMessage());
			return;
		}
	}
	
	/**
	 * Calls the disconnect method of KVStore
	 */
	private void disconnect() {
		if(client != null) {
			client.disconnect();
			client = null;
			System.out.print(PROMPT);
			System.out.println("Disconnected");
		}
	}
	
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t put a keyvalue data to the server \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t get a keyvalue data from the server \n");
		sb.append(PROMPT).append("merge <key> <value>");
		sb.append("\t\t merge a keyvalue data to the server \n");
		sb.append(PROMPT).append("subscribe CHANGE|DELETE <key>");
		sb.append("\t\t\t subscribes to change/delete of <key>");
		sb.append(PROMPT).append("unsubscribe CHANGE|DELETE <key>");
		sb.append("\t\t\t unsubscribes to change/delete of <key>");

		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");		

		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}
	
	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}
	
	/**
	 * Handles incoming messages on the client. Prints msg
	 * after the PROMT String.
	 */
	@Override
	public void handleNewMessage(CommonMessage msg) {
//		if(!stop) {
//			System.out.print(PROMPT);
//			System.out.println(msg.getMsg());
//		}
	}
	
	/**
	 * Handles the socket status if it is disconnected or
	 * connection lost. Informs the user about it.
	 */
	@Override
	public void handleStatus(SocketStatus status) {
		if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);
			
		} 
		else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
			System.out.print(PROMPT);
		}
		
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}
	
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.OFF);
			app_KVClient app = new app_KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }

}
