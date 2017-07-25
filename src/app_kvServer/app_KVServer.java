package app_kvServer;

import java.io.IOException;

import org.apache.log4j.Level;

import logger.LogSetup;

public class app_KVServer extends KVServer{
	
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
    		new LogSetup("logs/server.log", Level.ALL);
    		app_KVServer app_kvserver = new app_KVServer();
			app_kvserver.run(Integer.parseInt(args[0]));
//			app_kvserver.run(50000);			
		} catch (NumberFormatException | IOException nfe) {
			System.out.println("Error! Invalid argument <port> or <cacheSize>! Not a number!");
			System.out.println("Usage: Server <port> <cacheSize> <strategy>!");
			System.exit(1);
		}
    }
	
}
