package test_performance;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import app_kvClient.KVClient;
import app_kvServer.KVServer;

public class Performance {

	KVClient clients = null;
	
	/**
	 * Class that can be used for scaled performance measurements.
	 * It requires an existing configuration file for the ECS with
	 * 10 server names.
	 * The constructor of this class sets up the performance testing
	 * framework.
	 * @param clients	Number of clients
	 */
	public Performance(int clients){
		System.out.println("startinit");
		
		//this.clients = new KVClient[clients];
		
			this.clients = new KVClient("localhost", 50002);
			try {
				System.out.println("startconnect");
				
				this.clients.connect();
				System.out.println("connected");
				
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
	
	}
	
	public void finish(){
			this.clients.disconnect();

	}
	
	public void meassurePut()
	{
		long startTime = System.currentTimeMillis();
		System.out.println("start");
		try{
				for(int j = 0; j < 2; j++){
					clients.put(""+j, "Some message");
			}
		} 
		catch(Exception e){
			System.out.println("error");
				
		}

	      long stopTime = System.currentTimeMillis();
	      long elapsedTime = stopTime - startTime;
	      System.out.println(elapsedTime);
	}
	
	public void meassureGet()
	{
		long startTime = System.currentTimeMillis();

		try{
				for(int j = 0; j < 2; j++){
					clients.get(""+j);
			}
		} 
		catch(Exception e){
		}

	      long stopTime = System.currentTimeMillis();
	      long elapsedTime = stopTime - startTime;
	      System.out.println(elapsedTime);
	}
		
	public static void main(String[] args) throws Exception {
		
		KVClient kvClient = new KVClient("localhost", 50000);
		kvClient.connect();
			Performance performance = new Performance(1);
			performance.meassurePut();
//			performance.meassureGet();
			
			performance.finish();
			
			
//		}

	}

}
