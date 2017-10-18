package fridgegroup.fridgeartifact;


/**
 * This App is a consumer/listener for use with Event Hub or IoT Hub intended to run as a console app for diagnostic purposes.
 * It consumes all messages on all partitions and prints to console.
 * 
 * This code can be used as a starting point to create an application consuming from an Event Hub. 
 * 
 * To use:
 * Configure connection information in Consumer.java and package as an executable JAR
 * 
 * @author miimbruc Michael Imbruce
 * Last updated 12/5/16
 */

// https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-java-java-getstarted


import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.iot.service.sdk.ServiceClient;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//import com.microsoft.azure.eventhubs.*;
//import com.microsoft.azure.servicebus.*;
//import iot.mike.hub_consumer.*;

public class App {

	public static void main(String[] args) {

		// Open logfile
		try {
			Log.open();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		// Create thread pool = # partitions
		ExecutorService executor = Executors.newFixedThreadPool(Consumer.HubPartitions);
		

		try {
			
			// Show configuration settings
			Consumer.DisplayConfig();
			Producer.DisplayConfig();
			
			
			
			
			// create a single event hub client connection object shared among all receivers
			EventHubClient client = Consumer.createClient();
			
			// create a single service client object to be shared among all senders
			Producer.serviceClient = Producer.connectServiceClient();
			
			 Thread.sleep(4000);
			 
			// create a receiver object for each partition and execute object in a thread
			for (Integer partitionId=0; partitionId < Consumer.HubPartitions; partitionId++ ) {
				Consumer consumer = new Consumer();	
				PartitionReceiver receiver = consumer.createReceiver(client,partitionId.toString());
				consumer.receiver = receiver;
				//consumer.receiver = new Consumer.createReceiver(client,partitionId.toString());
				consumer.partitionId = partitionId.toString();
				executor.execute(consumer);
			}	
			
		 System.out.println("Press ENTER to exit.");
		 
		
		 
		 System.out.println("");
		 System.in.read(); // blocking read
		 System.out.print("Initiated shutdown, this may take a minute.");

		 Consumer.terminate(); // stop looping

		 Producer.serviceClient.close(); //close service client connection
		 
		 executor.shutdown(); // kill threads
		 
		 
		 // Wait for thread to exit. Use sleep to prevent race condition.
		 while (!executor.isTerminated()) {
			 System.out.print(".");
			 Thread.sleep(2000);
	        }
		
		System.out.println(" "); 
	    System.out.println("*** Shutdown Complete ***");

	        
	        
		 client.closeSync(); // close connections associated with connection object
		 System.exit(0);		
		}
		catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
		
	}

}
