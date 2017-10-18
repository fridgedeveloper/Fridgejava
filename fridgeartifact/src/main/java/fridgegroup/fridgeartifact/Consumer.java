package fridgegroup.fridgeartifact;

import com.microsoft.azure.eventhubs.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.time.*;

public class Consumer implements Runnable {

	/** Begin Configuration Changes **/
	
	// Obtain eventhub compatible endpoint and name from Azure portal for IoT Hub under "endpoints --> Events --> Properties".
	// (click on events to open properties blade)
	// Obtain access key and name from Azure portal for IoT Hub under "shared access policies --> service".
	//private static String connStr = "Endpoint={youreventhubcompatibleendpoint};EntityPath={youreventhubcompatiblename};SharedAccessKeyName=iothubowner;SharedAccessKey={youriothubkey}";

	//private static String connString = "Endpoint=sb://iothub-ns-frankenfri-118004-f4527069c8.servicebus.windows.net/;EntityPath=frankenfridge;SharedAccessKeyName=service;SharedAccessKey=Was2bNzRYZNSqtwuhou9BD7jfuyJDTVP7OKFtpk9f10=";
	private static String connString = "Endpoint=sb://iothub-ns-fridgedevi-233824-32d2f720f6.servicebus.windows.net/;EntityPath=fridgedeviothub;SharedAccessKeyName=iothubowner;SharedAccessKey=qaFDXXMs1FN8GaU9bHrtVRUIgbQj6YK9Ge9RLSDS6wE=";
	private static String consumerGroup = "javaapp";
	// Should be equal to number of partition configured for the IoT Hub
	static Integer HubPartitions = 4;
	
	
	
	// Set X to start reading from NOW - (X * 24 hours) in the past. Can be zero to start reading from present.
	private static Integer lookBackDays = 0;

	/** End Configuration Changes **/
	
	PartitionReceiver receiver = null;
	String partitionId = null;

	private static volatile boolean running = true;
	private static boolean printNewLine = true; // used to control new line print for console formatting purposes

	public static void terminate() {
		running = false; // set flag to stop loop on receiver event iterator
							// iterator
	}
	

	
	public static void DisplayConfig() {
		System.out.println(" *** Configuration Settings in Consumer.java ***");
		System.out.println("Connection: " + connString);
		System.out.println("Consumer Group: " + consumerGroup);
		System.out.println("Partitions: " + HubPartitions);
		System.out.println("Look Back Days: " + lookBackDays);
		System.out.println("Logfile: " + Log.logfile);
		System.out.println(" ");
	}

	/* Create Client */
	static EventHubClient createClient() {
		EventHubClient client = null;
		try {
			client = EventHubClient.createFromConnectionStringSync(connString);
			
			
		} catch (Exception e) {
			System.out.println("Failed to create client: " + e.getMessage());
			System.exit(1);
		}
		return client;

	}

	/*
	 * Create Receiver on Client for one Partition starting read from specified
	 * time
	 */

	PartitionReceiver createReceiver(EventHubClient client, final String partitionId) {

		PartitionReceiver receiver = null;

		try {
			// (.join) Returns PartitionReceiver after Completeable future is
			// complete
			// (.minus) subtracts specified duration of time from now
			//receiver = client.createReceiver(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME, partitionId,	Instant.now().minus(Duration.ofDays(lookBackDays))).join();
			receiver = client.createReceiver(consumerGroup, partitionId,	Instant.now().minus(Duration.ofDays(lookBackDays))).join();

			System.out.println("** Created receiver on partition " + partitionId);

		} catch (Exception e) {
			System.out.println("** Failed to create receiver on partition " + partitionId);
			System.out.println("Error: " + e.getMessage());
		}

		return receiver;
	}

	/* Get messages from receiver, passing partitionId for logging purposes */

	static void receiveMessages(PartitionReceiver receiver, String partitionId) {
		String lastOffset = null;
		Long lastSeqNum = null;
		String line;

		try {
			while (running) {
				Iterable<EventData> receivedEvents = receiver.receive(100).get();
				
				
				int batchSize = 0;
				if (receivedEvents != null) {
					for (EventData receivedEvent : receivedEvents) {
					

						line = String.format("Device ID: %s",
								receivedEvent.getProperties().get("iothub-connection-device-id")) +
								
								String.format(" Offset: %s, SeqNo: %s, EnqueueTime: %s",
								receivedEvent.getSystemProperties().getOffset(),
								receivedEvent.getSystemProperties().getSequenceNumber(),
								receivedEvent.getSystemProperties().getEnqueuedTime());
						
						Log.write(line);
						System.out.println(line);
						
						String json = new String(receivedEvent.getBody(), Charset.defaultCharset());
						
						line = String.format("Message Payload: " + json);
								
						//line = String.format("Message Payload: %s",
						//		new String(receivedEvent.getBody(), Charset.defaultCharset()));
						
						Log.write(line);
						System.out.println(line);
						
						EventProcess event = new EventProcess();
						if (json != null) {
							event.Deseralize(json);
						}
						
						batchSize++;
						
						lastOffset = receivedEvent.getSystemProperties().getOffset();
						lastSeqNum = receivedEvent.getSystemProperties().getSequenceNumber();
						
						
						
						
					}
					
					line =	String.format("Partition: %s, ReceivedBatch Size: %s, Last Offset: %s, Last SeqNo: %s",
									partitionId, batchSize, lastOffset, lastSeqNum);
					
					System.out.println(line);
					Log.write(line);
					
				}
			}

			if (printNewLine) { System.out.println(" "); } // print newline only on first pass
			System.out.println("Receiver for partition " + partitionId + " shutdown.");
			printNewLine = false;
			
		}

		catch (Exception e) {
			System.out.println("Consumer Error: " + e.getMessage());
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			System.out.println("StackTrace: " + sw.toString()); // stack trace as a string
		}

	}


	
	public void run() {

		receiveMessages(receiver, partitionId);

	}
}

