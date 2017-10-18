package fridgegroup.fridgeartifact;

import com.microsoft.azure.iot.service.sdk.*;

import fridgegroup.fridgeartifact.EventProcess.CommandMessage;

import java.io.IOException;
import java.net.URISyntaxException;

public class Producer {

	private static final String connectionString = "HostName=fridgedeviothub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=qaFDXXMs1FN8GaU9bHrtVRUIgbQj6YK9Ge9RLSDS6wE=";
	
	
	private static final IotHubServiceClientProtocol protocol = IotHubServiceClientProtocol.AMQPS;
	public static ServiceClient serviceClient;

	static ServiceClient connectServiceClient() throws Exception {
		ServiceClient serviceClient = ServiceClient.createFromConnectionString(connectionString, protocol);
		System.out.println("Producer: Service client connected. Command messages will be sent.");
		System.out.println("");
		return serviceClient;
	}
	
	public static void DisplayConfig() {
		System.out.println(" *** Configuration Settings in Producer.java ***");
		System.out.println("Connection String: " + connectionString);
		System.out.println(" ");
	}
	
	public static void sendCommandMessageToDevice(ServiceClient serviceClient, String deviceId, CommandMessage commandMessage) throws IOException, URISyntaxException, Exception {
		
		if (serviceClient == null) { 
			System.out.println("Producer: Critical error. Service client object does not exist.");
		}

		if (serviceClient != null) {
			serviceClient.open();
			FeedbackReceiver feedbackReceiver = serviceClient.getFeedbackReceiver(deviceId);
			if (feedbackReceiver != null)
				feedbackReceiver.open();

			String refMessageId = commandMessage.RefMessageId; // CommandMessage object passed as argument
			Message messageToSend = new Message(commandMessage.serialize()); // serialized to string here
			messageToSend.setDeliveryAcknowledgement(DeliveryAcknowledgement.Full);

			serviceClient.send(deviceId, messageToSend);
			System.out.println(refMessageId + " EventProcess: Command message sent to device " + deviceId + ": " + commandMessage.serialize());

		/**	FeedbackBatch feedbackBatch = feedbackReceiver.receive(10000);
			if (feedbackBatch != null) {
				//System.out.println("Message feedback received, feedback time: " + feedbackBatch.getEnqueuedTimeUtc().toString());
			}

			if (feedbackReceiver != null)
				feedbackReceiver.close();
			serviceClient.close(); **/
		}
	}

}