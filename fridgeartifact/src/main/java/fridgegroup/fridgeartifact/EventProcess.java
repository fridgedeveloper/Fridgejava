package fridgegroup.fridgeartifact;

import java.io.IOException;
import java.sql.Timestamp;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;



public class EventProcess {

	// Used to maintain 
	public static ConcurrentHashMap<String, Timestamp> Alarms = new ConcurrentHashMap<String, Timestamp>(50);

	final long AlarmSuppressionWindow = 20000; // in milliseconds
	
	// JSON Object 
	class CommandMessage {
		String MessageId;
		String RefMessageId;
		String Timestamp;
		String MessageType;
		String SourceName;
		String CommandType;
		String CommandAction;
		String Description;
		String Speech;
		
		CommandMessage() {
			// ISO 8601 Standardized
			TimeZone tz = TimeZone.getTimeZone("UTC");
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ms'Z'"); // Zulu Time																		
			df.setTimeZone(tz);
			String DateTime = df.format(new Date());
			
			Timestamp = DateTime;
			MessageType="Command";
			MessageId=java.util.UUID.randomUUID().toString();
		}
		
		String serialize() {
			Gson gson = new Gson();
			return gson.toJson(this);
		}
	}
	
	
	// JSON Object 
	class EventType {
		String MessageType = "0";
		String MessageId = "0";
	}
	
	// JSON Object 
	class AuthorizationEvent {
		String MessageId = "0";
		String Timestamp = "0";
		String MessageType = "0";
		String SourceName = "0";
		String PersonName = "0";
		String Age = "0";
		String Gender = "0";
		String Smile = "0";
		String Mustache = "0";
		String Beard = "0";
		String Sideburns = "0";
		String Glasses = "0";
		String State = "0";
		
		void process() throws IOException, URISyntaxException, Exception {
			
			System.out.println(this.MessageId + " EventProcess: Processing authorization event with state " + this.State);
			
			if (this.State.equals("Success")) {	
				CommandMessage message = new CommandMessage();
				message.CommandType="DoorLock";
				message.CommandAction="Unlock";
				message.RefMessageId=this.MessageId;
				Producer.sendCommandMessageToDevice(Producer.serviceClient,this.SourceName,message);
				System.out.println(this.MessageId + " EventProcess: Authorization Success. Sent unlock command.");
			}
			
		} 
		
	}
	
	// JSON Object 
	class SensorEvent {
		String DeviceId = "0";
		Integer MessageId = 0;
		String time = "00:00:00";		
		Double B0_SHT25_Humidity= 0.0;
		Double B0_SHT25_Temperature= 0.0;
		Integer B0_BH1745_AmbientLight_R= 0;
		Integer B0_BH1745_AmbientLight_G= 0;
		Integer B0_BH1745_AmbientLight_B= 0;
		Integer B0_BH1745_AmbientLight_C= 0;
		Double B0_LPS25HB_Pressure= 0.0;
		Double B0_LPS25HB_Temperature= 0.0;
		Integer B0_TMD26721_Promimity= 0;
		Integer B0_L3G4200_Gyro_X= 0;
		Integer B0_L3G4200_Gyro_Y= 0;
		Integer B0_L3G4200_Gyro_Z= 0;
		Double B0_LPG_MQ4_Gas= 0.0;
		Double B0_LPG_MQ9_Monoxide= 0.0;
		Double B1_MCP9803_FRZR_LEFT= 0.0;
		Double B1_MCP9803_FRZR_RIGHT= 0.0;
		Double B1_MCP9803_FRIDG_TOP_LEFT= 0.0;
		Double B1_MCP9803_FRIDG_TOP_RIGHT= 0.0;
		Double B1_MCP9803_FRIDG_BOTTOM_LEFT= 0.0;
		Double B1_MCP9803_FRIDG_BOTTOM_RIGHT= 0.0;
		Double B1_SHT25_Humidity= 0.0;
		Double B1_SHT25_Temperature= 0.0;
		Integer B2_MCP23008_STATUS= 0;
		Double B2_ADC121_Sound_Sensor= 0.0;
		Double B2_MCP3428_AD_FRZR_Door= 0.0;
		Double B2_MCP3428_AD_FRIDG_Door= 0.0;
		Double B4_MCP9600_Compressor_High_Side= 0.0;
		Double B4_MCP9600_Compressor_Low_Side= 0.0;
		Double B4_MCP9600_Evaporation_Vavle_Low_Side= 0.0;
		Boolean B0_Read_Success = true;
		Boolean B1_Read_Success = true;
		Boolean B2_Read_Success = true;
		Boolean B3_Read_Success = true;
		Boolean B4_Read_Success = true;		
		
		
		void process() throws IOException, URISyntaxException, Exception {
			
			// if any bounding conditions are violated transmit alarm command
			//if (this.InsideTemperature > 58 || this.InsideTemperature < 45) { 
			//	System.out.println(this.MessageId + " EventProcess: Detected out of bounds InsideTemp"); startAlarm(this, "InsideTemperature", "Frank not feel good inside. Check Franks temperature."); }
			//if (this.OutsideTemperature > 85 || this.OutsideTemperature < 40) { 
				//System.out.println(this.MessageId + " EventProcess: Detected out of bounds OutsideTemp"); startAlarm(this, "Frank not comfortable in here. Check room temperature."); }
			//if (this.DoorState == 1) { 
			//	System.out.println(this.MessageId + " EventProcess: Detected open door"); startAlarm(this, "DoorState", "Frank not like his insides left exposed. Close Franks door."); }
			//if (this.DoorState == 0 ) { 
			//	System.out.println(this.MessageId + " EventProcess: Detected closed door"); stopAlarm(this); }
			//if (Math.abs(this.CompressorVibration) > 15) { 
			//	System.out.println(this.MessageId + " EventProcess: Excessive compressor vibration"); startAlarm(this, "CompressorVibration", "Frank jiggly like massage chair in airport. Check Franks compressor."); }
			if (this.B1_MCP9803_FRZR_LEFT > 58 || this.B1_MCP9803_FRZR_LEFT < 45) { 
				System.out.println(this.MessageId + " EventProcess: Detected out of bounds InsideTemp"); startAlarm(this, "Temperature", "Frank not feel good inside. Check Franks temperature."); }
	
		
		}
		
		void startAlarm(SensorEvent sensorEvent, String AlarmTrigger, String Description) throws IOException, URISyntaxException, Exception {
		
			// Current Time
	        Timestamp currentTime = new Timestamp(System.currentTimeMillis());

	        // Get table entry for alarm type
	        Timestamp lastSentTime = Alarms.get(AlarmTrigger);
	        
	        long lastSentMillis = 0;
	        
	        if (lastSentTime != null) { lastSentMillis = lastSentTime.getTime(); }
	        
	        // Compare time of last sent and supress if too recent
			if ((currentTime.getTime() - lastSentMillis) > AlarmSuppressionWindow || lastSentTime == null) {
				CommandMessage message = new CommandMessage();
				message.CommandType="AlarmSound";
				message.CommandAction="Enable";
				message.Speech = Description;
				//TODO
				//message.RefMessageId=sensorEvent.MessageId;
				Producer.sendCommandMessageToDevice(Producer.serviceClient,sensorEvent.DeviceId,message);
				
				Alarms.put(AlarmTrigger, currentTime);
				System.out.println("Updated alarm last sent time. AlarmType " + AlarmTrigger + " Current Time: " + currentTime);
			} else {
				System.out.println("Supressed alarm. AlarmType " + AlarmTrigger + " Current Time: " + currentTime);	
			}
		
		}
		
		void stopAlarm(SensorEvent sensorEvent) throws IOException, URISyntaxException, Exception {
			CommandMessage message = new CommandMessage();
			message.CommandType="AlarmSound";
			message.CommandAction="Disable";
			//TODO
			//message.RefMessageId=sensorEvent.MessageId;
			Producer.sendCommandMessageToDevice(Producer.serviceClient,sensorEvent.DeviceId,message);
		}
	}
	
	

	String getEventType(String json) {	
			Gson gson = new GsonBuilder().create();
			EventType eventType=gson.fromJson(json, EventType.class);
			return eventType.MessageType.toString();
	}
		
	
		
	void Deseralize(String json) throws URISyntaxException, Exception {
		String type;
		
		System.out.println("EventProcess: Deserialize new message " + json);
		Gson gson = new GsonBuilder().create();
		EventType eventType=gson.fromJson(json, EventType.class);
		type = eventType.MessageType.toString();
		String messageId = "n/a";
		messageId = eventType.MessageId.toString();
		System.out.println(messageId + " EventProcess: Deserialize new " + type + " message " + json);
		
		
		switch(type) {
		case "Sensor" :
			//System.out.println("EventProcess: Sensor message type detected");
			SensorEvent sensorEvent = gson.fromJson(json, SensorEvent.class);
			sensorEvent.process();
			break;
		case "Authorization" :
			//System.out.println("EventProcess: Authorization message type detected");
			AuthorizationEvent authorizationEvent = gson.fromJson(json, AuthorizationEvent.class);
			authorizationEvent.process();
			break;
		default :
		}
	}
		
}
