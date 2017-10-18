package fridgegroup.fridgeartifact;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Log {

		// Logfile path
		static final String logfile = "./consumer.log";
		
		
		static BufferedWriter bw;
		static FileWriter fw;	
		
		 static void open() throws IOException {
			fw = new FileWriter(logfile, true);
			bw = new BufferedWriter(fw);		
				 
		 }
		 
		 static void write(String line) throws IOException {
			 bw.write(line);
			 bw.newLine();
		 }
		 
		 static void close() throws IOException {
			 fw.close();
			 bw.close();
		 }
	
}
