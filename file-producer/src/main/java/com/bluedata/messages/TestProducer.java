package com.bluedata.messages;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.io.*;
 
public class TestProducer {
    public static void main(String[] args) {
        long events = Long.parseLong(args[0]);
        Random rnd = new Random();
        BufferedReader reader = null;        


       Properties in_config = new Properties();
       ClassLoader loader = Thread.currentThread().getContextClassLoader();
       InputStream inputStream = loader.getResourceAsStream("config.properties");
       try { 
	  if (inputStream != null) {
		in_config.load(inputStream);
	   } else {
  		throw new FileNotFoundException("property file config.properties not found in the classpath");
	   }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
 
	// get the property value and print it out
	String broker = in_config.getProperty("broker");

  
        Properties props = new Properties();
       // props.put("metadata.broker.list", "bluedata-175.bdlocal:9092");
        props.put("metadata.broker.list", broker);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
        try {
           reader = new BufferedReader(new FileReader("Consumer_Complaints.csv")); //to read the file
        } catch (FileNotFoundException ex) { //file Consumer_Complaints.csv does not exists
    System.err.println(ex.toString());
      }
        
       String line; //to store a read line
       try{
     
         while(true){
           Thread.sleep(3000); 
           ArrayList<String> array = new ArrayList<String>();
           int N = 50; //max number of lines to read
           int counter = 0; //current number of lines already read
           //read line by line with the readLine() method
           while ((line = reader.readLine()) != null && counter < N) { 
              //check also the counter if it is smaller then desired amount of lines to read
              
	      line = line.toString();
              if (rnd.nextInt(7) <= 3)
                array.add(line); //add the line to the ArrayList of stringsa
              counter++; //update the counter of the read lines (increment by one)
           }
              String msg = array.toString();
              KeyedMessage<String, String> data = new KeyedMessage<String, String>("consumer_complaints", msg);
               producer.send(data);
               
             System.out.println(data);
         }
     } catch (Exception ex) { 
        System.err.println(ex.toString());
     }
 
    try {
       reader.close(); //close the reader and related streams
     } catch (Exception ex) { //file Consumer_Complaints.csv does not exists
        System.err.println(ex.toString());
     }
   }
}
