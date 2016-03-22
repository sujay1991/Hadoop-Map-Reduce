//package hadoop;

import java.io.IOException;
import java.util.*;
import java.lang.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class year {
	
  public static ArrayList<Integer> atemp = new ArrayList<Integer>();
  public static ArrayList<Integer> aprcp = new ArrayList<Integer>();
  public static ArrayList<Integer> awind = new ArrayList<Integer>();

  // Mapper Class
  public static class Mapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, Text> {


    public void map(LongWritable key, Text val,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

      // Getting each line of input and converting it into a string
      String line = val.toString();
      
      String getToken[] = line.split(",");
      String date = getToken[2];
      String prcp = getToken[6];
      String temp = getToken[3]; 
      String wind = getToken[13];
      
      if(StringUtils.isNumeric(date)) {
	      String year = date.substring(0, 4); 
	      String values = temp + "," + prcp + "," + wind; 
	      
	      output.collect(new Text(year), new Text(values));
      }
    }
  }


  // Reducer class
  public static class Reducer extends MapReduceBase
      implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

           int total_temp = 0, total_prcp = 0, total_wind=0; 
           int tempN = 0, prcpN = 0, windN=0; 
           int avgTemp = 0, avgPrcp = 0,avgWind=0; 
           float formatTemp, formatPrcp, formatWind; 
           String compTempRes = "NOT COMPARED", compPrcpRes = "NOT COMPARED", compWindRes = "NOT COMPARED"; 
           // Summing temperature, precipitation and wind for each year
           while (values.hasNext()) {
        	   
        	   String nextValue = values.next().toString();
        	   String Climate[] = nextValue.split(",");
        	   int current_temp = Integer.parseInt(Climate[0]);
        	   if(current_temp != -9999) {
        		   total_temp += current_temp; 
        		   tempN += 1; 
        	   }
        	   int current_prcp = Integer.parseInt(Climate[1]);
        	   if(current_prcp != -9999) {
        		   total_prcp += current_prcp; 
        		   prcpN += 1; 
        	   }
        	   int current_wind = Integer.parseInt(Climate[2]);        	   
        	   if(current_wind != -9999) {
        		   total_wind += current_wind; 
        		   windN += 1; 
        	   }
           }
          // Finding Average for temperature, Precipitation and Wind
           if(tempN != 0) {
        	   avgTemp = total_temp / tempN;
           }
           if(prcpN != 0) {
        	   avgPrcp = total_prcp / prcpN;
           }
           if(windN != 0) {
        	   avgWind = total_wind / windN;
           }
           // Next is to gather all the averages and compare with previous year
           atemp.add(avgTemp);
           aprcp.add(avgPrcp);
           awind.add(avgWind);
           
           int arrayTempn = atemp.size();
           int arrayPrcpn = aprcp.size();
           int arrayWindn = awind.size();
           
           // Compare and Record the Comparison result for Temperature, Precipitation and Wind
           if (arrayTempn > 1) {
        	   int compInt = atemp.get(arrayTempn - 2);
        	   if(compInt < avgTemp) {
        		   compTempRes = "Getting Warmer than Last Year";
        	   } else if(compInt > avgTemp) {
        		   compTempRes = "Getting Colder than Last Year";
        	   } else if(compInt == avgTemp) {
        		   compTempRes = "Same Result as Last Year";
        	   }
           }
           if (arrayPrcpn > 1) {
        	   int compInt = aprcp.get(arrayPrcpn - 2);
        	   if(compInt < avgPrcp) {
        		   compPrcpRes = "Getting Wetter than Last Year";
        	   } else if(compInt > avgPrcp) {
        		   compPrcpRes = "Getting Dryer than Last Year";
        	   } else if(compInt == avgPrcp) {
        		   compPrcpRes = "Same Result as Last Year";
        	   }
           }
           if (arrayWindn > 1) {
        	   int compInt = aprcp.get(arrayWindn - 2);
        	   if(compInt < avgWind) {
        		   compWindRes = "Getting more windy than Last Year";
        	   } else if(compInt > avgWind) {
        		   compWindRes = "Getting less windy than Last Year";
        	   } else if(compInt == avgWind) {
        		   compWindRes = "Same Result as Last Year";
        	   }
           }
           
           
           formatTemp = (float) (avgTemp * 0.1);
           formatPrcp = (float) (avgPrcp * 0.1);
           formatWind = (float) (avgWind);
           
         
           String comparison = " ---- " + String.valueOf(formatTemp) + "C -" + compTempRes + 
        		   " ---- " + String.valueOf(formatPrcp) + "mm -" + compPrcpRes + 
        		   " ---- " + String.valueOf(formatWind) + "miles/hr - " + compWindRes;
           
           output.collect(key, new Text(comparison));  
    }
  }


 
  
  public static void main(String[] args) {

    JobConf conf = new JobConf(year.class);
    conf.setJobName("year");
    
    conf.setNumMapTasks(Integer.parseInt(args[2]));
    conf.setNumReduceTasks(Integer.parseInt(args[3]));
    
    long jobStartTime = System.currentTimeMillis();

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
  
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    conf.setMapperClass(Mapper.class);
    conf.setReducerClass(Reducer.class);

    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    long jobEndTime = System.currentTimeMillis();
    
    // Calculating the Time Taken by the job
    long timeTaken = jobEndTime - jobStartTime;
    System.out.println("\n Time Taken = " + timeTaken);
    
  }
}


