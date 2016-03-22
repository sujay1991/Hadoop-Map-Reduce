
import java.io.IOException;
import java.util.*;
import java.lang.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class R {
	
  public static ArrayList<Integer> arrayTemp = new ArrayList<Integer>();
  public static ArrayList<Integer> arrayPrcp = new ArrayList<Integer>();
  public static ArrayList<Integer> arrayWind = new ArrayList<Integer>();

  // Mapper Class
  public static class MapReduceMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, Text> {


    public void map(LongWritable key, Text val,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

      // Getting each line of input and converting it into a string
      String line = val.toString();
      
      String getToken[] = line.split(",");
      String Date = getToken[2];
      String prcp = getToken[6];
      String tmp = getToken[3]; 
      String wind = getToken[13];
      
      if(StringUtils.isNumeric(Date)) {
	      String year = Date.substring(0, 4); 
	      String values = tmp + "," + prcp + "," + wind; 
	      
	      output.collect(new Text(year), new Text(values));
      }
    }
  }


  // Reducer class
  public static class MapReduceReducer extends MapReduceBase
      implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

           int temptot = 0, prcptot = 0, windtot=0; 
           int tempN = 0, prcpN = 0, windN=0; 
           int avgTemp = 0, avgPrcp = 0,avgWind=0; 
                      
           // Summing temperature, precipitation and wind for each year
           while (values.hasNext()) {
        	   
        	   String nextValue = values.next().toString();
        	   String Climate[] = nextValue.split(",");
        	   int currTemp = Integer.parseInt(Climate[0]);
        	   if(currTemp != -9999) {
        		   temptot += currTemp; 
        		   tempN += 1; 
        	   }
        	   int currPrcp = Integer.parseInt(Climate[1]);
        	   if(currPrcp != -9999) {
        		   prcptot += currPrcp; 
        		   prcpN += 1; 
        	   }
        	   int currWind = Integer.parseInt(Climate[2]);        	   
        	   if(currWind != -9999) {
        		   windtot += currWind; 
        		   windN += 1; 
        	   }
           }
          // Finding Average for temperature, Precipitation and Wind
           if(tempN != 0) {
        	   avgTemp = temptot / tempN;
           }
           if(prcpN != 0) {
        	   avgPrcp = prcptot / prcpN;
           }
           if(windN != 0) {
        	   avgWind = windtot / windN;
           }

           arrayTemp.add(avgTemp);
           arrayPrcp.add(avgPrcp);
           arrayWind.add(avgWind);
             
           String comparison = "," +String.valueOf(avgTemp) + "," + String.valueOf(avgPrcp) + "," + String.valueOf(avgWind);
           
           output.collect(key, new Text(comparison));  
    }
  }



  
  public static void main(String[] args) {

    JobConf conf = new JobConf(R.class);
    conf.setJobName("R");
    
    conf.setNumMapTasks(Integer.parseInt(args[2]));
    conf.setNumReduceTasks(Integer.parseInt(args[3]));
    
    long jobStartTime = System.currentTimeMillis();

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
  
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    conf.setMapperClass(MapReduceMapper.class);
    conf.setReducerClass(MapReduceReducer.class);

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


