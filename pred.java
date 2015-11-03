/* store by predicate in seperate file */
import java.lang.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
 import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs; //+++

public class code
{
  
	
	
    public static class joinsMap extends Mapper<LongWritable,Text,Text,Text>{

	
	private  static Text outKey=new Text();
	private  static Text outValue=new Text();
	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

	
	
 	String line = value.toString();
	

        StringTokenizer st = new StringTokenizer(line);
	      
	String token="";
	String tempKey="";
	String tempVal="";
		
		
	      
	      // checking next token
	     while (st.hasMoreTokens()){
	    	token= st.nextToken();
	    	String source=token.substring(token.indexOf("#")+1,token.indexOf(">"));
		
		String b="";
		String c="";
		
		b=token.substring(token.indexOf("(")+1,token.indexOf(",")); // s(b,c). 
		c=token.substring(token.indexOf(",")+1,token.indexOf(")")); 
			
				
			outKey.set(source);
			outValue.set(b+c);	
	    	  	context.write(outKey, outValue);//**** 
	    	 	
		
		
	    	source=null;
		
		b=null;
		c=null;
		

	    	 
	     } // end while , checking tokens.
		

               
			
   
 }// end map method

}//end map class
  
public static class joinsReduce extends Reducer<Text,Text,Text,Text>{
	   MultipleOutputs<Text, Text> mos; //++
	//private String fruitOutputName= "fruit";//+++
	//private String colorOutputName= "color";//+++
	

	@Override  //+++
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
    }

public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
	for (Text value : values){
	String str = value.toString();
        String[] items = str.split(",");
	// context.write(key,value);
	 mos.write("fruit", NullWritable.get(),new Text(items[1]));
         mos.write("color", NullWritable.get(), new Text(items[2]));
		} //end for 
	}//end method
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
	
	
}//end class

	public static void main( String[] args ) throws Exception {

	


     Configuration conf1 = new Configuration();
         
      
	 Job job1 = new Job(conf1, "job1");
	
	job1.setJarByClass(code.class);
	
	//job1.setNumReduceTasks(17);//***
     
     job1.setOutputKeyClass(Text.class);
     job1.setOutputValueClass(Text.class);
         
     job1.setMapperClass(joinsMap.class);
     job1.setReducerClass(joinsReduce.class);
         
     job1.setInputFormatClass(TextInputFormat.class);
     job1.setOutputFormatClass(TextOutputFormat.class);

// single input to  map. change to this
        FileInputFormat.addInputPath(job1, new Path(args[0]));   
  	
     // single output from reducer.
     FileOutputFormat.setOutputPath(job1, new Path(args[1]));
         
     	MultipleOutputs.addNamedOutput(job1, "fruit", TextOutputFormat.class, NullWritable.class, Text.class); //+++
   	MultipleOutputs.addNamedOutput(job1, "color", TextOutputFormat.class, NullWritable.class, Text.class); //+++

     job1.waitForCompletion(true);


} //end main
}
