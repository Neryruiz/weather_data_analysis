import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.mapred.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StormEventMR {
	//Mapper class for stromEvent data set	
	public static class DataMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
		
		//Mapper function
		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {		
			String valueString = value.toString();
			
			//split string to a string []
			String [] Data = valueString.split(",");

			///extract digits for string
			String t_d = Data[22].replaceAll("\\D+","");
			String t_ij = Data[20].replaceAll("\\D+","");
			
			int tmp_d; 
			int tmp_ij;
			
			//convert string to int
			if(!t_d.equals("")){
				tmp_d = Integer.parseInt(t_d);
			}else{
				tmp_d = 0;
			}

			if(!t_ij.equals("")){
				tmp_ij = Integer.parseInt(t_ij);
			}else{
				tmp_ij = 0;
			}

			//create Inwritable data types
			IntWritable numDeaths = new IntWritable(tmp_d);
			IntWritable numInjuries = new IntWritable(tmp_ij);
			
			//collect the data from right column 
			Text stateInjuries = new Text(Data[8]+"_injuries");
			Text stateDeaths = new Text(Data[8]+"_deaths");			
			
			//output a key and it value
			output.write(stateInjuries, numInjuries);
			output.write(stateDeaths, numDeaths);
		}
	}

	//reduce class for stromevent folder
	public static class ReducerSum extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable result = new IntWritable();
		public void reduce(Text t_key, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException {
			int num = 0;
			//sum up the values
			for (IntWritable val : values){
				num += (int)val.get();
			}
			result.set(num);
			
			//output the key with summed values
			output.write(t_key, result);
		}
	}

	//main class
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "count Death and Injuries");
		job.setJarByClass(StormEventMR.class);
		job.setMapperClass(DataMapper.class);
		job.setCombinerClass(ReducerSum.class);
    		job.setReducerClass(ReducerSum.class);
    		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
