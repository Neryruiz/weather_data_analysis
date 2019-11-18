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
	//Mapper for stromEvent data set	
	public static class DataMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
		//private IntWritable numDeaths;
		//private IntWritable numInjuries;
		//private Text stateInjuries; 
		//private Text stateDeath;

		//public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output) throws IOException {
		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {		
			String valueString = value.toString();
			String [] Data = valueString.split(",");

			String t_d = Data[22].replaceAll("\\D+","");
			String t_ij = Data[20].replaceAll("\\D+","");
			
			int tmp_d;// = 
			int tmp_ij;// = Integer.parseInt(Data[20]);
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


			IntWritable numDeaths = new IntWritable(tmp_d);
			IntWritable numInjuries = new IntWritable(tmp_ij);
			
			Text stateInjuries = new Text(Data[8]+"_injuries");
			Text stateDeaths = new Text(Data[8]+"_deaths");			

			//output.collect(stateInjuries, numInjuries);
			//output.collect(stateDeaths, numDeaths);
			
			output.write(stateInjuries, numInjuries);
			output.write(stateDeaths, numDeaths);
		}
	}

	//need to fix not working
	public static class ReducerSum extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable result = new IntWritable();
		public void reduce(Text t_key, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException {
			//Text k = t_key;
			int num = 0;
			//while(values.hasNext()){
			for (IntWritable val : values){
				//IntWritable value = (IntWritable) values.next();
				num += (int)val.get();
			}
			result.set(num);
			output.write(t_key, result);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "count Death and Injuries");
		job.setJarByClass(StormEventMR.class);
		job.setMapperClass(DataMapper.class);
		job.setCombinerClass(ReducerSum.class);
    		job.setReducerClass(ReducerSum.class);
    		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setInputFormat(TextInputFormat.class);
		//job.setOutputFormat(TextOutputFormat.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
