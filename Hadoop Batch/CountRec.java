package con.bat.titan;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CountRec {
	public static class MapCount extends MapReduceBase implements
	Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,OutputCollector<Text, Text>
		output,Reporter reporter) throws IOException{
			IntWritable one=new IntWritable(1);
			output.collect(new Text(String.valueOf(one)),new Text(String.valueOf(one)));
			
		}
	}
	public static class ReduceCount extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterator<Text> values,OutputCollector<Text,Text> output,Reporter reporter)throws IOException{
			int sum=0;
			while(values.hasNext()){
				values.next();
				sum++;
			}
			output.collect(key, new Text(String.valueOf(sum)));
		}
	}
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		long count=1;
		String f_idir="/out";
		String f_iname="part-00000",f_ipath,f_opath,f_send="/";
		
		for(count=1;count<81;count++){
		
			f_iname="part-00000";
			f_ipath=f_idir+count+f_send+f_iname;
			f_opath="/output"+String.valueOf(count)+f_send;
		
		
			JobClient client = new JobClient();
			//Used to distinguish Map and Reduce jobs from others
			JobConf conf = new JobConf(CountRec.class);
			
			//Specify key and value class for Mapper
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			
			// Specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			
			// Specify input and output DIRECTORIES (not files)
			FileInputFormat.addInputPath(conf, new Path(f_ipath));
			FileOutputFormat.setOutputPath(conf, new Path(f_opath));
			
			//Specify input and output format
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			//Specify Mapper and Reducer class
			conf.setMapperClass(MapCount.class);
			conf.setReducerClass(ReduceCount.class);
			client.setConf(conf);
			try{
		
				JobClient.runJob(conf);
			}
			catch (Exception e){
				e.printStackTrace();
			}	
		}
	}
}