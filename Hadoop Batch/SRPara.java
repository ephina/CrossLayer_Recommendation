package con.bat.titan;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapred.FileSplit;

public class SRPara {
	

	public static class SRMap extends MapReduceBase implements
	Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,OutputCollector<Text, Text>
		output,Reporter reporter) throws IOException{
			
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			String filename = fileSplit.getPath().getName();
			
			String line=value.toString();
			
			char lastchar=line.charAt(line.length() - 1);
			if(lastchar!=':')
					output.collect(new Text(filename),value);
		}
	}
	public static class SRReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterator<Text> values,OutputCollector<Text,Text> output,Reporter reporter)throws IOException{
			String concatstr=new String();
			while(values.hasNext()){
				concatstr=concatstr+"|"+String.valueOf(values.next());
			}
			output.collect(key, new Text(concatstr));
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String f_opath,f_send="/";
		
		try{
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path("hdfs://namenode:9002/netsample"));
			
			for (int i=0;i<status.length;i++){
				
				f_opath="/records/singleout"+String.valueOf((i+1))+f_send;
				
				
				JobClient client = new JobClient();
				
				//Used to distinguish Map and Reduce jobs from others
				JobConf conf = new JobConf(SRPara.class);
				
				//Specify key and value class for Mapper
				conf.setMapOutputKeyClass(Text.class);
				conf.setMapOutputValueClass(Text.class);
				
				// Specify output types
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(IntWritable.class);
				
				// Specify input and output DIRECTORIES (not files)
				//FileInputFormat.addInputPath(conf, new Path(f_ipath));
				FileInputFormat.addInputPath(conf, status[i].getPath());
				FileOutputFormat.setOutputPath(conf, new Path(f_opath));
				
				//Specify input and output format
				conf.setInputFormat(TextInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class);
				
				//Specify Mapper and Reducer class
				conf.setMapperClass(SRMap.class);
				conf.setReducerClass(SRReduce.class);
				client.setConf(conf);
				JobClient.runJob(conf);
			}
		}
		catch (Exception e){
				e.printStackTrace();
		}
			

	}

}
