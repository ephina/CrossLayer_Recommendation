package con.bat.titan;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
	
public class ParaInput {
	
	public static class MapInput extends MapReduceBase implements
	Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,OutputCollector<Text, Text>
		output,Reporter reporter) throws IOException{
			IntWritable one=new IntWritable(1);
			String line=value.toString();
			
			char lastchar=line.charAt(line.length() - 1);
			if(lastchar!=':'){
				String[] st=line.split(",");
				String[] dt=st[2].split("-");
				String yr_mn=dt[0]+"-"+dt[1];
				output.collect(new Text(yr_mn),new Text(String.valueOf(one)));
			}
		}
	}
	public static class ReduceInput extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
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
		// long num=10000000,mv_no,count=1;
		// String f_idir="/netflix/";
		String f_iname,f_ipath,f_sfix=".txt",f_pfix="mv_",f_opath,f_send="/";
		
		try{
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path("hdfs://namenode:9002/netsample"));
			
			for (int i=0;i<status.length;i++){
               
                
		
		/*	for(count=1;count<5;count++){
				mv_no=num + count;
				f_iname=f_pfix+String.valueOf(mv_no).substring(1)+f_sfix;
				f_ipath=f_idir+f_iname;
		*/
				f_opath="/out"+String.valueOf((i+1))+f_send;
		
			
				JobClient client = new JobClient();
				
				//Used to distinguish Map and Reduce jobs from others
				JobConf conf = new JobConf(ParaInput.class);
				
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
				conf.setMapperClass(MapInput.class);
				conf.setReducerClass(ReduceInput.class);
				client.setConf(conf);
				JobClient.runJob(conf);
			}
		}
		catch (Exception e){
				e.printStackTrace();
		}
	}
}
	
