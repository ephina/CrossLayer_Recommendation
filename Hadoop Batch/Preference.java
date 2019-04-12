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



import org.apache.hadoop.mapred.FileInputFormat;


public class Preference {
	public static class UMap extends MapReduceBase implements
	Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,OutputCollector<Text, Text>
		output,Reporter reporter) throws IOException{
			String comma=",";
			String line=value.toString();
			String[] st=line.split("\\|");
			Integer mv_id=Integer.valueOf(st[0].split("\\.")[0].split("_")[1]);
			
			for(int i=1;i<st.length;i++){
				
				String[] dt=st[i].split(",");
				
				String c_id=dt[0];
				String val=mv_id+comma+dt[1]+comma+dt[2];
				output.collect(new Text(c_id),new Text(val));
			}
		}
	}
	public static class UReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterator<Text> values,OutputCollector<Text,Text> output,Reporter reporter)throws IOException{
			String strout=new String();
			while(values.hasNext()){
				strout=String.valueOf(values.next());
			}
			output.collect(key, new Text(strout));
		}
	}
		
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String f_opath,f_ipath,f_name="part-00000",f_send="/";
	
		try{
			FileSystem fs = FileSystem.get(new Configuration());
			
			FileStatus[] status = fs.listStatus(new Path("hdfs://namenode:9002/records"));
			
			for (int i=0;i<status.length;i++){
				
				f_opath="/rout/custout"+String.valueOf((i+1))+f_send;
			
			
				JobClient client = new JobClient();
				
				//Used to distinguish Map and Reduce jobs from others
				JobConf conf = new JobConf(Preference.class);
				
				//Specify key and value class for Mapper
				conf.setMapOutputKeyClass(Text.class);
				conf.setMapOutputValueClass(Text.class);
				
				// Specify output types
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);
				
				
				// Specify input and output DIRECTORIES (not files)
				//FileInputFormat.addInputPath(conf, new Path(f_ipath));
				f_ipath=status[i].getPath().toString()+f_send+f_name;
				FileInputFormat.addInputPath(conf, new Path(f_ipath));
				FileInputFormat.addInputPath(conf, status[i].getPath());
				
				FileOutputFormat.setOutputPath(conf, new Path(f_opath));
				
				//Specify input and output format
				conf.setInputFormat(TextInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class);
				
				//Specify Mapper and Reducer class
				conf.setMapperClass(UMap.class);
				conf.setReducerClass(UReduce.class);
				
				client.setConf(conf);
				JobClient.runJob(conf);
			}
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

}
