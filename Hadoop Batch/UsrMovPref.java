package con.bat.titan;
import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.fs.Path;
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

public class UsrMovPref {
	
	public static class UMPMap extends MapReduceBase implements
	Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,OutputCollector<Text, Text>
		output,Reporter reporter) throws IOException{
			
			String line=value.toString();
			String[] st=line.split("\\s+");
			
			output.collect(new Text(st[0]),new Text(st[1]));
		}
	}
	public static class UMPReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterator<Text> values,OutputCollector<Text,Text> output,Reporter reporter)throws IOException{
			String concatstr=new String();
			while(values.hasNext()){
				concatstr=concatstr+String.valueOf(values.next())+"|";
			}
			output.collect(key, new Text(concatstr));
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			
			String f_ipath="/M_Cust/MCust_Pref";
			String f_opath="/Cust_Preference/Cust_Mov_Pref";
			JobClient client = new JobClient();
				
			//Used to distinguish Map and Reduce jobs from others
			JobConf conf = new JobConf(UsrMovPref.class);
			
			//Specify key and value class for Mapper
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			
			// Specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			
			
			// Specify input and output path
			
			FileInputFormat.addInputPath(conf, new Path(f_ipath));
			FileOutputFormat.setOutputPath(conf, new Path(f_opath));
			
			//Specify input and output format
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			//Specify Mapper and Reducer class
			conf.setMapperClass(UMPMap.class);
			conf.setReducerClass(UMPReduce.class);
			
			client.setConf(conf);
			JobClient.runJob(conf);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}

}
