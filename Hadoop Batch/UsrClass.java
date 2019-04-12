package con.bat.titan;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class UsrClass {
	
	public static class UsrMap extends MapReduceBase implements
	Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,OutputCollector<Text, Text>
		output,Reporter reporter) throws IOException{
			String line,outkey=null,comma=",",outval=null;
			String [] mv_desc,fir_arg;
			int max,inno,ea;
			line=value.toString();
			mv_desc=line.split(":");
			
			if(mv_desc.length!=1){
				fir_arg=mv_desc[0].split(",");
				max=Integer.parseInt(fir_arg[5]);
				
				outkey=fir_arg[0];
				inno=(int)(0.025*max);
				ea=(int)(0.16*max);
				outval=inno+comma+ea+comma+fir_arg[5]+comma+fir_arg[6];
			}		
			
			output.collect(new Text(outkey),new Text(outval));
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			
			String f_ipath="/BassInput/bassinput";
			String f_opath="/Class/usrclass";
			JobClient client = new JobClient();
				
			//Used to distinguish Map and Reduce jobs from others
			JobConf conf = new JobConf(UsrClass.class);
			
			//Specify key and value class for Mapper
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			
			
			
			// Specify input and output path
			
			FileInputFormat.addInputPath(conf, new Path(f_ipath));
			FileOutputFormat.setOutputPath(conf, new Path(f_opath));
			
			//Specify input and output format
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			//Specify Mapper and Reducer class
			conf.setMapperClass(UsrMap.class);
			
			
			client.setConf(conf);
			JobClient.runJob(conf);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}

}


