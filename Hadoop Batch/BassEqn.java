package con.bat.titan;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BassEqn extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {

		
		Job  conf=new Job(getConf()); 
		conf.setJarByClass(BassEqn.class);		

		conf.setMapperClass(BassMap.class);
		conf.setNumReduceTasks(0);
		
		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputFormatClass(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(conf, new Path("/BassInput/bassinput"));
		FileOutputFormat.setOutputPath(conf, new Path("/BassLeqns/"));
		
		
		boolean success = conf.waitForCompletion(true);
		return success ? 0 : 1;

	}
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new Configuration(), new BassEqn(), args);
		System.exit(exitCode);
	}
}
class BassMap extends Mapper<LongWritable,Text,IntWritable,Text>{
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		int i=0,tot_eqn_no=0,N=0,v1=0,x1=0,y1=0,Qt_p=0,no_itr=0;
		String line=null,delim2="|", delim3=",",eqns=null;
		
		String [] mv_desc=null;
		String [] fir_arg=null;
		String [] sec_arg=null;
		String [] part2=null;
		
		
			
		line=value.toString();
		mv_desc=line.split(":");
		
		if(mv_desc.length!=1){
			fir_arg=mv_desc[0].split(",");
			
			N=Math.round(Float.parseFloat(fir_arg[4]));
			sec_arg=mv_desc[1].split("\\|");
			tot_eqn_no=Integer.parseInt(fir_arg[1]);
			part2=new String[tot_eqn_no];
			if(tot_eqn_no>1){
				
				for(i=0;i<(tot_eqn_no-1);i++){
					
					String [] detp=sec_arg[i].split(",");
					String [] detc=sec_arg[i+1].split(",");
					
					Qt_p=Math.round(Float.parseFloat(detp[2]));
					x1=N-Qt_p;
					y1=(Qt_p-(Qt_p*Qt_p/N));
					//v1=Math.round(Float.parseFloat(week_det[1][2]));
					v1=Math.round(Float.parseFloat(detc[1]));
					part2[i]= x1+delim3+y1+delim3+v1;

				}
				
				for(i=0;i<(tot_eqn_no-3);i++){
					
					eqns=part2[i]+delim2+part2[i+1];
					context.write(new IntWritable(Integer.parseInt(fir_arg[0])), new Text(eqns));
				
				}
			}
		}
	}
}


	

