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
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SingularValueDecomposition;
import org.apache.mahout.math.Vector;


public class BassCoeff {
	public static class BCMap extends MapReduceBase implements
	Mapper<LongWritable,Text,IntWritable,Text>{
		public void map(LongWritable key,Text value,OutputCollector<IntWritable, Text>
		output,Reporter reporter) throws IOException{
			
			int row=2,col=2,i,j,noln;
			String [] lneqns,sin_ln_eqn,record;
			String line;
			String sKey;
			Text out_val=new Text();
			double[][] left;
			double[] d_right=new double[col];
			double[] right;
			Matrix m_inv=new DenseMatrix(row,col);
			Matrix m=new DenseMatrix(row,col);
			Matrix s_inv=new DenseMatrix(row,col);
			Vector temp=new DenseVector(row);
			Vector v=new DenseVector(col);
			Vector v_out=new DenseVector(col);
			Vector v_right=new DenseVector(col);
				
			line=value.toString();
			record=line.split("\\s+");
			sKey=record[0];
			lneqns=record[1].split("\\|");
			
			noln=lneqns.length;
			left=new double[noln][col];
			right=new double[noln];
			for(i=0;i<noln;i++){
				sin_ln_eqn=lneqns[i].split(",");
				for(j=0;j<noln;j++){
					left[i][j]=Double.parseDouble(sin_ln_eqn[j]);
				}
				right[i]=Double.parseDouble(sin_ln_eqn[2]);
			}
			v.assign(left[0]);
			m.assignRow(0,v);
			
			v.assign(left[1]);
			m.assignRow(1,v);
			
			d_right[0]=right[0];
			d_right[1]=right[1];
			v_right.assign(d_right);
			
			SingularValueDecomposition svd = new SingularValueDecomposition(m);
			double[] sig_val=svd.getSingularValues();
			double[] zigma = new double[col];
			
			for(i=0;i<row;i++){
				for(j=0;j<col;j++){
					zigma[j]=svd.getS().get(i,j)/(sig_val[i]*sig_val[i]);
				}
				temp.assign(zigma);
				s_inv.assignRow(i,temp);
			}
			
			m_inv=svd.getV().times(s_inv).times(svd.getU().transpose());
			
			v_out=m_inv.times(v_right);
			
			out_val.set(String.valueOf(v_out));
			output.collect(new IntWritable(Integer.valueOf(sKey)), out_val);
		
		}
	
	}
	public static class BCReduce extends MapReduceBase implements Reducer<IntWritable,Text,IntWritable,Text>{
		public void reduce(IntWritable key,Iterator<Text> values,OutputCollector<IntWritable,Text> output,Reporter reporter)throws IOException{
			int cnt=0,len=0;
			double sum1=0,sum2=0,p,q;
			String p_str,q_str,pq,outstr;
			while(values.hasNext()){
				pq=values.next().toString();
				p_str=pq.split(",")[0].split(":")[1];
				q_str=pq.split(",")[1].split(":")[1];
				len=q_str.length()-1;
				p=Double.parseDouble(p_str);
				q=Double.parseDouble(q_str.substring(0,len));
				if(!Double.isNaN(p)|!Double.isNaN(q)){
					sum1+=p;
					sum2+=q;
				}
				cnt++;
			}
			outstr=String.valueOf(sum1/cnt)+","+String.valueOf(sum2/cnt);
			output.collect(key, new Text(outstr));
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JobClient client = new JobClient();
		//Used to distinguish Map and Reduce jobs from others
		JobConf conf = new JobConf(BassCoeff.class);
		//Specify key and value class for Mapper
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		// Specify output types
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		// Specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path("/BassLeqns/part-m-00000"));
		FileOutputFormat.setOutputPath(conf, new Path("/OutPQ/"));
		//Specify input and output format
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//Specify Mapper and Reducer class
		conf.setMapperClass(BCMap.class);
		conf.setReducerClass(BCReduce.class);
		client.setConf(conf);
		try
		{
			JobClient.runJob(conf);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

}
