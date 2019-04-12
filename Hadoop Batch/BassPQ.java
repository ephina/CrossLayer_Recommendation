package con.bat.titan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class BassPQ {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String fn="/home/hadoop/R6/ydata-fp-td-clicks-v1_0.20090501";
				
		String line=null;
		String [] mv_desc=null;
		String [] clk_desc=null;
		String mv_name=null;
		long count=0,c=0;
		Configuration conf = new Configuration();
		 
		try{
			FileSystem fs = FileSystem.get(conf);
			FileReader fr=new FileReader(fn);
			BufferedReader br=new BufferedReader(fr);
			while((line=br.readLine())!= null){
				mv_desc=line.split("\\|");
				clk_desc=mv_desc[0].split("\\s+");
				if(clk_desc[2].trim().matches("1")==true)count++;
				if(clk_desc[1].trim().matches("109525")==true)c++;
			}
			br.close();
			System.out.println("count="+count);
			System.out.println("c="+c);
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
}
