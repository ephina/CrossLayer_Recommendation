package con.bat.titan;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class NBassMovie{

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//FileSystem.delete(Path, BOOLEAN)
		//String fn="/data/movie_desc";
		String fn="/home/hadoop/NewProject/TBM/data/movie_desc";
		String line=null;
		String [] mv_desc=null;
		String mv_name=null;
		
		String f_name=null;
		String f_pfix="mv_";
		String f_sfix=".txt";
		String f_path=null;
		String f_dir="/netdata/";
		
		String s_dir="/home/hadoop/netflix/training_set/";
		String s_path=null;
		long num=10000000,mv_no;
		
		Configuration conf = new Configuration();
		 
		try{
			FileSystem fs = FileSystem.get(conf);
			FileReader fr=new FileReader(fn);
			BufferedReader br=new BufferedReader(fr);
			while((line=br.readLine())!= null){
				mv_desc=line.split("\\|");
				mv_no=num + Long.parseLong(mv_desc[0]);
				f_name=f_pfix+String.valueOf(mv_no).substring(1)+f_sfix;
				f_path=f_dir+f_name;
				
				s_path=s_dir+f_name;
				fs.copyFromLocalFile(new Path(s_path), new Path(f_path));
			}
			br.close();
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
}
