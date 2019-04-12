package con.bat.titan;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SingleRec {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String delimit="|";
		try{
			
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path("hdfs://namenode:9002/netsample"));
			String fnw=new String("/home/hadoop/ephi-jars/List");
			String f_path=new String("hdfs://namenode:9002/netsingle");
			FileWriter fw=new FileWriter(fnw);
			BufferedWriter bw=new BufferedWriter(fw);
			for (int i=0;i<status.length;i++){
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line=null;
                line=br.readLine();
                while (line != null){
                        line=line+br.readLine()+delimit;
                }
                bw.write(line);
			}
			bw.close();
			fs.copyFromLocalFile(new Path(fnw), new Path(f_path));
		}
		catch(IOException e){
			e.printStackTrace();
		}

	}

}
