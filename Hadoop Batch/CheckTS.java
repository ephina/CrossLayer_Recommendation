package con.bat.titan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;



public class CheckTS {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String fn="/home/hadoop/R6/ydata-fp-td-clicks-v1_0.20090501";
		String fn1="/home/hadoop/R6/ydata-fp-td-clicks-v1_0.20090502";
		
		String line=null,line1=null;
		String [] mv_desc=null;
		String [] mv_desc1=null;
		String mv_name=null;
		long count=0,c=0;
				 
		try{
			
			FileReader fr=new FileReader(fn);
			BufferedReader br=new BufferedReader(fr);
			while((line=br.readLine())!= null){
				mv_desc=line.split("\\|");
				System.out.println("f1="+mv_desc[1].trim());
				FileReader fr1=new FileReader(fn1);
				BufferedReader br1=new BufferedReader(fr1);
				while((line1=br1.readLine())!= null){
					mv_desc1=line.split("|");
					if(mv_desc[1].trim().matches(mv_desc1[1].trim())==true){
						//System.out.println("f1="+mv_desc[1]);
						//System.out.println("f2="+mv_desc1[1].trim());
						count++;
					}
				}
				System.out.println("count="+count);
				count=0;
				br1.close();
			}
			
			br.close();
			
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}

	}

}
