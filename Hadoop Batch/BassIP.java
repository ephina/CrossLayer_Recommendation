package con.bat.titan;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;


public class BassIP {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String mv_name,mv_dir,fn,fn1,fnout,tail1="output",tail2="out",f_name="part-00000",end="/",line,line1,outline,comma=",",colon=":",pipe="|",peak_time=null;
		String [] mv_parts,mv_part1,mv_wk_sale;
		String hdfs_dir="hdfs://namenode:9002";
		String fout="/BassInput/BI";
		String fdir1="/MonthSales/";
		String fdir2="/TotalWeeks/";
		String fdir3="/netsample/";
		String fnw="/home/hadoop/bassinput";
		String fnw1="/home/hadoop/usrcategory";
		long mv_id,tot_sales=(long) 0;
		int tot_weeks=0,file_no,div_f=0,sum=0,cnt=0,j=0,cum_wsale=0,max=0,period=0;
		int [] cum_sale;
		String [] yr_mn;
		
		try{
			

			/**Tests
			String fnw="/home/hadoop/BatchProject/BatchTiTan/data/movieid";
			FileWriter fw=new FileWriter(fnw);
			BufferedWriter bw=new BufferedWriter(fw);
			*/
			mv_dir=hdfs_dir+fdir3;
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(mv_dir));
                      
            for (int i=0;i<status.length;i++){
            	
            		max=0;
            		//get the movie-id
            		mv_name=status[i].getPath().getName();
            		mv_parts=mv_name.split("\\.");
            		mv_part1=mv_parts[0].split("\\_");
            		mv_id=Long.parseLong(mv_part1[1]);
            		
            		//get the total weeks sale            		
            		file_no=i+1;
            		fn=hdfs_dir+fdir2+tail1+file_no+end+f_name;
            		
            		FileSystem fs2 = FileSystem.get(new Configuration());
        			BufferedReader br=new BufferedReader(new InputStreamReader(fs2.open(new Path(fn))));
            		
        			line=br.readLine();
        			tot_weeks=Integer.parseInt(line.split("\\s+")[1]);
        			
        			br.close();
        			if(tot_weeks<28)div_f=3;
        			else div_f=6;
        					
        			fn1=hdfs_dir+fdir1+tail2+file_no+end+f_name;
        			FileSystem fs1 = FileSystem.get(new Configuration());
        			BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(new Path(fn1))));
        			j=0;
        			int ln_no=0;
        			cum_sale=new int[20];
        			yr_mn=new String[20];
        			while((line1=br1.readLine())!= null){
        				cnt++;
        				mv_wk_sale=line1.split("\\s+");
        				sum=sum+Integer.parseInt(mv_wk_sale[1]);
        				if(cnt==div_f){
        					cum_sale[j]=sum;
        					yr_mn[j]=mv_wk_sale[0];
        					j++;
        					sum=0;
        					cnt=0;
        				}
        				if(ln_no==(tot_weeks-1)){
        					
        					cum_sale[j]=sum;
        					yr_mn[j]=mv_wk_sale[0];
        					j++;
        					sum=0;
        					cnt=0;
        				}
        				ln_no++;
        			}
        			br1.close();
        			
        			fnout=hdfs_dir+fout;
        			
/*hdfs filesystem        			
        			DistributedFileSystem hdfs = (DistributedFileSystem) FileSystem.get(new Configuration());
       			
        			FSDataOutputStream fsOutStream = hdfs.append(new Path(fnout));

        			outline=mv_id+comma+tot_weeks+colon;
        			for(int k=1;k<=j;k++){
        				outline=outline+k+comma+String.valueOf(cum_sale[k])+comma+yr_mn[k]+pipe;
        			}
        			
        			fsOutStream.writeChars(outline);
        			fsOutStream.hflush();
        			fsOutStream.close();
        			
        			hdfs.close();
        			
*/        			
        			for(int k=0;k<j;k++){
        				tot_sales=tot_sales+cum_sale[k];
        			}
        			for(int k=0;k<j;k++){
        				if(max<cum_sale[k]){
        					period=k;
        					max=cum_sale[k];
        					peak_time=yr_mn[k];
        				}
        			}
        			FileWriter fw=new FileWriter(fnw,true);
        			BufferedWriter bw=new BufferedWriter(fw);
        			outline=mv_id+comma+j+comma+div_f+comma+tot_weeks+comma+tot_sales+comma+max+comma+peak_time+colon;
        			for(int k=0;k<j;k++){
        				cum_wsale=cum_wsale+cum_sale[k];
        				outline=outline+k+comma+String.valueOf(cum_sale[k])+comma+cum_wsale+comma+yr_mn[k]+pipe;
        			}
        			bw.write(outline);
        			bw.newLine();
        			bw.close();
            }
		}
		catch (Exception e){
				e.printStackTrace();
		}

	}

}
