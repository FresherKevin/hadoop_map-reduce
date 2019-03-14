/**
 * @Title: CacKDA.java 
 * @Description:
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月27日
 * @version 1.0
 */
package LOL;

import java.io.IOException;

import javax.xml.transform.Source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.Q;


/**
 * @author Kevin
 *
 */
public class CacKDA {
	private static class CacKDAMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		private Text OutputKey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String []aWord=value.toString().split("\\s+");
			OutputKey.set(aWord[2]);
			context.write(OutputKey, value);
		}
	}
	private static class CacKDAReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text OutputKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int allkill=0;int alldeath=0;int allhelp=0;
			double maxkda=0.0;double minkda=100.0; 
			
			for (Text value : values) {
				String []aWord=value.toString().split("\\s+");
				String death=aWord[3].split("/")[1];
				String kill=aWord[3].split("/")[0];
				
				String help=aWord[3].split("/")[2];
				int deathnum=Integer.parseInt(death);
				allkill=allkill+Integer.parseInt(kill);
				alldeath=alldeath+Integer.parseInt(death);
				allhelp=allhelp+Integer.parseInt(help);
				
				
				if (deathnum==0) {
					deathnum=1;
				}
				double temponekda=1.0*(Integer.parseInt(kill)+Integer.parseInt(help))/deathnum;
				
				if(temponekda>=maxkda){
					maxkda=temponekda;
					//System.out.println(maxkda);
				}
				if (temponekda<=minkda) {
					minkda=temponekda;
				}
			}
			
			double tempkda=1.0*(allkill+allhelp)/alldeath;
			String kda=String.format("%.2f", tempkda);
			String kdamax=String.format("%.2f", maxkda);
			String kdamin=String.format("%.2f", minkda);
			OutputKey.set("总KDA："+kda+"\t"+"最高KDA："+kdamax+"\t"+"最低KDA："+kdamin);
			context.write(key, OutputKey);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"CacKDA");
		job.setJarByClass(CacKDA.class);

		job.setMapperClass(CacKDAMapper.class);
		job.setReducerClass(CacKDAReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);



		job.setInputFormatClass(TextInputFormat.class);


		FileInputFormat.addInputPath(job, new Path("/LOL_Data"));

		Path outputpath=new Path("/lol/UserKDA");

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}
