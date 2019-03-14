/**
 * @Title: UserBaseOnHeros.java
 * @Description:
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月27日
 * @version 1.0
 */
package LOL;

import java.io.IOException;

import javax.xml.transform.OutputKeys;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * @author Kevin
 *
 */
public class UserBaseOnHeros {
	private static class UserBaseOnHerosMapper extends Mapper<LongWritable, Text, Text, Text>{

		private Text Outputkey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String user=value.toString().split("\\s+")[2];
			Outputkey.set(user);
			context.write(Outputkey, value);
		}
	}
	
	private static class UserBaseOnHerosReducer extends Reducer<Text, Text, Text, NullWritable> {
		private Text OutputValue = new Text();
		public String hero = new String();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int winnum=0;int losenum=0;int junum=0;
			String rate;
			String Record,result,win_false,hero_a;
		
			for (Text value : values) {
				Record=value.toString().split("\\s+")[0];
				result=Record.split(",")[1];
				win_false=result.substring(0, 1);
				
				hero_a=Record.split(",")[0];
				
				hero=hero_a.substring(1, hero_a.length());
				
				int num=Integer.parseInt(win_false);
				junum++;
				if (num==0) {
					losenum++;
				}else {
					winnum++;
				}
			}
			
			rate = String.format("%.2f", (double)(winnum)/(winnum+losenum)*100);
			OutputValue.set(hero+"\t"+key+"\t"+rate+"%"+"\t"+junum);
			context.write(OutputValue, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"UserBaseOnHeros");
		job.setJarByClass(UserBaseOnHeros.class);

		job.setMapperClass(UserBaseOnHerosMapper.class);
		job.setReducerClass(UserBaseOnHerosReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);



		job.setInputFormatClass(TextInputFormat.class);


		FileInputFormat.addInputPath(job, new Path("/lol/part/part-r-00020"));

		Path outputpath=new Path("/lol/HeroOrder/HeroOrder"+20);

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
}
