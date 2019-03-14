/**
 * @Title: DeatNum.java
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
public class DeathNum {
	private static class DeatNumMapper extends Mapper<LongWritable, Text, Text, Text>{

		private Text Outputkey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String Record=value.toString().split("\\s+")[0];
			String hero_a=Record.split(",")[0];
			String hero=hero_a.substring(1, hero_a.length());
			Outputkey.set(hero);
			context.write(Outputkey, value);
		}
	}
	
	private static class DeatNumReducer extends Reducer<Text, Text, Text, NullWritable> {
		private Text OutputValue = new Text();
	//	public String hero = new String();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int alldeathnum=0;
			for (Text value : values) {
				String []aWord=value.toString().split("\\s+");
				String death=aWord[3].split("/")[1];
				int deathnum=Integer.parseInt(death);
				alldeathnum+=deathnum;
			}
			OutputValue.set(key+"\t"+alldeathnum);
			context.write(OutputValue, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"DeatNum");
		job.setJarByClass(DeathNum.class);

		job.setMapperClass(DeatNumMapper.class);
		job.setReducerClass(DeatNumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);



		job.setInputFormatClass(TextInputFormat.class);


		FileInputFormat.addInputPath(job, new Path("/LOL_Data"));

		Path outputpath=new Path("/lol/deathOrder");

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
}
