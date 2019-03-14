/**
 * @Title: AddData.java
 * @Description:
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月26日
 * @version 1.0
 */
package LOL;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class AddData {
	private static class AddDataMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);

		}
	}
	private static class AddDataReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
		
		private Text OutputValue = new Text();
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				int max=20;
				int KillNum=(int) (Math.random()*max);
				int DeathNum=(int) (Math.random()*max);
				int HelpNum=(int) (Math.random()*max);
				OutputValue.set(value+"\t"+KillNum+"/"+DeathNum+"/"+HelpNum);
				context.write(OutputValue, NullWritable.get());
			}
			
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"lol");
		job.setJarByClass(AddData.class);

		job.setMapperClass(AddDataMapper.class);
		job.setReducerClass(AddDataReducer.class);

	
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		
		//job.setGroupingComparatorClass(WordCountSortCompare2.class);
		//job.setSortComparatorClass(au.class);
		//job.setGroupingComparatorClass(Top20Compare.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);

	
		FileInputFormat.addInputPath(job, new Path("/heros.txt"));

		Path outputpath=new Path("/lol/newdata");

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	
	}

}
