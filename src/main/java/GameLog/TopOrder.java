/**
 * @Title: TopOrder.java 
 * @Description:
 * 用户排序
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月25日
 * @version 1.0
 */
package GameLog;

import java.io.IOException;
import java.time.Duration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import GameLog.UserInfo.UserInfoMapper;
import GameLog.UserInfo.UserInfoReducer;

/**
 * @author Kevin
 *
 *每个用户的id、在线时间、次数、首登
 */
public class TopOrder {
	private static class TopOrderMapper extends Mapper<Text, Text, Text, Text>{

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	private static class TopOrderReducer extends Reducer<Text, Text, Text, NullWritable> {
		private Text OutputKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			long AllDuration=0;
			int AllTimes=0;
			String FirstLoad = null;
			for (Text value : values) {
				String []words=value.toString().split("\\s+");
				AllTimes++;
				AllDuration+=Long.parseLong(words[4]);
				if (AllTimes==1) {
					FirstLoad=words[2];
				}
			}
			OutputKey.set(key+"\t"+AllDuration+"\t"+AllTimes+"\t"+FirstLoad);
			context.write(OutputKey, NullWritable.get());
			
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"TopOrder");
		job.setJarByClass(TopOrder.class);

		job.setMapperClass(TopOrderMapper.class);
		job.setReducerClass(TopOrderReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);



		job.setInputFormatClass(KeyValueTextInputFormat.class);


		FileInputFormat.addInputPath(job, new Path("/game/day-parition/"));

		Path outputpath=new Path("/game/TopOrder");

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	}
		
	

}
