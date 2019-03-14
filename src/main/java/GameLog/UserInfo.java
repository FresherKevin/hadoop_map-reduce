/**
 * @Title: UserInfo.java 
 * @Description:
 * 
 * 统计总用户量，使用次数和总时长
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月24日
 * @version 1.0
 */
package GameLog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import GameLog.UserNum.UserNumMappper;
import GameLog.UserNum.UserNumReducer;

/**
 * @author Kevin
 *
 */
public class UserInfo {
	public static class UserInfoMapper extends Mapper<Text, Text, Text, Text>{

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	public static class UserInfoReducer extends Reducer<Text, Text,Text , NullWritable>{

		private Text OutputKey = new Text();
		//总用户量
		private int TotalObject = 0;
		//总次数
		private int TotalTimes = 0;
		//总时长
		private long AllTime = 0;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			TotalObject++;
			for (Text value : values) {
				TotalTimes++;

				AllTime=AllTime+Long.parseLong(value.toString().split("\\s+")[4]);
			}

		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);	
			OutputKey.set("总用户量："+TotalObject+"总使用次数："+TotalTimes+"人均使用时间："+AllTime/TotalObject+"次均使用时长："+AllTime/TotalTimes);
			context.write(OutputKey, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"UserInfo");
		job.setJarByClass(UserInfo.class);

		job.setMapperClass(UserInfoMapper.class);
		job.setReducerClass(UserInfoReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);



		job.setInputFormatClass(KeyValueTextInputFormat.class);


		FileInputFormat.addInputPath(job, new Path("/game_log.log"));

		Path outputpath=new Path("/game/Userinfo");

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}


