/**
 * @Title: UserNum.java 
 * @Description:
 * 统计用户数量和使用次数
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月23日
 * @version 1.0
 */
package GameLog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import kevin.wordcount1;
import kevin.wordcount1.wordcount1Mapper;
import kevin.wordcount1.wordcount1reduce;

/**
 * @author Kevin
 *
 */
public class UserNum {
	
	public static int AllNum=0;
	public static int times=0;
	
	
	public static class UserNumMappper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Text outputkey=new Text();
		private IntWritable one=new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String []UserInfos=value.toString().split("\\s+");
			outputkey.set(UserInfos[0]);
			context.write(outputkey, one);
		
		}
	}
	//<a,1 1 1 1>
	public static class UserNumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values) {
				sum+=value.get();	
				times++;//使用次数加1
			}
			AllNum++;//每调用一次reduce即一次用户
			System.out.println(AllNum);
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		
		//创建机架
		job=Job.getInstance(conf,"UserNum");
		job.setJarByClass(UserNum.class);
		
		job.setMapperClass(UserNumMappper.class);
		job.setReducerClass(UserNumReducer.class);
		
		
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
	
		job.setInputFormatClass(TextInputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path("/heros.txt"));
				
		Path outputpath=new Path("/game/UserNum"+4);
		
		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);
		//System.out.println(AllNum);
		if(job.waitForCompletion(true)){
			System.out.println(AllNum+"  "+times);
			System.exit(0);
		}
		else System.exit(1);
		//System.exit(job.waitForCompletion(true)?0:1);
	}
		
}
