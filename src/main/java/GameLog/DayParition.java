/**
 * @Title: DayParition.java

 * @Description:
 * 调用分区进行分区
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import kevin.GetPartitionTest;
import kevin.WordCountPartition;
import kevin.WordCountPartition.WordCountPartitionMapper;
import kevin.WordCountPartition.WordCountPartitionReducer;

/**
 * @author Kevin
 *
 */
public class DayParition {
	public static class DayParitionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		private IntWritable outputkey = new IntWritable();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String []UserInfos=value.toString().split("\\s+");
			String []DateInfos=UserInfos[3].toString().split("T");
			String []DayInfos=DateInfos[0].toString().split("-");
			int day=Integer.parseInt(DayInfos[2]);
			outputkey.set(day);
			context.write(value, outputkey);
		}
		
	}
	public static class DayParitionReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		private IntWritable outputday = new IntWritable();
		@Override
		protected void reduce(Text text, Iterable<IntWritable> days,
				Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (IntWritable day : days) {
				context.write(text, NullWritable.get());
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job = Job.getInstance(conf, "DayPartition");
		job.setJarByClass(DayParition.class);
		
		job.setMapperClass(DayParitionMapper.class);
		job.setReducerClass(DayParitionReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/game-2017-01-01-2017-01-07(1).log"));
		//FileInputFormat.addInputPath(job, new Path("/number.log"));
		
		Path outputPath=new Path("/game/day-parition");
		FileSystem.get(conf).delete(outputPath,true);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(7);
		job.setPartitionerClass(GetDayParition.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	
	}
}


