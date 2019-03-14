/**
 * Title: WordCountPartition.java 
 * Description:
 * 
 * 对一百万的数据进行分区
 * Copyright: Copyright (c) 2018 
 * Company:nuaa
 * @author xck
 * @date 2018年7月23日
 * @version 1.0
 */
package kevin;

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
import org.apache.xerces.impl.dv.xs.AbstractDateTimeDV;

import kevin.WordCountSort.WordCountSortMapper;
import kevin.WordCountSort.WordCountSortReducer;

/**
 * @author Kevin
 *
 */
public class WordCountPartition {
	public static class WordCountPartitionMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable>{
		private IntWritable outputkey = new IntWritable();
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String []words=value.toString().split("\\s+");
			for (String word : words) {
				
				outputkey.set(Integer.parseInt(word));
				System.out.println(outputkey.get());
			}
			
			context.write(outputkey, NullWritable.get());
			
		}
	}
	public static class WordCountPartitionReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		//Iterable  这个迭代器还会对int型的进行排序
		protected void reduce(IntWritable key, Iterable<NullWritable> values,
				Reducer<IntWritable, NullWritable, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("**************************");
			System.out.println(key);
			for (NullWritable value : values) {
				context.write(key, NullWritable.get());
			}
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job = Job.getInstance(conf, "WordCountPartition");
		job.setJarByClass(WordCountPartition.class);
		
		job.setMapperClass(WordCountPartitionMapper.class);
		job.setReducerClass(WordCountPartitionReducer.class);
	
		//设置排序规则
		//job.setSortComparatorClass(WordCountSortCompare2.class);
		
		//分区和分组考虑的排序
		//job.setGroupingComparatorClass(WordCountSortCompare2.class);
		//输出格式相同
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/it.log"));
		//FileInputFormat.addInputPath(job, new Path("/number.log"));
		
		Path outputPath=new Path("/word-partition-test");
		FileSystem.get(conf).delete(outputPath,true);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(5);
		job.setPartitionerClass(GetPartitionTest.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	
	}
}
