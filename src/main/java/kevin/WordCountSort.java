/**
 * <p> Title: WordCountSort.java</p> 
 * <p> Description:</p> 
 * 对数字出现的次数进行排序
 * 次数相同，按数字本身大小进行排序
 * <p> Copyright: Copyright (c) 2018</p> 
 * <p> Company:nuaa</p>
 * @author xck
 * @date 2018年7月23日
 * @version 1.0
 */
package kevin;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Kevin
 *
 */
public class WordCountSort {
	
	public static class WordCountSortMapper extends Mapper<LongWritable	, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		/* (non-Javadoc)
			 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
			 */
			@Override
			protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				String []words=value.toString().split("\\s+");
				outputKey.set(words[0]+"=="+words[1]);
				outputValue.set(words[1]);
				context.write(outputKey, outputValue);
				
			}
	}
	
	public static class WordCountSortReducer extends Reducer<Text, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text 	key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			for (Text value : values) {
				outputValue.set(value);
			}
			String []keyOne=key.toString().split("==");
			
			outputKey.set(keyOne[0]);
			context.write(outputKey, outputValue);
		}
			
		
	}

	
	public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job = Job.getInstance(conf, "WordCountSort");
		
		job.setMapperClass(WordCountSortMapper.class);
		job.setReducerClass(WordCountSortReducer.class);
	
		//设置排序规则
		job.setSortComparatorClass(WordCountSortCompare2.class);
		
		//分区和分组考虑的排序
		job.setGroupingComparatorClass(WordCountSortCompare2.class);
		//输出格式相同
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/wordcount3/part-r-00000"));
		
		Path outputPath=new Path("/Sort");
		FileSystem.get(conf).delete(outputPath,true);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
