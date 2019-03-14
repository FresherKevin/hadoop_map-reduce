/**
 * @Title: Log7.java 
 * @Description:
 * 根据输入参数，判断某一天登陆的总人数
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月25日
 * @version 1.0
 */
package GameLog;

import java.io.IOException;

import javax.swing.JOptionPane;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Kevin
 *24 11000	
 */
public class Log7 {
	private static int OneDay;
	
	private static class Log7Mapper extends Mapper<Text, Text, Text, NullWritable> {
		
		private int Day=1<<(OneDay-1);//3 ,100
										//\
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int flag=Integer.parseInt(value.toString());
			//
			if ((flag & Day)==Day) {
				context.write(key, NullWritable.get());
			}
			
		}
	}
	private static class Log7Reducer extends Reducer<Text, NullWritable, IntWritable, NullWritable>{

		private IntWritable OutputValue = new IntWritable();
		private int Sum=0;
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Sum++;
		}
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void cleanup(Reducer<Text, NullWritable, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			OutputValue.set(Sum);
			context.write(OutputValue, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//arg[]代表的是参数列表，默认以空格或者逗号进行分割
		//OneDay=Integer.parseInt(args[0]);
		OneDay=Integer.parseInt(JOptionPane.showInputDialog("输入想查看第几天的登陆人数"));
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"Log7");
		job.setJarByClass(Log7.class);

		job.setMapperClass(Log7Mapper.class);
		job.setReducerClass(Log7Reducer.class);

	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		
		
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);

		
		//job.setGroupingComparatorClass(WordCountSortCompare2.class);
		//job.setSortComparatorClass(au.class);
		//job.setGroupingComparatorClass(Top20Compare.class);
		
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);

	
		FileInputFormat.addInputPath(job, new Path("/game/Authority_byte/part-r-00000"));

		Path outputpath=new Path("/game/Log7"+OneDay);

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	}
}
