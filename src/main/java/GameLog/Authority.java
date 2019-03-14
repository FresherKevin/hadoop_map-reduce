/**
 * @Title: Authority.java 
 * @Description:
 * 为之设置权限可以知道他什么时候登陆过
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Kevin
 *
 */
public class Authority {
	private static class AuthorityMapper extends Mapper<Text, Text, Text, Text> {
		
		private Text outputvalue = new Text();
		@Override
		//key-value
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String []UserInfos=value.toString().split("\\s+");
			String []DateInfos=UserInfos[3].toString().split("T");
			String []DayInfos=DateInfos[0].toString().split("-");
			
			outputvalue.set(DayInfos[2]);
			context.write(key, outputvalue);
		}
		
	}
	private static class AuthorityReducer extends Reducer<Text, Text, Text, IntWritable>{
		/*
		
		
		private Text temp=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			String outputvalue = new String();
			int onecount=0;
			
			for (Text value : values) {
				outputvalue=outputvalue+value;
			}
			temp.set(outputvalue);
			context.write(key, temp);
		}
		*/
		private IntWritable OutputValue = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int flag=0;
			//直接将1移位，最后合并，比如说第一天出现则为00000001，第二天则为000000010，都出现则为000000011
			for (Text value : values) {
				int i=Integer.parseInt(value.toString());
				flag=flag|(1<<(i-1));
			}
			OutputValue.set(flag);
			context.write(key, OutputValue);
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"Authority");
		job.setJarByClass(Authority.class);

		job.setMapperClass(AuthorityMapper.class);
		job.setReducerClass(AuthorityReducer.class);

	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		//job.setGroupingComparatorClass(WordCountSortCompare2.class);
		//job.setSortComparatorClass(au.class);
		//job.setGroupingComparatorClass(Top20Compare.class);
		
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);

	
		FileInputFormat.addInputPath(job, new Path("/game_log.log"));

		Path outputpath=new Path("/game/Authority_byte");

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	}
}
