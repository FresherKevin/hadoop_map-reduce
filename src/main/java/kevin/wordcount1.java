/**
 * <p> Title: wordcount1.java</p>  
 * <p> Description:</p> 
 * <p> Copyright: Copyright (c) 2018</p> 
 * <p> Company:nuaa</p>
 * @author xck
 * @date 2018年7月13日
 * @version 1.0
 */
package kevin;

import java.io.IOException; 

import javax.servlet.jsp.tagext.TryCatchFinally;
import javax.xml.transform.OutputKeys;

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

import io.netty.handler.codec.http.HttpHeaders.Values;





/**
 * @author Kevin
 *
 */
public class wordcount1 {
	public static class wordcount1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		private static final IntWritable one=new IntWritable(1);
		private Text ouputkey=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String []words=value.toString().split("\\s+");
			for (String string : words) {
				//outputkeys.set(string);
				/*if (Integer.parseInt(string)>85) {
					ouputkey.set(string);
					context.write(ouputkey,one);
				}*/
				ouputkey.set(string);
				context.write(ouputkey,one);
				
				/*
				char a[]=new char[56];
				a=string.toCharArray();
				
				for (int i = 0; i < a.length; i++) {
					char temp=a[i];
					if (temp=='l'||temp=='s') {
						ouputkey.set(string);
						context.write(ouputkey,one);
						break;
					}
					
				}*/
				
			}
		
		}
		
	
	}
	 public static class wordcount1reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		 
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum+=value.get();
				result.set(sum);
				context.write(key, result);
			}
			
		}



	public static void main(String []args){
		
	try {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		
		//创建机架
		job=Job.getInstance(conf,"wordcount1");
		job.setJarByClass(wordcount1.class);
		
		job.setMapperClass(wordcount1Mapper.class);
		job.setReducerClass(wordcount1reduce.class);
		
		
		/**
		 * 设置map和reduce的输出格式
		 * 如果两者相同 则只需设置一个
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		/**这是对数据的处理方式
		 * TextInputFormat
		 * aa bb cc <0,aa bb cc>  
		 * aa dd ee <1,aa dd ee>
		 * 
		 * KeyValueTextInputFormat
		 * aa bb cc <aa,bb cc>   	
		 * aa dd ee <aa,dd ee>
		 */
		job.setInputFormatClass(TextInputFormat.class);
		
		//设置文件的输入和输出路径
		
		//所有的包都应该是mapreduce
		
		FileInputFormat.addInputPath(job, new Path("/Sort.txt"));
				
		Path outputpath=new Path("/wordcount4");
		
		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
		} catch (Exception e) {	
			// TODO: handle exception
			e.printStackTrace();
		}
	
	}
}
}
