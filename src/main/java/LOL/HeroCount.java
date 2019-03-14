/**
 * <p> Title: wordcount1.java</p>  
 * <p> Description:</p> 
 * <p> Copyright: Copyright (c) 2018</p> 
 * <p> Company:nuaa</p>
 * @author xck
 * @date 2018年7月13日
 * @version 1.0
 */
package LOL;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import javax.servlet.jsp.tagext.TryCatchFinally;
import javax.xml.transform.OutputKeys;

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







/**
 * @author Kevin
 *
 */

public class HeroCount {
	public static HashSet<String> LOLNAME = new HashSet<String>();
	
	public static class HeroCountMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

		private Text outputkey=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String Record=value.toString().split("\\s+")[0];
			String hero_a=Record.split(",")[0];
			String hero=hero_a.substring(1, hero_a.length());
			outputkey.set(hero);
			context.write(outputkey, NullWritable.get());
			}
		
		}
		

	 public static class HeroReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		 
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		
			LOLNAME.add(key.toString());
		
			context.write(key, NullWritable.get());
			
		}
		
	public static void main(String []args){
		
	try {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		
		//创建机架
		job=Job.getInstance(conf,"wordcount1");
		job.setJarByClass(HeroCount.class);
		
		job.setMapperClass(HeroCountMapper.class);
		job.setReducerClass(HeroReducer.class);
		
		
		/**
		 * 设置map和reduce的输出格式
		 * 如果两者相同 则只需设置一个
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
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
		
		FileInputFormat.addInputPath(job, new Path("/heros.txt"));
				
		Path outputpath=new Path("/wordcount5");
		
		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);
		if (job.waitForCompletion(true)) {
			Iterator name = LOLNAME.iterator();
			while (name.hasNext()) {
				System.out.println(name.next());
				
			}
			System.exit(0);
		}
		//System.exit(job.waitForCompletion(true)?0:1);
		else System.exit(1);
		
		} catch (Exception e) {	
			// TODO: handle exception
			e.printStackTrace();
		}
	
	}
}
}
