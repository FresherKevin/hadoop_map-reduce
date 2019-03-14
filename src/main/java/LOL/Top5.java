/**
 * @Title: Top5.java  
 * @Description:
 * 取出前二十
 * 取出前20（在线时间、登录次数、首次登录）
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月25日
 * @version 1.0
 */
package LOL; 

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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
public class Top5 {
	
	private static class Top5Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		//private Text OutputKey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}
	private static class  Top5Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		private int num=0;
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			if (num++<5) {
				context.write(key, NullWritable.get());
			}
		}
	}
//compare作用于什么时期
	public static class Top5Compare extends WritableComparator {
		

		public Top5Compare() {
			// TODO Auto-generated constructor stub
			super(Text.class,true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) { 
			String []aWord=((Text)a).toString().split("\\s+");
			String []bWord=((Text)b).toString().split("\\s+");
			
			String aRate=new String(aWord[2].substring(0, aWord[2].length()-1));
			System.out.println(aRate);
			String bRate=new String(bWord[2].substring(0, bWord[2].length()-1));
			
			Double aaRate=Double.parseDouble(aRate);
			Double bbRate=Double.parseDouble(bRate);
			
			Integer aCount=Integer.parseInt(aWord[3]);
			Integer bCount=Integer.parseInt(bWord[3]);
			
			if (bbRate.equals(aaRate)) {
				System.out.println(bbRate+"  "+aaRate);
				return bCount.compareTo(aCount);
			}else  return bbRate.compareTo(aaRate);
			
		
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		for (int i = 0; i < 20; i++) {
			Job job;
			//创建机架
			job=Job.getInstance(conf,"Top5");
			job.setJarByClass(Top5.class);

			job.setMapperClass(Top5Mapper.class);
			job.setReducerClass(Top5Reducer.class);

		
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			
			//job.setGroupingComparatorClass(WordCountSortCompare2.class);
			job.setSortComparatorClass(Top5Compare.class);
			job.setGroupingComparatorClass(Top5Compare.class);
			
			
			job.setInputFormatClass(TextInputFormat.class);

			//type name = new type();
			FileInputFormat.addInputPath(job, new Path("/lol/HeroOrder/HeroOrder"+i+"/part-r-00000"));

			Path outputpath=new Path("/lol/Top5/Hero"+i);

			FileSystem.get(conf).delete(outputpath,true);
			FileOutputFormat.setOutputPath(job, outputpath);

			job.waitForCompletion(true);
		}
		
	}

}
