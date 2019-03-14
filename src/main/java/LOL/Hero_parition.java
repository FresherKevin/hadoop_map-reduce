/**
 * @Title: Hero_parition.java 
 * @Description:
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月27日
 * @version 1.0
 */
package LOL;


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.collections.functors.IfClosure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Kevin
 *
 */
public class Hero_parition {
	
	public static HashSet<String> LOLNAME2 = new HashSet<String>();
	public static ArrayList<String> name2 = new ArrayList<String>();
	
	private static class HeroParitionMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text OutputKey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String Record=value.toString().split("\\s+")[0];
			String hero_a=Record.split(",")[0];
			String hero=hero_a.substring(1, hero_a.length());
			OutputKey.set(hero);
			LOLNAME2.add(hero);
			context.write(OutputKey, value);
		}
	}
	private static class HeroParitionReducer extends Reducer<Text, Text, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, NullWritable.get());
			}

		}
	}
	public static class PartitionPart extends Partitioner<Text, Text>{
	
	
		public void fuzhi(){
			Iterator iterator=LOLNAME2.iterator();
			while (iterator.hasNext()) {
				System.out.println("*********************");
				System.out.println("*********************");
				System.out.println(iterator.next());
				name2.add((String)iterator.next());
			}
		}

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			
			//fuzhi();
			String name = new String ();
			name=key.toString();
			//System.out.println(name);
			//System.out.println(name2.indexOf(name));
			//return name2.indexOf(name)+1;
			
			
			
			if (name.equals("Alistar")) {
				return 0;
			}
			if (name.equals("Amumu")) {
				return 1;
			}
			if (name.equals("Anivia")) {
				return 2;
			}
			if (name.equals("Annie")) {
				return 3;
			}
			if (name.equals("Ashe")) {
				return 4;
			}
			if (name.equals("ChoGath")) {
				return 5;
			}
			if (name.equals("Corki")) {
				return 6;
			}
			if (name.equals("DrMundo")) {
				return 7;
			}
			if (name.equals("Evelynn")) {
				return 8;
			}
			if (name.equals("Ezreal")) {
				return 9;
			}
			if (name.equals("Fiddlesticks")) {
				return 10;
			}
			if (name.equals("Gangplank")) {
				return 11;
			}
			if (name.equals("Irelia")) {
				return 12;
			}
			if (name.equals("Jax")) {
				return 13;
			}
			if (name.equals("Karthus")) {
				return 14;
			} 
			if (name.equals("Kassadin")) {
				return 15;
			}
			if (name.equals("Kayle")) {
				return 16;
			}
			if (name.equals("Nunu")) {
				return 17;
			}
			if (name.equals("Teemo")) {
				return 18;
			}
			if (name.equals("Trundle")) {
				return 19;
			}
			if (name.equals("Yi")) {
				return 20;
			}
		
			return numPartitions;
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job = Job.getInstance(conf, "HeroPartition");
		job.setJarByClass(Hero_parition.class);
		
		job.setMapperClass(HeroParitionMapper.class);
		job.setReducerClass(HeroParitionReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/LOL_Data"));
		//FileInputFormat.addInputPath(job, new Path("/number.log"));
		
		Path outputPath=new Path("/lol/part");
		FileSystem.get(conf).delete(outputPath,true);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(21);
		job.setPartitionerClass(PartitionPart.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	
	}

}

	
