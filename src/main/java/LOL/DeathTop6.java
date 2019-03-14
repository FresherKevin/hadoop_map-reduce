/**
 * @Title: DeathTop6.java  
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
public class DeathTop6 {
	
	private static class DeathTop6Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		//private Text OutputKey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}
	private static class  DeathTop6Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		//private int num=0;
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
				context.write(key, NullWritable.get());
		}
	}
//compare作用于什么时期
	public static class DeathTop6Compare extends WritableComparator {
		

		public DeathTop6Compare() {
			// TODO Auto-generated constructor stub
			super(Text.class,true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) { 
			String []aWord=((Text)a).toString().split("\\s+");
			String []bWord=((Text)b).toString().split("\\s+");
			Integer anum=Integer.parseInt(aWord[1]);
			Integer bnum=Integer.parseInt(bWord[1]);
			
			return bnum.compareTo(anum);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
			Job job;
			//创建机架
			job=Job.getInstance(conf,"DeathTop6");
			job.setJarByClass(DeathTop6.class);

			job.setMapperClass(DeathTop6Mapper.class);
			job.setReducerClass(DeathTop6Reducer.class);

		
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			
			//job.setGroupingComparatorClass(WordCountSortCompare2.class);
			job.setSortComparatorClass(DeathTop6Compare.class);
			job.setGroupingComparatorClass(DeathTop6Compare.class);
			
			
			job.setInputFormatClass(TextInputFormat.class);

			//type name = new type();
			FileInputFormat.addInputPath(job, new Path("/lol/deathOrder/part-r-00000"));

			Path outputpath=new Path("/lol/DeathTop6");

			FileSystem.get(conf).delete(outputpath,true);
			FileOutputFormat.setOutputPath(job, outputpath);

			job.waitForCompletion(true);
	
		
	}

}
