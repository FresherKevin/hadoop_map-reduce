/**
 * @Title: Reten.java
 * @Description:
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月24日
 * @version 1.0
 */
package GameLog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import GameLog.NewOld.NewOldReducer;
import GameLog.NewOld.NewOldrMapper;

/**
 * @author Kevin
 *
 */
public class Reten {
	private static class RetenMapper extends Mapper<Text, Text, Text, Text>{
		
		private Text OutputValue = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] valuespilt = value.toString().split("\\s+");
			String[] Datesplit = valuespilt[2].split("T");
			String day=Datesplit[0].split("-")[2];
			OutputValue.set(day);
			context.write(key, OutputValue);
			
		}
	}
	private static class RetenReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		private Text OutputKey = new Text();
		private int TwoLoadNum = 0;
		private int TwoAll=0;
		private int ThreeAll=0;
		private int SevenAll=0;
		private int ThreeLoadNum = 0;
		private int SevenLoadNum = 0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
	
			boolean []mark=new boolean[7];
			for (boolean b : mark) {
				b=false;
			}
			SevenAll++;
			for (Text value : values) {
				String valueday = value.toString();
			
				if ("01".equals(valueday)) {
					mark[0]=true;			
				}
				if ("02".equals(valueday)) {
					mark[1]=true;
				}
				if ("03".equals(valueday)) {
					mark[2]=true;
				}
				if ("04".equals(valueday)) {
					mark[3]=true;
				}
				if ("05".equals(valueday)) {
					mark[4]=true;
				}
				if ("06".equals(valueday)) {
					mark[5]=true;
				}
				if ("07".equals(valueday)) {
					mark[6]=true;
				}
			}
			if (mark[6]||mark[5]) {
				TwoAll++;
			}
			if (mark[6]||mark[5]||mark[4]) {
				ThreeAll++;
			}
			
			if (mark[6]==true&&mark[5]==true) {//计算两天留存率
				TwoLoadNum++;
			} 
			if (mark[6]==true&&mark[5]==true&&mark[4]==true) {
				ThreeLoadNum++;
			}
			if (mark[6]==true&&mark[5]==true&&mark[4]==true&&
					mark[3]==true&&mark[2]==true&&mark[1]==true&&mark[0]==true) {
				SevenLoadNum++;
			}
		}
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String TwoRate = String.format("%.2f", (double)TwoLoadNum/TwoAll*100);
			String ThreeRate = String.format("%.2f", (double)ThreeLoadNum/ThreeAll*100);
			String SevenRate = String.format("%.2f", (double)SevenLoadNum/SevenAll*100);
			OutputKey.set("两日留存率："+TwoRate+"%"+"三日留存率："+ThreeRate+"%"+"七日留存率："+SevenRate+"%");
			context.write(OutputKey, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"Retention");
		job.setJarByClass(Reten.class);

		job.setMapperClass(RetenMapper.class);
		job.setReducerClass(RetenReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);



		job.setInputFormatClass(KeyValueTextInputFormat.class);


		FileInputFormat.addInputPath(job, new Path("/game_log.log"));
		/*
		Path InputPath1 = new Path("/game/day-parition/part-r-00000");
		Path InputPath2 = new Path("/game/day-parition/part-r-00001");
		FileInputFormat.addInputPath(job, InputPath1);
		FileInputFormat.addInputPath(job, InputPath2);s
		*/
		Path outputpath=new Path("/game/Retention");

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}
