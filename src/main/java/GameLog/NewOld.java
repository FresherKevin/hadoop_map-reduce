/**
 * @Title: NewOld.java 
 * @Description:
 * 统计第二天相对于第一天来说的新老用户
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

import GameLog.UserInfo.UserInfoMapper;
import GameLog.UserInfo.UserInfoReducer;

/**
 * @author Kevin
 *
 */
public class NewOld {
	public static class NewOldrMapper extends Mapper<Text, Text, Text, Text>{
		//e12a2d17-ac78-4a35-a183-a1ff9b89e4b1	Android	5.0	2017-01-01T00:00		2017-01-01T18:00:10	64810
		private Text OutputValue = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] valuespilt = value.toString().split("\\s+");
			String[] Datesplit = valuespilt[2].split("T");
			String day=Datesplit[0].split("-")[2];
			OutputValue.set(day);
			context.write(key, OutputValue);
		}
	}
	public static class NewOldReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		private Text OutputKey = new Text();
		private int NewNum=0;
		private int OldNum=0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			boolean FirstDay = false;
			boolean SecondDay = false;
			for (Text value : values) {
				String day = value.toString();
				if ("01".equals(day)) {
					FirstDay=true;
				}
				else if ("02".equals(day)) {	
					SecondDay=true;
				}
			}
			if (FirstDay && SecondDay) {
				OldNum++;
			}else if (!FirstDay&&SecondDay) {//第一天登入，第二天不登入不统计在内
				NewNum++;
			}
		}
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			OutputKey.set("新用户："+NewNum+"老用户："+OldNum);
			context.write(OutputKey, NullWritable.get());
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://master:9000");
		Job job;
		//创建机架
		job=Job.getInstance(conf,"NewOld");
		job.setJarByClass(NewOld.class);

		job.setMapperClass(NewOldrMapper.class);
		job.setReducerClass(NewOldReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);



		job.setInputFormatClass(KeyValueTextInputFormat.class);


		//FileInputFormat.addInputPath(job, new Path("/game_log.log"));
		Path InputPath1 = new Path("/game/day-parition/part-r-00000");
		Path InputPath2 = new Path("/game/day-parition/part-r-00001");
		FileInputFormat.addInputPath(job, InputPath1);
		FileInputFormat.addInputPath(job, InputPath2);
		
		Path outputpath=new Path("/game/NewOld");

		FileSystem.get(conf).delete(outputpath,true);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.exit(job.waitForCompletion(true)?0:1);
	}
		
	

}
