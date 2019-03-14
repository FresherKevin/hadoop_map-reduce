/**
 * @Title: GetDayParition.java 
 * @Description:
 * 
 * 按时间进行分区
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月23日
 * @version 1.0
 */
package GameLog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Kevin
 *
 */
public class GetDayParition extends Partitioner<Text, IntWritable>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		int num=value.get();
		if (num==1) {
			return 0;
		}else if (num==2) {
			return 1;
		}else if (num==3) {
			return 2;
		}else if (num==4) {
			return 3;
		}else if (num==5) {
			return 4;
		}else if (num==6) {
			return 5;
		}
		else return 6;
	}

}
