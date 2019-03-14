/**
 * @Title: GetPartition.java  
 * @Description:
 * 对数据进行分区，0-2000，2000-4000等
 * 
 * @Copyright: Copyright (c) 2018 
 * @Company:nuaa
 * @author xck&kevin
 * @date 2018年7月23日
 * @version 1.0
 */
package kevin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.yarn.util.UTCClock;

/**
 * @author Kevin
 *
 */
//key value即map端的输出
public class GetPartitionTest extends Partitioner<IntWritable, NullWritable>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(IntWritable key, NullWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		int num = key.get();
		if (num < 20) {
			return 0;
		}else if (num<40) {
			return 1;
		}else if (num<60) {
			return 2;
		}else if (num<80) {
			return 3;
		}else return 4;
	}

}
