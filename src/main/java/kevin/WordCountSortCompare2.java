/**
 * <p> Title: WordCountSortCompare.java</p>  
 * <p> Description:</p> 
 * 
 * 进行二次排序，即出现次数相同的按照大小在来一次排序
 * <p> Copyright: Copyright (c) 2018</p> 
 * <p> Company:nuaa</p>
 * @author xck
 * @date 2018年7月23日
 * @version 1.0
 */
package kevin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Kevin
 *
 */

//11==8 8 >>>> 11 8
public class WordCountSortCompare2 extends WritableComparator{


	
	public WordCountSortCompare2() {
		// TODO Auto-generated constructor stub
		super(Text.class,true);
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	//任意两个map段的输出的key例如11==8，12==9
	public int compare(WritableComparable a, WritableComparable b) {
		
		Text akey = (Text)a;
		Text bkey = (Text)b;
		
		//拆分11==8，12==9
		//8
		String avalue=akey.toString().split("==")[1];
		//9
		String bvalue=bkey.toString().split("==")[1];
		//11
		String ak=akey.toString().split("==")[0];
		//12
		String bk=bkey.toString().split("==")[0];
		//string转换成Integer
		Integer aint = Integer.parseInt(avalue);
		Integer bint = Integer.parseInt(bvalue);
		
		Integer akint = Integer.parseInt(ak);
		Integer bkint = Integer.parseInt(bk);

		//进行排序
		if (aint.equals(bint)) {
			return akint.compareTo(bkint);	
		} else {
			return aint.compareTo(bint);
		}
		
	}
}
