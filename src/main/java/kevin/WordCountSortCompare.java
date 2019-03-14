/**
 * <p> Title: WordCountSortCompare.java</p> 

 * <p> Description:</p> 
 * 
 * 进行排序，排序依据为出现的次数，顺序或逆序由重写的compare决定，但是并没有进行二次排序
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
public class WordCountSortCompare extends WritableComparator{


	public WordCountSortCompare() {
		// TODO Auto-generated constructor stub
		super(Text.class,true);
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		Text akey = (Text)a;
		Text bkey = (Text)b;
		
		String avalue=akey.toString().split("==")[1];
		String bvalue=bkey.toString().split("==")[1];
		Integer aint = Integer.parseInt(avalue);
		Integer bint = Integer.parseInt(bvalue);

		if (aint.equals(bint)) {
			return 1;	
		} else {
			return aint.compareTo(bint);
		}
		
	}
}
