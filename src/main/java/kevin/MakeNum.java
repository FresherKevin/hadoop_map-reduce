/**
 * Title: MakeNum.java 
 * Description:
 * 
 * 生成大量的数字
 * Copyright: Copyright (c) 2018 
 * Company:nuaa
 * @author xck
 * @date 2018年7月23日
 * @version 1.0
 */
package kevin;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author Kevin
 *
 */
public class MakeNum {
	public static void main(String[] args) throws IOException {
		FileWriter fileWriter = new FileWriter("F://代码//WorkSpace/number.log");
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		int max=10000;
		for (int i = 0; i < 1000000; i++) {
			int num = (int) (Math.random()*max);
			bufferedWriter.write(String.valueOf(num));
			bufferedWriter.newLine();
		}
		System.out.println("success");
	}
}
