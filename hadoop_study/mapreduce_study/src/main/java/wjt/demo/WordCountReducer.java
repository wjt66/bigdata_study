package wjt.demo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/6 20:19
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    /*
     四个范型解释：
     keyin:        k2类型
     VALUEIN:      v2类型
     KEYOUT:       k3类型
     VALUEOUT:     v3类型
     */

    //reduce方法作用：将新的k2和v2转化为k3和v3，将k3和v3写入上下文当中
    /*
     参数：
     key:        新k2
     values:     集合新的v2
     context:    上下文对象
     --------------------------------------------
     如何将新的k2和v2转化为k3和v3
     新     k2           v2
           hello        <1,1,1>
           world         <1,1>
           hadoop       <1>
     --------------------------------------------
            k3           v3
           hello          3
           world          2
           hadoop         1
     */


    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;

        //1. 遍历集合，将集合中的数字相加，得到v3
        for (LongWritable value : values) {
            count += value.get();
        }

        //2. 将k3和v3写入上下文中
        context.write(key, new LongWritable(count));
    }
}
