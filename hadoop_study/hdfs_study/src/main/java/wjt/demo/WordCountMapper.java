package wjt.demo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/6 19:56
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    
    //map方法就是将k1和v1转化为k2和v2
    /*
       参数：
           key      :k1的行偏移量
           value    :v1每一行的文本
           context  :表示上下文对象
     -------------------------------------------
     如何将k1和v1转化为k2和v2
     k1         v2
     0      hello,world,hadoop
     15     hdfs,hive,hello
     -------------------------------------------
     k2         v2
     hello       1
     world       1
     hadoop      1
     hdfs        1
     hive        1
     hello       1
     */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();

        //1. 将一行的文本数据进行拆分
        String[] split = value.toString().split(",");

        //2. 遍历数组，组装k2和v2
        for (String word : split) {
            //3. 将k2和v2写入上下文
            text.set(word);
            longWritable.set(1);
            context.write(text, longWritable);
        }

    }
}
