package wjt.demo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/17 16:12
 */
public class ETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    //定义全局的value
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1. 获取数据
        String oriStr = value.toString();

        //2. 过滤数据
        String etlStr = ETLUtil.etlStr(oriStr);

        //3. 写出
        if (etlStr == null) {
            return;
        }
        v.set(etlStr);
        context.write(NullWritable.get(), v);
    }
}
