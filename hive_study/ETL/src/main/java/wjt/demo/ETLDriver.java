package wjt.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/17 17:04
 */
public class ETLDriver implements Tool {

    private Configuration configuration;

    public int run(String[] args) throws Exception {

        //1. 获取Job对象
        Job job = Job.getInstance(configuration);

        //2. 设置Jar包路径
        job.setJarByClass(ETLDriver.class);

        //3. 设置Mapper类&输出KV类型
        job.setMapperClass(ETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //4. 设置最终输出的KV类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //5. 设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6. 提交任务
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public void setConf(Configuration conf) {

        configuration = conf;
    }

    public Configuration getConf() {

        return configuration;
    }

    public static void main(String[] args) {

        //构建配置信息
        Configuration configuration = new Configuration();
        try {
            int run = ToolRunner.run(configuration, new ETLDriver(), args);
            System.out.println(run);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
