package wjt.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/6 20:49
 */
public class JobMain extends Configured implements Tool {

    //该方法用于指定一个job任务
    public int run(String[] args) throws Exception {
        //1. 用于创建一个job对象
        Job job = Job.getInstance(super.getConf(), "wordcount");

        //2. 配置job任务对象(八个步骤)

        //第一步:指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://westgis181:9000/wjt/test001.txt") );

        //第二步:指定map阶段的处理方式和数据类型
        job.setMapperClass(WordCountMapper.class);
        //设置map阶段k2的类型
        job.setMapOutputKeyClass(Text.class);
        //设置map阶段v2的类型
        job.setMapOutputValueClass(LongWritable.class);

        //第三、四、五、六步采用默认方式不做处理

        //第七步:指定reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReducer.class);
        //设置reduce阶段k3的类型
        job.setOutputKeyClass(Text.class);
        //设置reduce阶段v3的类型
        job.setOutputValueClass(LongWritable.class);

        //第八步:设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出的路径
        TextOutputFormat.setOutputPath(job, new Path("hdfs://westgis181:9000/wjt/test005_out"));

        //等待任务结束
        boolean b1 = job.waitForCompletion(true);

        return b1 ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        //启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}