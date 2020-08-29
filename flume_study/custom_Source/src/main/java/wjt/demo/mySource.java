package wjt.demo;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/29 12:25
 */
public class mySource extends AbstractSource implements Configurable, PollableSource {

    //定义全局的前缀&后缀
    private String prefix;
    private String subfix;

    @Override
    public void configure(Context context) {

        //读取配置信息，给前缀和后缀赋值
        prefix = context.getString("aaa");
        subfix = context.getString("bbb", "wanjintao");

    }

    /**
     * 1. 接收数据（for循环啊造数据）
     * 2. 封装为事件
     * 3. 将事件传给Channel
     */
    @Override
    public Status process() throws EventDeliveryException {

        Status status = null;

        //1. 接收数据
        try {
            for (int i = 0; i < 5; i++) {

                //2. 构建事件对象
                SimpleEvent event = new SimpleEvent();

                //3. 给事件设置值
                event.setBody((prefix + "--" + i + "--" + subfix).getBytes());

                //4. 将事件传给Channel
                getChannelProcessor().processEvent(event);

                status = Status.READY;

            }
        } catch (Exception e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //返回结果
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

}
