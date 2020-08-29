package wjt.demo;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: wanjintao
 * @time: 2020/8/29 11:45
 */
public class myInterceptor implements Interceptor {

    //声明一个存放事件的集合
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {

        //初始化存放事件的集合
        addHeaderEvents = new ArrayList<>();

    }

    //单个事件拦截
    @Override
    public Event intercept(Event event) {

        //1. 获取事件中的头信息
        Map<String, String> headers = event.getHeaders();

        //2. 获取事件中的body信息
        String body = new String(event.getBody());

        //3. 根据body中是否有“Hello”来决定是否添加头信息
        if (body.contains("hello")) {

            //4. 有hello添加“wan”头信息
            headers.put("type", "wan");

        } else {

            //4. 没有hello添加“tao”头信息
            headers.put("type", "tao");

        }

        return event;
    }

    //批量事件拦截
    @Override
    public List<Event> intercept(List<Event> events) {

        //1. 清空集合
        addHeaderEvents.clear();

        //2. 遍历events
        for (Event event : events) {

            //3. 给每一个事件添加头信息
            addHeaderEvents.add(intercept(event));

        }

        //4. 返回结果
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new myInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
