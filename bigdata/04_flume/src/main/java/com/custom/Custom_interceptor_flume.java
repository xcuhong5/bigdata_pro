package com.custom;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-10 15:47
 * flume 自定义 拦截器，和 Multiplexing 多路复用 配合 使用
 * 需求： 判断 数据 如果包含 xc 发送 到 channel 1；否则 发送到 channel2
 * 实现 Interceptor  接口
 */
public class Custom_interceptor_flume implements Interceptor {
    // 全局 装event 的 list
    private List<Event> addInterceptorList;

    // 初始化
    @Override
    public void initialize() {
        addInterceptorList = new ArrayList<>();
    }

    // 当个  事件 处理
    @Override
    public Event intercept(Event event) {
        // 1. 获取 head 信息、
        Map<String, String> headers = event.getHeaders();
        // 2. 获取 body，转 字符串
        String body = new String(event.getBody());
        // 3. 判断 body 是否 包含 xc 字符
        if (body.contains("xc")) {
            /*
             4. 该事件 包含 xc 字符，给head 添加 标识，发送到 channel1
             flume 根据 head 中 键值对 的 value 区分数据 发送到 哪一个管道
             下面的 配置 header = state 就是  type ，CZ US 就是xc_event ，指定 数据流向哪个 管道
              default = c4  就是默认的 ，就是此处的 other
                a1.sources = r1
                a1.channels = c1 c2 c3 c4
                a1.sources.r1.selector.type = multiplexing
                a1.sources.r1.selector.header = state
                a1.sources.r1.selector.mapping.CZ = c1
                a1.sources.r1.selector.mapping.US = c2 c3
                a1.sources.r1.selector.default = c4
             */
            headers.put("type", "xc_event");
        } else {
            // 5. 不包含xc 的 数据 发送 给 channel2
            headers.put("type", "other");
        }
        return event;
    }

    // 6. 批量 事件 处理
    @Override
    public List<Event> intercept(List<Event> events) {
        // 先清空 list
        addInterceptorList.clear();
        // 遍历 event
        for (Event event : events) {
            // 调用 上面 写的 单个 event 处理方法
            addInterceptorList.add(intercept(event));
        }

        return addInterceptorList;
    }

    @Override
    public void close() {

    }

    // 7. 将 自定义 过滤器 类 build
    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new Custom_interceptor_flume();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
