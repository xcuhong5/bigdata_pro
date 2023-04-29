package com.xc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-29 17:26
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hdp100", 11111), "example", "", "");
        while (true) {
            //2.获取连接
            canalConnector.connect();
            //3.指定要监控的数据库
            canalConnector.subscribe("fc.*");
            //4.获取Message
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() <= 0) {
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    //  获取表名
                    String tableName = entry.getHeader().getTableName();
                    //  Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //  判断entryType是否为 rowdata
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //   序列化数据, bin log 原生的 样子数据
                        ByteString storeValue = entry.getStoreValue();
                        //   反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //  获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //  获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //  遍历并打印数据
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            // 修改数据时，更新前的数据
                            JSONObject beforeData = new JSONObject();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                beforeData.put(column.getName(), column.getValue());
                            }
                            //  修改数据时，更新后的数据
                            JSONObject afterData = new JSONObject();
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            for (CanalEntry.Column column :afterColumnsList) {
                                afterData.put(column.getName(), column.getValue());
                            }
                            System.out.println("TableName:" + tableName
                                    +
                                    ",EventType:" + eventType +
                                    ",Befor:" + beforeData +
                                    ",After:" + afterData);
                        }
                    }
                }
            }
        }
    }
}

