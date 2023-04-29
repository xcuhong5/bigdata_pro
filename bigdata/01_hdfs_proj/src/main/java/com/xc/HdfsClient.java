package com.xc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created with IntelliJ IDEA.
 * Description:bigdata
 *
 * @Author: sky
 * DateTime: 2022-11-03 21:23
 * hdfs API
 */
public class HdfsClient {
    //全局 调用 ，提出来 公共 调用
    private DistributedFileSystem distribut_hdfs;

    // 测试 方法执行之前 执行 此方法进行 初始化
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // hadoop 集群 地址
        URI uri = new URI("hdfs://xccluster");
        // 创建 配置 对象
        Configuration conf = new Configuration();
        // 设置 副本数
        conf.set("dfs.replication", "3");
        // hadoop 用户
        String user = "xc";
        // 从 DistributedFileSystem  分布式文件系统  调用 get 函数 获取文件对象
        distribut_hdfs = (DistributedFileSystem) FileSystem.get(uri, conf, user);
    }

    // 测试方法 执行 之后 执行 ；该方法 关闭 文件系统 对象
    @After
    public void closeFileSystem() throws IOException {
        distribut_hdfs.close();
    }
}
