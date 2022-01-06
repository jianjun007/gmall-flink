package com.atguigu.common;

/**
 * @author JianJun
 * @create 2021/12/27 9:21
 */
public class GmallConfig {

    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_210726_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}

