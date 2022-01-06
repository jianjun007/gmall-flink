package com.atguigu.app.dws;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境应该设置为Kafka主题的分区数

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.使用DDL的方式创建动态表，提取事件时间生成Watermark
        String groupId = "province_stats_210726";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("create table order_wide(  " +
                "    province_id bigint,  " +
                "    province_name String,  " +
                "    province_area_code String,  " +
                "    province_iso_code String,  " +
                "    province_3166_2_code String,  " +
                "    order_id bigint,  " +
                "    split_total_amount DECIMAL,  " +
                "    create_time String,  " +
                "    rt AS TO_TIMESTAMP(create_time),  " +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND  " +
                ")with (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        //TODO 3.计算订单数&订单总金额  开窗 10秒的滚动窗口
        Table resultTable = tableEnv.sqlQuery("" +
                "select  " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,  " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,  " +
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    count(distinct order_id) order_count,  " +
                "    sum(split_total_amount) order_amount,  " +
                "    UNIX_TIMESTAMP()*1000 ts  " +
                "from order_wide  " +
                "group by   " +
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 4.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(resultTable, ProvinceStats.class);

        //TODO 5.将数据写出
        provinceStatsDataStream.print(">>>>>>>>>");
        provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats_210726 values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动任务
        env.execute("ProvinceStatsSqlApp");
    }

}
