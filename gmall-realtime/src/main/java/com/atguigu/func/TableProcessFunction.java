package com.atguigu.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.util.*;

/**
 * @author JianJun
 * @create 2021/12/27 8:55
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> outputTag;
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        System.out.println("开启任务!!!");
//        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//        System.out.println("111111111111111111");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//        System.out.println("Connection:" + connection);
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
//        System.out.println("调用广播流方法");

        //1.解析数据为JavaBean
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.建表
        //如果Sink类型为Hbase则需要创建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            creteTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkExtend());
        }


        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);

    }

    //sql:create table if not exists db.tn (id varchar primary key,name varchar,sex varchar) ...;
    private void creteTable(String sinkTable, String sinkPk, String sinkColumns, String sinkExtend) {
//        System.out.println("调用建表语句");

        //避免主键字段和建表拓展语句空指针异常
        if (sinkPk == null || sinkPk.equals("")) {
            //将主键字段默认为id
            sinkPk = "id";
        }

        if (sinkExtend == null) {
            sinkExtend = "";
        }

        //拼接建表SQL语句
        StringBuilder createSQL = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append(" (");

        //切分字段
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            //判断是否为主键字段
            if (sinkPk.equals(column)) {
                createSQL.append(column)
                        .append(" varchar primary key");
            } else {
                createSQL.append(column)
                        .append(" varchar");
            }

            //判断是否为最后一个字段
            if (i < columns.length - 1) {
                //不是最后一个字段才加","
                createSQL.append(",");
            }
        }

        //最后加上建表扩展语句
        createSQL.append(")")
                .append(sinkExtend);

        //打印建表语句
        System.out.println("建表语句为: " + createSQL);


        PreparedStatement preparedStatement = null;

        try {
            //预编译SQL
                preparedStatement = connection.prepareStatement(createSQL.toString());
                preparedStatement.execute();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("创建表:" + sinkTable + "失败!");
            }finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }


    //jsonObject:{"database":"gmall_flink","tableName":"base_trademark","before":{},"after":{"id":"",...},"type":"insert"}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
//        System.out.println("调用主流方法");

        //1.提取状态信息
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            //2.过滤字段
            filterColumn(value.getJSONObject("after"),tableProcess.getSinkColumns());

            //3.分流
            value.put("sinkTable", tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(outputTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            }

        } else {
            System.out.println("组合Key:" + key + "不存在!");
        }



    }

    private void filterColumn(JSONObject after, String sinkColumns) {
//        System.out.println("调用过滤字段方法");


        String[] columns = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = after.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            //如果after中的Key不包含在广播流的sinkColumns中则移除
            if (!columnsList.contains(next.getKey())) {
                iterator.remove();
            }
        }
    }


}
