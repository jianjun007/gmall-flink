package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * @author JianJun
 * @create 2021/12/29 19:54
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String table, String key) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {
        //查询Redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":" + key;
        String jsonStr = jedis.get(redisKey);
        if (jsonStr != null) {
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return JSONObject.parseObject(jsonStr);
        }

        //拼接SQL
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." +
                table + " where id = '" + key + "'";
        System.out.println("查询SQL为:" + sql);

        //查询数据
        List<JSONObject> list = JdbcUtil.queryData(connection, sql, JSONObject.class, false);

        //写入Redis
        JSONObject dimInfo = list.get(0);
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //返回结果
        return dimInfo;

    }

    public static void delDimInfo(String table, String key) {
        String redisKey = "DIM:" + table + ":" + key;
        Jedis jedis = RedisUtil.getJedis();

        jedis.del(redisKey);

        jedis.close();
    }

    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);



        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,"DIM_BASE_TRADEMARK", "1"));
        long end = System.currentTimeMillis();
        System.out.println(end - start);

        System.out.println(getDimInfo(connection,"DIM_BASE_TRADEMARK", "5"));
        long end1 = System.currentTimeMillis();
        System.out.println(end1 -end);

        connection.close();
    }

}
