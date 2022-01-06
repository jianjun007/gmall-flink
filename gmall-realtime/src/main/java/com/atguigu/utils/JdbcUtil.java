package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author JianJun
 * @create 2021/12/29 14:37
 */
public class JdbcUtil {
    public static <T> List<T> queryData(Connection connection, String sql, Class<T> clz, Boolean toCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {

        //创建返回对象
        ArrayList<T> resultList = new ArrayList<>();

        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //遍历resultSet,将每行数据转换为T对象
        while (resultSet.next()) {
            //创建T对象
            T t = clz.newInstance();

            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(columnName);

                if (toCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());

                }

                //给T对象赋值
                BeanUtils.setProperty(t, columnName, value);
            }

            //将T对象存放至集合
            resultList.add(t);
        }

        resultSet.close();
        preparedStatement.close();

        return resultList;

    }

    public static void main(String[] args) throws SQLException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> jsonObjects = queryData(connection,
                "select * from GMALL_210726_REALTIME.DIM_BASE_TRADEMARK",
                JSONObject.class,
                false);

        for (JSONObject jsonObject : jsonObjects) {

            System.out.println(jsonObject);
        }

        connection.close();

    }
}
