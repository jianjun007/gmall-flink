package com.atguigu.app;

import com.atguigu.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author JianJun
 * @create 2021/12/27 21:23
 */

public class TestPhoenix {

    public static void main(String[] args) throws Exception {

//        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        PreparedStatement preparedStatement = connection.prepareStatement("select * from GMALL210726_DAU");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            System.out.println("MID:" + resultSet.getObject(1) + ",UID:" + resultSet.getObject(2));
        }

        preparedStatement.close();
        connection.close();

    }

}

