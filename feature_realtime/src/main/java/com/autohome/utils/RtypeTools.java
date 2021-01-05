package com.autohome.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName RtypeTools
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/05 19:11
 **/
public class RtypeTools {

    private static final Logger logger = LoggerFactory.getLogger(RtypeTools.class);

    public static Map<String,String> getRTypeData(){
        Map<String,String> rTypeInner = new HashMap<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://dp-mw0-3306-db.lq.autohome.com.cn/bdp_recommend_operate", "bdp_recommend_operate_read", "BvO4dAOsZmdxjgQH");
            preparedStatement = connection.prepareStatement("select biz_type, code from r_type_mapping");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                rTypeInner.put(resultSet.getString("biz_type"), resultSet.getString("code"));
            }
        }catch (SQLException e){
            logger.info("SQLException: {}", e);
        }catch (Exception ee){
            logger.info("ComException: {} -> {}",ee.getMessage(), ee);
        }finally {
            try {
                if(preparedStatement!=null)
                    preparedStatement.close();
                if(connection!=null)
                    connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return rTypeInner;
    }
}
