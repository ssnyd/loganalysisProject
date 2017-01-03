package com.yonyou.myTest;

import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.JDBCUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by chenxiaolei on 17/1/3.
 */
public class t0103 implements Serializable{
    private static final long serialVersionUID = -701633232681493186L;
    public static void main(String[] args) {
        final List<Tuple2<String, Boolean>> list = new ArrayList<Tuple2<String, Boolean>>();
        String selectSQL = "select distinct memberId from rp_activity_data where action = 'statisticsRegister' and created >=1481698591 and created <= 1483286400";
        //
        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        Connection conn = jdbcUtils.getConnection();
        JDBCHelper.executeQuery(conn, selectSQL, null, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
                    list.add(new Tuple2<String, Boolean>(rs.getInt(1) + "", true));
                }
            }
        });
        Map<String, Boolean> whiteMap = new HashMap<String, Boolean>();
        for (Tuple2<String, Boolean> memberId : list) {
            whiteMap.put(memberId._1, memberId._2);
        }
        Boolean s = whiteMap.get("4343");
        if (s==null){
            System.out.println(false);
        }else {
            System.out.println(s);
        }





    }


}
