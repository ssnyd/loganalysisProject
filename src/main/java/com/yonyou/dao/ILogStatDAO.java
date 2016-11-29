package com.yonyou.dao;



import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Created by ChenXiaoLei on 2016/11/9.
 */
public interface ILogStatDAO {
    void updataBatch(List<Map<String, String>> pvStats, Connection connection);
}



//package com.yonyou.dao;
//
//
//import com.yonyou.entity.PVStat;
//
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by ChenXiaoLei on 2016/11/9.
// */
//public interface ILogStatDAO {
//    void updataBatch(List<PVStat> pvStats);
//}
