package com.yonyou.dao.factory;


import com.yonyou.dao.*;
import com.yonyou.dao.impl.*;

/**
 * DAO工厂类
 * Created by ChenXiaoLei on 2016/11/9.
 */
public class DAOFactory {
    /**
     * pv实时接口
     *
     * @return
     */
    public static ILogStatDAO getLogStatDAO() {
        return new LogStatDAOImpl();
    }

    /**
     * uv实时接口
     *
     * @return
     */
    public static IUVStatDAO getUVStatDAO() {
        return new UVStatDAOImpl();
    }

    /**
     * mem实时接口
     *
     * @return
     */
    public static IMemIdStatDAO getMemIdStatDAO() {
        return new MemIdStatDAOImpl();
    }

    /**
     * ipv实时接口
     *
     * @return
     */
    public static IIPVStatDAO getIPVStatDAO() {
        return new IPVStatDAOImpl();
    }
    /**
     * pv实时接口
     *
     * @return
     */
    public static IPVStatDAO getPVStatDAO() {
        return new PVStatDAOImpl();
    }
    /**
     * apply实时接口
     *
     * @return
     */
    public static IApplyStatDAO getApplyStatDAO() {
        return new ApplyStatImpl();
    }
    /**
     * apply2实时接口
     *
     * @return
     */
    public static IApplyEStatDAO getApplyEStatDAO() {
        return new ApplyEStatImpl();
    }
    /**
     * apply3实时接口
     *
     * @return
     */
    public static IApplyQStatDAO getApplyQStatDAO() {
        return new ApplyQStatImpl();
    }
    /**
     * ev实时接口
     *
     * @return
     */
    public static IEVStatDAO getEVStatDAO() {
        return new EVStatDAOImpl();
    }

    /**
     * euv实时接口
     *
     * @return
     */
    public static IEUVStatDAO getEUVStatDAO() {
        return new EUVStatDAOImpl();
    }
    /**
     * epv实时接口
     *
     * @return
     *
     */
    public static IEPVStatDAO getEPVStatDAO() {
        return new EPVStatDAOImpl();
    }
    /**
     * flow接口
     *
     * @return
     *
     */
    public static FlowStatDAO getFlowStatDAO() {
        return new FlowStatDAOImpl();
    }
}

