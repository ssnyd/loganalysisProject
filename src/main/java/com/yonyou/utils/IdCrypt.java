package com.yonyou.utils;

/**
 * ID混淆算法
 */
public class IdCrypt
{
    public static void main(String args[]) {
        String qzId = "385503";
        String qzIdHeader = IdCrypt.encodeId(qzId);
        System.out.println(qzId + " 加密后数据 " + qzIdHeader);

        String qzIdSource = IdCrypt.decodeId("77295200");
        System.out.println(qzIdHeader + " 解密后数据 " + qzIdSource);
    }

    /**
     * 对整数id进行可逆混淆
     */
    protected static String encodeId(String idStr)
    {
        int id = Integer.parseInt(idStr);
        int sid = (id & 0xff000000);
        sid += (id & 0x0000ff00) << 8;
        sid += (id & 0x00ff0000) >> 8;
        sid += (id & 0x0000000f) << 4;
        sid += (id & 0x000000f0) >> 4;
        sid ^= 82920181;
        return Integer.toString(sid);
    }

    /**
     * 对通过encodeId混淆的id进行还原
     */
    public static String decodeId(String sidStr)
    {
        if (!IdCrypt.isNumeric(sidStr)) {
            return "";
        }
        int sid = Integer.parseInt(sidStr);
        sid ^= 82920181;
        int id = (sid & 0xff000000);
        id += (sid & 0x00ff0000) >> 8;
        id += (sid & 0x0000ff00) << 8;
        id += (sid & 0x000000f0) >> 4;
        id += (sid & 0x0000000f) << 4;
        return Integer.toString(id);
    }

    public static boolean isNumeric(String str) {
        for (int i = str.length(); --i >= 0;) {
            int chr = str.charAt(i);
            if (chr < 48 || chr > 57)
                return false;
        }
        return true;
    }
}