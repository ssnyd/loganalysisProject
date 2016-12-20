package com.yonyou.myTest;

import com.alibaba.fastjson.JSONObject;
import com.yonyou.utils.DateUtils;

/**
 * Created by chenxiaolei on 16/12/16.
 */
public class test1216 {
    public static void main(String[] args) {
        String s = "139.129.98.32   -   10.163.252.95   openapi 2316    2   809 [16/Dec/2016:15:53:22 +0800]    1481874802.319  \"POST /rest/message/app/share?access_token=fdfe3d9a1911a7166f872b94055a7cb2c1c017c70c53b8bbf3d88a330a19240d HTTP/1.1\" {\\x22taskId\\x22:\\x2219057\\x22,\\x22access_token\\x22:\\x22fdfe3d9a1911a7166f872b94055a7cb2c1c017c70c53b8bbf3d88a330a19240d\\x22,\\x22dir\\x22:\\x22eyJ0YWJUeXBlIjoiY2hhcmdlSm9pbiIsInBhbmVsVHlwZSI6Imxpc3QiLCJwYWdlIjoxLCJ0YXNrTmFtZSI6IiIsImRhdGVUeXBlIjo0LCJzdGF0dXMiOjEsInJvbGUiOjB9\\x22,\\x22{\\x5C\\x22spaceId\\x5C\\x22:\\x5C\\x2283905\\x5C\\x22,\\x5C\\x22appId\\x5C\\x22:\\x5C\\x2227018\\x5C\\x22,\\x5C\\x22sendThrough\\x5C\\x22:\\x5C\\x22appNotify\\x5C\\x22,\\x5C\\x22sendScope\\x5C\\x22:\\x5C\\x22list\\x5C\\x22,\\x5C\\x22to\\x5C\\x22:[\\x5C\\x222907444\\x5C\\x22],\\x5C\\x22title\\x5C\\x22:\\x5C\\x22\\xE5\\x9B\\x9E\\xE5\\xA4\\x8D\\xE4\\xBB\\xBB\\xE5\\x8A\\xA1\\x5C\\x22,\\x5C\\x22desc\\x5C\\x22:\\x5C\\x22\\xE7\\x8E\\x8B\\xE6\\x9D\\xA8 \\xE5\\xAF\\xB9\\xE6\\x82\\xA8\\xE7\\x9A\\x84\\xE4\\xBB\\xBB\\xE5\\x8A\\xA1 \\xE8\\xAE\\xBE\\xE5\\xA4\\x87\\xE6\\x9D\\x90\\xE6\\x96\\x99\\xE9\\x9C\\x80\\xE6\\xB1\\x82\\xE8\\xA1\\xA8 \\xE8\\xBF\\x9B\\xE8\\xA1\\x8C\\xE4\\xBA\\x86\\xE5\\x9B\\x9E\\xE5\\xA4\\x8D\\xEF\\xBC\\x8C\\xE5\\x8E\\xBB\\xE7\\x9C\\x8B\\xE7\\x9C\\x8B\\xEF\\xBC\\x81\\x5C\\x22,\\x5C\\x22detailUrl\\x5C\\x22:\\x5C\\x22https:\\x5C/\\x5C/ezone.upesn.com\\x5C/task\\x5C/index\\x5C/messageUrlRedirect?pushSalt\\x22:\\x22bu\\x5C/XAfQma9EBBZGX+P9IaA==\\x22,\\x22memberid\\x22:\\x22${memberid}\\x22,\\x22qzid\\x22:\\x22${qzid}\\x5C\\x22}\\x22}   200 30  159 \"-\"   \"Java/1.8.0_51\"   \"-\"   0.031   0.031   \"-\"";


        System.out.println(s.split("\t").length);
        String s1 = "\"POST /rest/message/app/share?access_token=fdfe3d9a1911a7166f872b94055a7cb2c1c017c70c53b8bbf3d88a330a19240d HTTP/1.1\"";
        String s2 = "{\\x22taskId\\x22:\\x2219057\\x22,\\x22access_token\\x22:\\x22fdfe3d9a1911a7166f872b94055a7cb2c1c017c70c53b8bbf3d88a330a19240d\\x22,\\x22dir\\x22:\\x22eyJ0YWJUeXBlIjoiY2hhcmdlSm9pbiIsInBhbmVsVHlwZSI6Imxpc3QiLCJwYWdlIjoxLCJ0YXNrTmFtZSI6IiIsImRhdGVUeXBlIjo0LCJzdGF0dXMiOjEsInJvbGUiOjB9\\x22,\\x22{\\x5C\\x22spaceId\\x5C\\x22:\\x5C\\x2283905\\x5C\\x22,\\x5C\\x22appId\\x5C\\x22:\\x5C\\x2227018\\x5C\\x22,\\x5C\\x22sendThrough\\x5C\\x22:\\x5C\\x22appNotify\\x5C\\x22,\\x5C\\x22sendScope\\x5C\\x22:\\x5C\\x22list\\x5C\\x22,\\x5C\\x22to\\x5C\\x22:[\\x5C\\x222907444\\x5C\\x22],\\x5C\\x22title\\x5C\\x22:\\x5C\\x22\\xE5\\x9B\\x9E\\xE5\\xA4\\x8D\\xE4\\xBB\\xBB\\xE5\\x8A\\xA1\\x5C\\x22,\\x5C\\x22desc\\x5C\\x22:\\x5C\\x22\\xE7\\x8E\\x8B\\xE6\\x9D\\xA8 \\xE5\\xAF\\xB9\\xE6\\x82\\xA8\\xE7\\x9A\\x84\\xE4\\xBB\\xBB\\xE5\\x8A\\xA1 \\xE8\\xAE\\xBE\\xE5\\xA4\\x87\\xE6\\x9D\\x90\\xE6\\x96\\x99\\xE9\\x9C\\x80\\xE6\\xB1\\x82\\xE8\\xA1\\xA8 \\xE8\\xBF\\x9B\\xE8\\xA1\\x8C\\xE4\\xBA\\x86\\xE5\\x9B\\x9E\\xE5\\xA4\\x8D\\xEF\\xBC\\x8C\\xE5\\x8E\\xBB\\xE7\\x9C\\x8B\\xE7\\x9C\\x8B\\xEF\\xBC\\x81\\x5C\\x22,\\x5C\\x22detailUrl\\x5C\\x22:\\x5C\\x22https:\\x5C/\\x5C/ezone.upesn.com\\x5C/task\\x5C/index\\x5C/messageUrlRedirect?pushSalt\\x22:\\x22bu\\x5C/XAfQma9EBBZGX+P9IaA==\\x22,\\x22memberid\\x22:\\x22${memberid}\\x22,\\x22qzid\\x22:\\x22${qzid}\\x5C\\x22}\\x22}";

        System.out.println(Token(s2));
        String s3 ="{\"action\":\"add\",\"app_id\":\"27018\",\"client\":\"web\",\"client_ip\":\"127.0.0.1\",\"device_model\":\"\",\"device_name\":\"\",\"instance_id\":\"4785\",\"member_id\":\"2837974\",\"mtime\":\"1482137522954\",\"object_id\":\"\",\"qz_id\":\"74269\",\"user_id\":\"0\",\"ver_code\":\"\"}";
        System.out.println(s3.contains("}")&&s3.contains("{")&&s3.contains("mtime"));
        JSONObject jsonObject = JSONObject.parseObject(s3);
        Long mtime = jsonObject.getLong("mtime");
        long time = DateUtils.timeStamp2Date(mtime, null);
        System.out.println(mtime+"***"+time);

    }
    private static String Token(String s) {
        String _token = "";
        String s1 = s.replace("\\x22", "\"").replace("\"", "").replace("\\x5C", "");
        String[] split = s1.split(",");
        for (int i = 0; i < split.length; i++) {
            if (split[i].contains("token")) {
                if (split[i].split(":").length == 2)
                    _token = split[i].split(":")[1].replace("}", "").replace("{", "");
                break;
            }
        }
        return _token;
    }
}
