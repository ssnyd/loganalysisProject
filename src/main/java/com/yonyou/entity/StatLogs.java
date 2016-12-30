package com.yonyou.entity;

import java.io.Serializable;

/**
 * Created by ChenXiaoLei on 2016/11/1.
 * ####################################
 remote_addr #记录客户端IP地址0
 remote_user #记录客户端用户名称1
 server_addr #本机IP2
 app_name #应用名称3
 connection #连接的序列号4
 connection_requests #当前通过一个连接获得的请求数量5
 request_length #请求的长度（包括请求行，请求头和请求正文6
 time_local #通用日志格式下的本地时间7
 msec #日志写入时间。单位为秒，精度是毫秒8
 request #请求地址9
 request_args #请求参数10
 status #相应状态11
 body_bytes_sent #发送body字节数12
 bytes_sent #发送字节数13
 http_referer #记录从哪个页面链接访问过来的14
 http_user_agent #记录客户端浏览器相关信息15
 http_x_forwarded_for #这个也是针对记录客户端IP地址的，为了防止篡改，暂时屏蔽掉了16
 request_time #请求处理时间，单位为秒，精度毫秒； 从读入客户端的第一个字节开始，直到把最后一个字符发送给客户端后进行日志写入为止17
 upstream_response_time #代理相应时间18
 sent_http_set_cookie #cookie 19
 */
public class StatLogs implements Serializable {
    private static final long serialVersionUID = -1489996937308036740L;
    private String remote_addr;//记录客户端ip地址                             //0
    private String remote_user;//记录客户端名称                               //1
    private String server_addr;//本机ip                                      //2
    private String app_name;//应用名称                                       //3
    private String connection;//连接的序列号                                 //4
    private String connection_requests;//当前通过一个链接获得的请求数量        //5
    private String request_length;//请求的长度(包括前当请求行 请求头和请求正文) //6
    private String time_local;//通用日志格式下的本地时间                      //7
    private String msec;//日志写入时间 单位秒 精度毫秒                        //8
    private Request request;//请求地址                                      //9
    private String request_args;//请求参数                                  //10
    private String status;//相应状态                                        //11
    private String body_bytes_sent;//发送body字节数                         //12
    private String bytes_sent;//发送字节数                                  //13
    private String http_referer;//记录从哪个页面链接访问过来的                //14
    private String http_user_agent;//记录客户端浏览器相关信息                //15
    private String http_x_forwarded_for;//这个也是针对记录客户端IP地址       //16
    private String request_time;//请求处理时间                              //17
    private String upstream_response_time;//请求处理时间                    //18
    private String sent_http_set_cookie;//cookie                           //19
    //#########################################################################
    private String country;//国家                           //20
    private String region;//省                              //21
    private String city;//地市                             //22
    private String member_id;//member_id                           //23
    private String qz_id;//qz_id                           //24

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getMember_id() {
        return member_id;
    }

    public void setMember_id(String member_id) {
        this.member_id = member_id;
    }

    public String getQz_id() {
        return qz_id;
    }

    public void setQz_id(String qz_id) {
        this.qz_id = qz_id;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    private String user_id;//user_id                           //25


    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getServer_addr() {
        return server_addr;
    }

    public void setServer_addr(String server_addr) {
        this.server_addr = server_addr;
    }

    public String getApp_name() {
        return app_name;
    }

    public void setApp_name(String app_name) {
        this.app_name = app_name;
    }

    public String getConnection() {
        return connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public String getConnection_requests() {
        return connection_requests;
    }

    public void setConnection_requests(String connection_requests) {
        this.connection_requests = connection_requests;
    }

    public String getRequest_length() {
        return request_length;
    }

    public void setRequest_length(String request_length) {
        this.request_length = request_length;
    }

    public String getMsec() {
        return msec;
    }

    public void setMsec(String msec) {
        this.msec = msec;
    }

    public Request getRequest() {
        return request;
    }

    public void setRequest(Request request) {
        this.request = request;
    }

    public String getRequest_args() {
        return request_args;
    }

    public void setRequest_args(String request_args) {
        this.request_args = request_args;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getBytes_sent() {
        return bytes_sent;
    }

    public void setBytes_sent(String bytes_sent) {
        this.bytes_sent = bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public String getHttp_x_forwarded_for() {
        return http_x_forwarded_for;
    }

    public void setHttp_x_forwarded_for(String http_x_forwarded_for) {
        this.http_x_forwarded_for = http_x_forwarded_for;
    }

    public String getRequest_time() {
        return request_time;
    }

    public void setRequest_time(String request_time) {
        this.request_time = request_time;
    }

    public String getUpstream_response_time() {
        return upstream_response_time;
    }

    public void setUpstream_response_time(String upstream_response_time) {
        this.upstream_response_time = upstream_response_time;
    }

    public String getSent_http_set_cookie() {
        return sent_http_set_cookie;
    }

    public void setSent_http_set_cookie(String sent_http_set_cookie) {
        this.sent_http_set_cookie = sent_http_set_cookie;
    }
}
