package com.yonyou.utils;

import java.security.SecureRandom;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

public class MyHttpsConnection
{
  private myX509TrustManager xtm = new myX509TrustManager();
  private myHostnameVerifier hnv = new myHostnameVerifier();

  public MyHttpsConnection() {
    SSLContext sslContext = null;
    try {
      sslContext = SSLContext.getInstance("TLS");
      X509TrustManager[] xtmArray = { this.xtm };
      sslContext.init(null, xtmArray, new SecureRandom());
    } catch (Exception gse) {
      gse.printStackTrace();
    }
    if (sslContext != null) {
      HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    }
    HttpsURLConnection.setDefaultHostnameVerifier(this.hnv);
  }
}