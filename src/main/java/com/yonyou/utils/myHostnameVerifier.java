package com.yonyou.utils;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

class myHostnameVerifier
  implements HostnameVerifier
{
  public boolean verify(String hostname, SSLSession session)
  {
    return true;
  }
}