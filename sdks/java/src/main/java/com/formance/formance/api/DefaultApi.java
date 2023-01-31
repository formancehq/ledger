package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.ServerInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DefaultApi {
  /**
   * Get server info
   * 
   * @return Call&lt;ServerInfo&gt;
   */
  @GET("api/auth/_info")
  Call<ServerInfo> getServerInfo();
    

  /**
   * Get server info
   * 
   * @return Call&lt;ServerInfo&gt;
   */
  @GET("api/payments/_info")
  Call<ServerInfo> paymentsgetServerInfo();
    

  /**
   * Get server info
   * 
   * @return Call&lt;ServerInfo&gt;
   */
  @GET("api/search/_info")
  Call<ServerInfo> searchgetServerInfo();
    

}
