package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.ConfigInfoResponse;
import com.formance.formance.model.ErrorResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ServerApi {
  /**
   * Show server information
   * 
   * @return Call&lt;ConfigInfoResponse&gt;
   */
  @GET("api/ledger/_info")
  Call<ConfigInfoResponse> getInfo();
    

}
