package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.Script;
import com.formance.formance.model.ScriptResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface ScriptApi {
  /**
   * Execute a Numscript
   * This route is deprecated, and has been merged into &#x60;POST /{ledger}/transactions&#x60;. 
   * @param ledger Name of the ledger. (required)
   * @param script  (required)
   * @param preview Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker. (optional)
   * @return Call&lt;ScriptResponse&gt;
   * @deprecated
   */
  @Deprecated
  @Headers({
    "Content-Type:application/json"
  })
  @POST("api/ledger/{ledger}/script")
  Call<ScriptResponse> runScript(
    @retrofit2.http.Path("ledger") String ledger, @retrofit2.http.Body Script script, @retrofit2.http.Query("preview") Boolean preview
  );

}
