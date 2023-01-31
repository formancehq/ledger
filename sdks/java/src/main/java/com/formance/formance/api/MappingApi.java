package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.ErrorResponse;
import com.formance.formance.model.Mapping;
import com.formance.formance.model.MappingResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MappingApi {
  /**
   * Get the mapping of a ledger
   * 
   * @param ledger Name of the ledger. (required)
   * @return Call&lt;MappingResponse&gt;
   */
  @GET("api/ledger/{ledger}/mapping")
  Call<MappingResponse> getMapping(
    @retrofit2.http.Path("ledger") String ledger
  );

  /**
   * Update the mapping of a ledger
   * 
   * @param ledger Name of the ledger. (required)
   * @param mapping  (required)
   * @return Call&lt;MappingResponse&gt;
   */
  @Headers({
    "Content-Type:application/json"
  })
  @PUT("api/ledger/{ledger}/mapping")
  Call<MappingResponse> updateMapping(
    @retrofit2.http.Path("ledger") String ledger, @retrofit2.http.Body Mapping mapping
  );

}
