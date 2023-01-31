package com.formance.formance.api;

import com.formance.formance.CollectionFormats.*;

import retrofit2.Call;
import retrofit2.http.*;

import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.MultipartBody;

import com.formance.formance.model.ListUsersResponse;
import com.formance.formance.model.ReadUserResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface UsersApi {
  /**
   * List users
   * List users
   * @return Call&lt;ListUsersResponse&gt;
   */
  @GET("api/auth/users")
  Call<ListUsersResponse> listUsers();
    

  /**
   * Read user
   * Read user
   * @param userId User ID (required)
   * @return Call&lt;ReadUserResponse&gt;
   */
  @GET("api/auth/users/{userId}")
  Call<ReadUserResponse> readUser(
    @retrofit2.http.Path("userId") String userId
  );

}
