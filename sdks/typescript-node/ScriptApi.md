# formance.ScriptApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**runScript**](ScriptApi.md#runScript) | **POST** /api/ledger/{ledger}/script | Execute a Numscript


# **runScript**
> ScriptResponse runScript(script)

This route is deprecated, and has been merged into `POST /{ledger}/transactions`. 

### Example


```typescript
import { ScriptApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ScriptApi(configuration);

let body:ScriptApiRunScriptRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // Script
  script: {
    plain: `vars {
account $user
}
send [COIN 10] (
	source = @world
	destination = $user
)
`,
    vars: {},
    reference: "order_1234",
    metadata: {
      "key": null,
    },
  },
  // boolean | Set the preview mode. Preview mode doesn't add the logs to the database or publish a message to the message broker. (optional)
  preview: true,
};

apiInstance.runScript(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **script** | **Script**|  |
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **preview** | [**boolean**] | Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker. | (optional) defaults to undefined


### Return type

**ScriptResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | On success, it will return a 200 status code, and the resulting transaction under the &#x60;transaction&#x60; field.  On failure, it will also return a 200 status code, and the following fields:   - &#x60;details&#x60;: contains a URL. When there is an error parsing Numscript, the result can be difficult to read—the provided URL will render the error in an easy-to-read format.   - &#x60;errorCode&#x60; and &#x60;error_code&#x60; (deprecated): contains the string code of the error   - &#x60;errorMessage&#x60; and &#x60;error_message&#x60; (deprecated): contains a human-readable indication of what went wrong, for example that an account had insufficient funds, or that there was an error in the provided Numscript.  |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

