// TODO: better import syntax?
import {BaseAPIRequestFactory, RequiredError, COLLECTION_FORMATS} from './baseapi';
import {Configuration} from '../configuration';
import {RequestContext, HttpMethod, ResponseContext, HttpFile} from '../http/http';
import * as FormData from "form-data";
import { URLSearchParams } from 'url';
import {ObjectSerializer} from '../models/ObjectSerializer';
import {ApiException} from './exception';
import {canConsumeForm, isCodeInRange} from '../util';
import {SecurityAuthentication} from '../auth/auth';


import { ErrorResponse } from '../models/ErrorResponse';
import { LogsCursorResponse } from '../models/LogsCursorResponse';

/**
 * no description
 */
export class LogsApiRequestFactory extends BaseAPIRequestFactory {

    /**
     * List the logs from a ledger, sorted by ID in descending order.
     * List the logs from a ledger
     * @param ledger Name of the ledger.
     * @param pageSize The maximum number of results to return per page. 
     * @param pageSize2 The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead. 
     * @param after Pagination cursor, will return the logs after a given ID. (in descending order).
     * @param startTime Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). 
     * @param startTime2 Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead. 
     * @param endTime Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). 
     * @param endTime2 Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead. 
     * @param cursor Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
     * @param paginationToken Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead. 
     */
    public async listLogs(ledger: string, pageSize?: number, pageSize2?: number, after?: string, startTime?: Date, startTime2?: Date, endTime?: Date, endTime2?: Date, cursor?: string, paginationToken?: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("LogsApi", "listLogs", "ledger");
        }











        // Path Params
        const localVarPath = '/api/ledger/{ledger}/log'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (pageSize !== undefined) {
            requestContext.setQueryParam("pageSize", ObjectSerializer.serialize(pageSize, "number", "int64"));
        }

        // Query Params
        if (pageSize2 !== undefined) {
            requestContext.setQueryParam("page_size", ObjectSerializer.serialize(pageSize2, "number", "int64"));
        }

        // Query Params
        if (after !== undefined) {
            requestContext.setQueryParam("after", ObjectSerializer.serialize(after, "string", ""));
        }

        // Query Params
        if (startTime !== undefined) {
            requestContext.setQueryParam("startTime", ObjectSerializer.serialize(startTime, "Date", "date-time"));
        }

        // Query Params
        if (startTime2 !== undefined) {
            requestContext.setQueryParam("start_time", ObjectSerializer.serialize(startTime2, "Date", "date-time"));
        }

        // Query Params
        if (endTime !== undefined) {
            requestContext.setQueryParam("endTime", ObjectSerializer.serialize(endTime, "Date", "date-time"));
        }

        // Query Params
        if (endTime2 !== undefined) {
            requestContext.setQueryParam("end_time", ObjectSerializer.serialize(endTime2, "Date", "date-time"));
        }

        // Query Params
        if (cursor !== undefined) {
            requestContext.setQueryParam("cursor", ObjectSerializer.serialize(cursor, "string", ""));
        }

        // Query Params
        if (paginationToken !== undefined) {
            requestContext.setQueryParam("pagination_token", ObjectSerializer.serialize(paginationToken, "string", ""));
        }


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["Authorization"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

}

export class LogsApiResponseProcessor {

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to listLogs
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async listLogs(response: ResponseContext): Promise<LogsCursorResponse > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: LogsCursorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "LogsCursorResponse", ""
            ) as LogsCursorResponse;
            return body;
        }
        if (isCodeInRange("0", response.httpStatusCode)) {
            const body: ErrorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ErrorResponse", ""
            ) as ErrorResponse;
            throw new ApiException<ErrorResponse>(response.httpStatusCode, "Error", body, response.headers);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: LogsCursorResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "LogsCursorResponse", ""
            ) as LogsCursorResponse;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

}
