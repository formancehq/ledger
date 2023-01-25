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


import { Script } from '../models/Script';
import { ScriptResponse } from '../models/ScriptResponse';

/**
 * no description
 */
export class ScriptApiRequestFactory extends BaseAPIRequestFactory {

    /**
     * This route is deprecated, and has been merged into `POST /{ledger}/transactions`. 
     * Execute a Numscript
     * @param ledger Name of the ledger.
     * @param script 
     * @param preview Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker.
     */
    public async runScript(ledger: string, script: Script, preview?: boolean, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'ledger' is not null or undefined
        if (ledger === null || ledger === undefined) {
            throw new RequiredError("ScriptApi", "runScript", "ledger");
        }


        // verify required parameter 'script' is not null or undefined
        if (script === null || script === undefined) {
            throw new RequiredError("ScriptApi", "runScript", "script");
        }



        // Path Params
        const localVarPath = '/api/ledger/{ledger}/script'
            .replace('{' + 'ledger' + '}', encodeURIComponent(String(ledger)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (preview !== undefined) {
            requestContext.setQueryParam("preview", ObjectSerializer.serialize(preview, "boolean", ""));
        }


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(script, "Script", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

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

export class ScriptApiResponseProcessor {

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to runScript
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async runScript(response: ResponseContext): Promise<ScriptResponse > {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: ScriptResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ScriptResponse", ""
            ) as ScriptResponse;
            return body;
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: ScriptResponse = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "ScriptResponse", ""
            ) as ScriptResponse;
            return body;
        }

        throw new ApiException<string | Buffer | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

}
