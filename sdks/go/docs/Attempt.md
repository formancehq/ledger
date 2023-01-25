# Attempt

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**WebhookID** | Pointer to **string** |  | [optional] 
**CreatedAt** | Pointer to **time.Time** |  | [optional] 
**UpdatedAt** | Pointer to **time.Time** |  | [optional] 
**Config** | Pointer to [**WebhooksConfig**](WebhooksConfig.md) |  | [optional] 
**Payload** | Pointer to **string** |  | [optional] 
**StatusCode** | Pointer to **int32** |  | [optional] 
**RetryAttempt** | Pointer to **int32** |  | [optional] 
**Status** | Pointer to **string** |  | [optional] 
**NextRetryAfter** | Pointer to **time.Time** |  | [optional] 

## Methods

### NewAttempt

`func NewAttempt() *Attempt`

NewAttempt instantiates a new Attempt object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewAttemptWithDefaults

`func NewAttemptWithDefaults() *Attempt`

NewAttemptWithDefaults instantiates a new Attempt object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *Attempt) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *Attempt) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *Attempt) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *Attempt) HasId() bool`

HasId returns a boolean if a field has been set.

### GetWebhookID

`func (o *Attempt) GetWebhookID() string`

GetWebhookID returns the WebhookID field if non-nil, zero value otherwise.

### GetWebhookIDOk

`func (o *Attempt) GetWebhookIDOk() (*string, bool)`

GetWebhookIDOk returns a tuple with the WebhookID field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetWebhookID

`func (o *Attempt) SetWebhookID(v string)`

SetWebhookID sets WebhookID field to given value.

### HasWebhookID

`func (o *Attempt) HasWebhookID() bool`

HasWebhookID returns a boolean if a field has been set.

### GetCreatedAt

`func (o *Attempt) GetCreatedAt() time.Time`

GetCreatedAt returns the CreatedAt field if non-nil, zero value otherwise.

### GetCreatedAtOk

`func (o *Attempt) GetCreatedAtOk() (*time.Time, bool)`

GetCreatedAtOk returns a tuple with the CreatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedAt

`func (o *Attempt) SetCreatedAt(v time.Time)`

SetCreatedAt sets CreatedAt field to given value.

### HasCreatedAt

`func (o *Attempt) HasCreatedAt() bool`

HasCreatedAt returns a boolean if a field has been set.

### GetUpdatedAt

`func (o *Attempt) GetUpdatedAt() time.Time`

GetUpdatedAt returns the UpdatedAt field if non-nil, zero value otherwise.

### GetUpdatedAtOk

`func (o *Attempt) GetUpdatedAtOk() (*time.Time, bool)`

GetUpdatedAtOk returns a tuple with the UpdatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUpdatedAt

`func (o *Attempt) SetUpdatedAt(v time.Time)`

SetUpdatedAt sets UpdatedAt field to given value.

### HasUpdatedAt

`func (o *Attempt) HasUpdatedAt() bool`

HasUpdatedAt returns a boolean if a field has been set.

### GetConfig

`func (o *Attempt) GetConfig() WebhooksConfig`

GetConfig returns the Config field if non-nil, zero value otherwise.

### GetConfigOk

`func (o *Attempt) GetConfigOk() (*WebhooksConfig, bool)`

GetConfigOk returns a tuple with the Config field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConfig

`func (o *Attempt) SetConfig(v WebhooksConfig)`

SetConfig sets Config field to given value.

### HasConfig

`func (o *Attempt) HasConfig() bool`

HasConfig returns a boolean if a field has been set.

### GetPayload

`func (o *Attempt) GetPayload() string`

GetPayload returns the Payload field if non-nil, zero value otherwise.

### GetPayloadOk

`func (o *Attempt) GetPayloadOk() (*string, bool)`

GetPayloadOk returns a tuple with the Payload field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPayload

`func (o *Attempt) SetPayload(v string)`

SetPayload sets Payload field to given value.

### HasPayload

`func (o *Attempt) HasPayload() bool`

HasPayload returns a boolean if a field has been set.

### GetStatusCode

`func (o *Attempt) GetStatusCode() int32`

GetStatusCode returns the StatusCode field if non-nil, zero value otherwise.

### GetStatusCodeOk

`func (o *Attempt) GetStatusCodeOk() (*int32, bool)`

GetStatusCodeOk returns a tuple with the StatusCode field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatusCode

`func (o *Attempt) SetStatusCode(v int32)`

SetStatusCode sets StatusCode field to given value.

### HasStatusCode

`func (o *Attempt) HasStatusCode() bool`

HasStatusCode returns a boolean if a field has been set.

### GetRetryAttempt

`func (o *Attempt) GetRetryAttempt() int32`

GetRetryAttempt returns the RetryAttempt field if non-nil, zero value otherwise.

### GetRetryAttemptOk

`func (o *Attempt) GetRetryAttemptOk() (*int32, bool)`

GetRetryAttemptOk returns a tuple with the RetryAttempt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRetryAttempt

`func (o *Attempt) SetRetryAttempt(v int32)`

SetRetryAttempt sets RetryAttempt field to given value.

### HasRetryAttempt

`func (o *Attempt) HasRetryAttempt() bool`

HasRetryAttempt returns a boolean if a field has been set.

### GetStatus

`func (o *Attempt) GetStatus() string`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *Attempt) GetStatusOk() (*string, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *Attempt) SetStatus(v string)`

SetStatus sets Status field to given value.

### HasStatus

`func (o *Attempt) HasStatus() bool`

HasStatus returns a boolean if a field has been set.

### GetNextRetryAfter

`func (o *Attempt) GetNextRetryAfter() time.Time`

GetNextRetryAfter returns the NextRetryAfter field if non-nil, zero value otherwise.

### GetNextRetryAfterOk

`func (o *Attempt) GetNextRetryAfterOk() (*time.Time, bool)`

GetNextRetryAfterOk returns a tuple with the NextRetryAfter field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNextRetryAfter

`func (o *Attempt) SetNextRetryAfter(v time.Time)`

SetNextRetryAfter sets NextRetryAfter field to given value.

### HasNextRetryAfter

`func (o *Attempt) HasNextRetryAfter() bool`

HasNextRetryAfter returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


