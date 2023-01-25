# WebhooksConfigsResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Cursor** | [**WebhooksCursor**](WebhooksCursor.md) |  |

## Methods

### NewWebhooksConfigsResponse

`func NewWebhooksConfigsResponse(cursor WebhooksCursor, ) *WebhooksConfigsResponse`

NewWebhooksConfigsResponse instantiates a new WebhooksConfigsResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewWebhooksConfigsResponseWithDefaults

`func NewWebhooksConfigsResponseWithDefaults() *WebhooksConfigsResponse`

NewWebhooksConfigsResponseWithDefaults instantiates a new WebhooksConfigsResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCursor

`func (o *WebhooksConfigsResponse) GetCursor() WebhooksCursor`

GetCursor returns the Cursor field if non-nil, zero value otherwise.

### GetCursorOk

`func (o *WebhooksConfigsResponse) GetCursorOk() (*WebhooksCursor, bool)`

GetCursorOk returns a tuple with the Cursor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCursor

`func (o *WebhooksConfigsResponse) SetCursor(v WebhooksCursor)`

SetCursor sets Cursor field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
