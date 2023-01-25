# WebhooksCursor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | **bool** |  |
**Data** | [**[]WebhooksConfig**](WebhooksConfig.md) |  |

## Methods

### NewWebhooksCursor

`func NewWebhooksCursor(hasMore bool, data []WebhooksConfig, ) *WebhooksCursor`

NewWebhooksCursor instantiates a new WebhooksCursor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewWebhooksCursorWithDefaults

`func NewWebhooksCursorWithDefaults() *WebhooksCursor`

NewWebhooksCursorWithDefaults instantiates a new WebhooksCursor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *WebhooksCursor) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *WebhooksCursor) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *WebhooksCursor) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.


### GetData

`func (o *WebhooksCursor) GetData() []WebhooksConfig`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *WebhooksCursor) GetDataOk() (*[]WebhooksConfig, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *WebhooksCursor) SetData(v []WebhooksConfig)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
