# StripeConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PollingPeriod** | Pointer to **string** | The frequency at which the connector will try to fetch new BalanceTransaction objects from Stripe API.  | [optional] [default to "120s"]
**ApiKey** | **string** |  | 
**PageSize** | Pointer to **int64** | Number of BalanceTransaction to fetch at each polling interval.  | [optional] [default to 10]

## Methods

### NewStripeConfig

`func NewStripeConfig(apiKey string, ) *StripeConfig`

NewStripeConfig instantiates a new StripeConfig object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewStripeConfigWithDefaults

`func NewStripeConfigWithDefaults() *StripeConfig`

NewStripeConfigWithDefaults instantiates a new StripeConfig object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPollingPeriod

`func (o *StripeConfig) GetPollingPeriod() string`

GetPollingPeriod returns the PollingPeriod field if non-nil, zero value otherwise.

### GetPollingPeriodOk

`func (o *StripeConfig) GetPollingPeriodOk() (*string, bool)`

GetPollingPeriodOk returns a tuple with the PollingPeriod field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPollingPeriod

`func (o *StripeConfig) SetPollingPeriod(v string)`

SetPollingPeriod sets PollingPeriod field to given value.

### HasPollingPeriod

`func (o *StripeConfig) HasPollingPeriod() bool`

HasPollingPeriod returns a boolean if a field has been set.

### GetApiKey

`func (o *StripeConfig) GetApiKey() string`

GetApiKey returns the ApiKey field if non-nil, zero value otherwise.

### GetApiKeyOk

`func (o *StripeConfig) GetApiKeyOk() (*string, bool)`

GetApiKeyOk returns a tuple with the ApiKey field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetApiKey

`func (o *StripeConfig) SetApiKey(v string)`

SetApiKey sets ApiKey field to given value.


### GetPageSize

`func (o *StripeConfig) GetPageSize() int64`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *StripeConfig) GetPageSizeOk() (*int64, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *StripeConfig) SetPageSize(v int64)`

SetPageSize sets PageSize field to given value.

### HasPageSize

`func (o *StripeConfig) HasPageSize() bool`

HasPageSize returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


