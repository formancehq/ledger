# StripeTask

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**OldestId** | Pointer to **string** | The id of the oldest BalanceTransaction fetched from stripe for this account | [optional]
**OldestDate** | Pointer to **time.Time** | The creation date of the oldest BalanceTransaction fetched from stripe for this account | [optional]
**MoreRecentId** | Pointer to **string** | The id of the more recent BalanceTransaction fetched from stripe for this account | [optional]
**MoreRecentDate** | Pointer to **time.Time** | The creation date of the more recent BalanceTransaction fetched from stripe for this account | [optional]
**NoMoreHistory** | Pointer to **bool** |  | [optional]

## Methods

### NewStripeTask

`func NewStripeTask() *StripeTask`

NewStripeTask instantiates a new StripeTask object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewStripeTaskWithDefaults

`func NewStripeTaskWithDefaults() *StripeTask`

NewStripeTaskWithDefaults instantiates a new StripeTask object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetOldestId

`func (o *StripeTask) GetOldestId() string`

GetOldestId returns the OldestId field if non-nil, zero value otherwise.

### GetOldestIdOk

`func (o *StripeTask) GetOldestIdOk() (*string, bool)`

GetOldestIdOk returns a tuple with the OldestId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOldestId

`func (o *StripeTask) SetOldestId(v string)`

SetOldestId sets OldestId field to given value.

### HasOldestId

`func (o *StripeTask) HasOldestId() bool`

HasOldestId returns a boolean if a field has been set.

### GetOldestDate

`func (o *StripeTask) GetOldestDate() time.Time`

GetOldestDate returns the OldestDate field if non-nil, zero value otherwise.

### GetOldestDateOk

`func (o *StripeTask) GetOldestDateOk() (*time.Time, bool)`

GetOldestDateOk returns a tuple with the OldestDate field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOldestDate

`func (o *StripeTask) SetOldestDate(v time.Time)`

SetOldestDate sets OldestDate field to given value.

### HasOldestDate

`func (o *StripeTask) HasOldestDate() bool`

HasOldestDate returns a boolean if a field has been set.

### GetMoreRecentId

`func (o *StripeTask) GetMoreRecentId() string`

GetMoreRecentId returns the MoreRecentId field if non-nil, zero value otherwise.

### GetMoreRecentIdOk

`func (o *StripeTask) GetMoreRecentIdOk() (*string, bool)`

GetMoreRecentIdOk returns a tuple with the MoreRecentId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMoreRecentId

`func (o *StripeTask) SetMoreRecentId(v string)`

SetMoreRecentId sets MoreRecentId field to given value.

### HasMoreRecentId

`func (o *StripeTask) HasMoreRecentId() bool`

HasMoreRecentId returns a boolean if a field has been set.

### GetMoreRecentDate

`func (o *StripeTask) GetMoreRecentDate() time.Time`

GetMoreRecentDate returns the MoreRecentDate field if non-nil, zero value otherwise.

### GetMoreRecentDateOk

`func (o *StripeTask) GetMoreRecentDateOk() (*time.Time, bool)`

GetMoreRecentDateOk returns a tuple with the MoreRecentDate field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMoreRecentDate

`func (o *StripeTask) SetMoreRecentDate(v time.Time)`

SetMoreRecentDate sets MoreRecentDate field to given value.

### HasMoreRecentDate

`func (o *StripeTask) HasMoreRecentDate() bool`

HasMoreRecentDate returns a boolean if a field has been set.

### GetNoMoreHistory

`func (o *StripeTask) GetNoMoreHistory() bool`

GetNoMoreHistory returns the NoMoreHistory field if non-nil, zero value otherwise.

### GetNoMoreHistoryOk

`func (o *StripeTask) GetNoMoreHistoryOk() (*bool, bool)`

GetNoMoreHistoryOk returns a tuple with the NoMoreHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNoMoreHistory

`func (o *StripeTask) SetNoMoreHistory(v bool)`

SetNoMoreHistory sets NoMoreHistory field to given value.

### HasNoMoreHistory

`func (o *StripeTask) HasNoMoreHistory() bool`

HasNoMoreHistory returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
