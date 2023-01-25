# Query

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Ledgers** | Pointer to **[]string** |  | [optional] 
**After** | Pointer to **[]string** |  | [optional] 
**PageSize** | Pointer to **int64** |  | [optional] 
**Terms** | Pointer to **[]string** |  | [optional] 
**Sort** | Pointer to **string** |  | [optional] 
**Policy** | Pointer to **string** |  | [optional] 
**Target** | Pointer to **string** |  | [optional] 
**Cursor** | Pointer to **string** |  | [optional] 

## Methods

### NewQuery

`func NewQuery() *Query`

NewQuery instantiates a new Query object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewQueryWithDefaults

`func NewQueryWithDefaults() *Query`

NewQueryWithDefaults instantiates a new Query object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetLedgers

`func (o *Query) GetLedgers() []string`

GetLedgers returns the Ledgers field if non-nil, zero value otherwise.

### GetLedgersOk

`func (o *Query) GetLedgersOk() (*[]string, bool)`

GetLedgersOk returns a tuple with the Ledgers field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLedgers

`func (o *Query) SetLedgers(v []string)`

SetLedgers sets Ledgers field to given value.

### HasLedgers

`func (o *Query) HasLedgers() bool`

HasLedgers returns a boolean if a field has been set.

### GetAfter

`func (o *Query) GetAfter() []string`

GetAfter returns the After field if non-nil, zero value otherwise.

### GetAfterOk

`func (o *Query) GetAfterOk() (*[]string, bool)`

GetAfterOk returns a tuple with the After field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAfter

`func (o *Query) SetAfter(v []string)`

SetAfter sets After field to given value.

### HasAfter

`func (o *Query) HasAfter() bool`

HasAfter returns a boolean if a field has been set.

### GetPageSize

`func (o *Query) GetPageSize() int64`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *Query) GetPageSizeOk() (*int64, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *Query) SetPageSize(v int64)`

SetPageSize sets PageSize field to given value.

### HasPageSize

`func (o *Query) HasPageSize() bool`

HasPageSize returns a boolean if a field has been set.

### GetTerms

`func (o *Query) GetTerms() []string`

GetTerms returns the Terms field if non-nil, zero value otherwise.

### GetTermsOk

`func (o *Query) GetTermsOk() (*[]string, bool)`

GetTermsOk returns a tuple with the Terms field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTerms

`func (o *Query) SetTerms(v []string)`

SetTerms sets Terms field to given value.

### HasTerms

`func (o *Query) HasTerms() bool`

HasTerms returns a boolean if a field has been set.

### GetSort

`func (o *Query) GetSort() string`

GetSort returns the Sort field if non-nil, zero value otherwise.

### GetSortOk

`func (o *Query) GetSortOk() (*string, bool)`

GetSortOk returns a tuple with the Sort field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSort

`func (o *Query) SetSort(v string)`

SetSort sets Sort field to given value.

### HasSort

`func (o *Query) HasSort() bool`

HasSort returns a boolean if a field has been set.

### GetPolicy

`func (o *Query) GetPolicy() string`

GetPolicy returns the Policy field if non-nil, zero value otherwise.

### GetPolicyOk

`func (o *Query) GetPolicyOk() (*string, bool)`

GetPolicyOk returns a tuple with the Policy field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPolicy

`func (o *Query) SetPolicy(v string)`

SetPolicy sets Policy field to given value.

### HasPolicy

`func (o *Query) HasPolicy() bool`

HasPolicy returns a boolean if a field has been set.

### GetTarget

`func (o *Query) GetTarget() string`

GetTarget returns the Target field if non-nil, zero value otherwise.

### GetTargetOk

`func (o *Query) GetTargetOk() (*string, bool)`

GetTargetOk returns a tuple with the Target field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTarget

`func (o *Query) SetTarget(v string)`

SetTarget sets Target field to given value.

### HasTarget

`func (o *Query) HasTarget() bool`

HasTarget returns a boolean if a field has been set.

### GetCursor

`func (o *Query) GetCursor() string`

GetCursor returns the Cursor field if non-nil, zero value otherwise.

### GetCursorOk

`func (o *Query) GetCursorOk() (*string, bool)`

GetCursorOk returns a tuple with the Cursor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCursor

`func (o *Query) SetCursor(v string)`

SetCursor sets Cursor field to given value.

### HasCursor

`func (o *Query) HasCursor() bool`

HasCursor returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


