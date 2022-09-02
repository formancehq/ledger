# StatsResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Data** | [**Stats**](Stats.md) |  | 

## Methods

### NewStatsResponse

`func NewStatsResponse(data Stats, ) *StatsResponse`

NewStatsResponse instantiates a new StatsResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewStatsResponseWithDefaults

`func NewStatsResponseWithDefaults() *StatsResponse`

NewStatsResponseWithDefaults instantiates a new StatsResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetData

`func (o *StatsResponse) GetData() Stats`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *StatsResponse) GetDataOk() (*Stats, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *StatsResponse) SetData(v Stats)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


