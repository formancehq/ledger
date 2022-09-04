# MappingResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Data** | Pointer to [**NullableMapping**](Mapping.md) |  | [optional] 

## Methods

### NewMappingResponse

`func NewMappingResponse() *MappingResponse`

NewMappingResponse instantiates a new MappingResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewMappingResponseWithDefaults

`func NewMappingResponseWithDefaults() *MappingResponse`

NewMappingResponseWithDefaults instantiates a new MappingResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetData

`func (o *MappingResponse) GetData() Mapping`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *MappingResponse) GetDataOk() (*Mapping, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *MappingResponse) SetData(v Mapping)`

SetData sets Data field to given value.

### HasData

`func (o *MappingResponse) HasData() bool`

HasData returns a boolean if a field has been set.

### SetDataNil

`func (o *MappingResponse) SetDataNil(b bool)`

 SetDataNil sets the value for Data to be an explicit nil

### UnsetData
`func (o *MappingResponse) UnsetData()`

UnsetData ensures that no value is present for Data, not even an explicit nil

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


