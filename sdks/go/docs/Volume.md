# Volume

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Input** | **int64** |  | 
**Output** | **int64** |  | 
**Balance** | Pointer to **int64** |  | [optional] 

## Methods

### NewVolume

`func NewVolume(input int64, output int64, ) *Volume`

NewVolume instantiates a new Volume object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewVolumeWithDefaults

`func NewVolumeWithDefaults() *Volume`

NewVolumeWithDefaults instantiates a new Volume object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetInput

`func (o *Volume) GetInput() int64`

GetInput returns the Input field if non-nil, zero value otherwise.

### GetInputOk

`func (o *Volume) GetInputOk() (*int64, bool)`

GetInputOk returns a tuple with the Input field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetInput

`func (o *Volume) SetInput(v int64)`

SetInput sets Input field to given value.


### GetOutput

`func (o *Volume) GetOutput() int64`

GetOutput returns the Output field if non-nil, zero value otherwise.

### GetOutputOk

`func (o *Volume) GetOutputOk() (*int64, bool)`

GetOutputOk returns a tuple with the Output field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOutput

`func (o *Volume) SetOutput(v int64)`

SetOutput sets Output field to given value.


### GetBalance

`func (o *Volume) GetBalance() int64`

GetBalance returns the Balance field if non-nil, zero value otherwise.

### GetBalanceOk

`func (o *Volume) GetBalanceOk() (*int64, bool)`

GetBalanceOk returns a tuple with the Balance field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBalance

`func (o *Volume) SetBalance(v int64)`

SetBalance sets Balance field to given value.

### HasBalance

`func (o *Volume) HasBalance() bool`

HasBalance returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


