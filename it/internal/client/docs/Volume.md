# Volume

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Input** | **float32** |  | 
**Output** | **float32** |  | 
**Balance** | **float32** |  | 

## Methods

### NewVolume

`func NewVolume(input float32, output float32, balance float32, ) *Volume`

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

`func (o *Volume) GetInput() float32`

GetInput returns the Input field if non-nil, zero value otherwise.

### GetInputOk

`func (o *Volume) GetInputOk() (*float32, bool)`

GetInputOk returns a tuple with the Input field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetInput

`func (o *Volume) SetInput(v float32)`

SetInput sets Input field to given value.


### GetOutput

`func (o *Volume) GetOutput() float32`

GetOutput returns the Output field if non-nil, zero value otherwise.

### GetOutputOk

`func (o *Volume) GetOutputOk() (*float32, bool)`

GetOutputOk returns a tuple with the Output field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOutput

`func (o *Volume) SetOutput(v float32)`

SetOutput sets Output field to given value.


### GetBalance

`func (o *Volume) GetBalance() float32`

GetBalance returns the Balance field if non-nil, zero value otherwise.

### GetBalanceOk

`func (o *Volume) GetBalanceOk() (*float32, bool)`

GetBalanceOk returns a tuple with the Balance field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBalance

`func (o *Volume) SetBalance(v float32)`

SetBalance sets Balance field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


