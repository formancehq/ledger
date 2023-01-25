# TaskStripeDescriptor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Main** | Pointer to **bool** |  | [optional] 
**Account** | Pointer to **string** |  | [optional] 

## Methods

### NewTaskStripeDescriptor

`func NewTaskStripeDescriptor(name string, ) *TaskStripeDescriptor`

NewTaskStripeDescriptor instantiates a new TaskStripeDescriptor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTaskStripeDescriptorWithDefaults

`func NewTaskStripeDescriptorWithDefaults() *TaskStripeDescriptor`

NewTaskStripeDescriptorWithDefaults instantiates a new TaskStripeDescriptor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *TaskStripeDescriptor) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *TaskStripeDescriptor) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *TaskStripeDescriptor) SetName(v string)`

SetName sets Name field to given value.


### GetMain

`func (o *TaskStripeDescriptor) GetMain() bool`

GetMain returns the Main field if non-nil, zero value otherwise.

### GetMainOk

`func (o *TaskStripeDescriptor) GetMainOk() (*bool, bool)`

GetMainOk returns a tuple with the Main field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMain

`func (o *TaskStripeDescriptor) SetMain(v bool)`

SetMain sets Main field to given value.

### HasMain

`func (o *TaskStripeDescriptor) HasMain() bool`

HasMain returns a boolean if a field has been set.

### GetAccount

`func (o *TaskStripeDescriptor) GetAccount() string`

GetAccount returns the Account field if non-nil, zero value otherwise.

### GetAccountOk

`func (o *TaskStripeDescriptor) GetAccountOk() (*string, bool)`

GetAccountOk returns a tuple with the Account field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccount

`func (o *TaskStripeDescriptor) SetAccount(v string)`

SetAccount sets Account field to given value.

### HasAccount

`func (o *TaskStripeDescriptor) HasAccount() bool`

HasAccount returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


