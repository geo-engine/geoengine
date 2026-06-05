# UserSession

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **uuid::Uuid** |  | 
**user** | [**models::UserInfo**](UserInfo.md) |  | 
**created** | **String** |  | 
**valid_until** | **String** |  | 
**project** | Option<**uuid::Uuid**> |  | [optional]
**view** | Option<[**models::StRectangle**](STRectangle.md)> |  | [optional]
**roles** | **Vec<uuid::Uuid>** |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


