# OnnxObjectDetectionParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**model** | **String** | The object-detection model to run. | 
**expected_resolution** | **f64** | The spatial resolution (in linear units per pixel) the model was trained at. | 
**resolution_epsilon** | **f64** | Relative tolerance for the resolution check. A source is accepted when `|actual - expected| / expected <= resolution_epsilon`. | 
**layout** | [**models::DetectionLayout**](DetectionLayout.md) | How the model's raw output tensor is interpreted. | 
**num_classes** | **i32** | Number of object classes the model can predict. | 
**conf_threshold** | **f32** | Confidence threshold applied to decoded detections. | 
**iou_threshold** | **f32** | `IoU` threshold for non-maximum suppression. | 
**class_names** | Option<**Vec<String>**> | Optional human-readable class labels, indexed by class id. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


