
# MlModel


## Properties

Name | Type
------------ | -------------
`name` | string
`displayName` | string
`description` | string
`upload` | string
`metadata` | [MlModelMetadata](MlModelMetadata.md)
`fileName` | string

## Example

```typescript
import type { MlModel } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "name": null,
  "displayName": null,
  "description": null,
  "upload": null,
  "metadata": null,
  "fileName": null,
} satisfies MlModel

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MlModel
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


