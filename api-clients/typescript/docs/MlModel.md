
# MlModel


## Properties

Name | Type
------------ | -------------
`description` | string
`displayName` | string
`fileName` | string
`metadata` | [MlModelMetadata](MlModelMetadata.md)
`name` | string
`upload` | string

## Example

```typescript
import type { MlModel } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "description": null,
  "displayName": null,
  "fileName": null,
  "metadata": null,
  "name": null,
  "upload": null,
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


