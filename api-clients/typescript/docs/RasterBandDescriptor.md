
# RasterBandDescriptor


## Properties

Name | Type
------------ | -------------
`name` | string
`measurement` | [Measurement](Measurement.md)

## Example

```typescript
import type { RasterBandDescriptor } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "name": null,
  "measurement": null,
} satisfies RasterBandDescriptor

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterBandDescriptor
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


