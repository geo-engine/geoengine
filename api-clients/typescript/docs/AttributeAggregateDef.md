
# AttributeAggregateDef


## Properties

Name | Type
------------ | -------------
`columnName` | string
`aggregateType` | [AttributeAggregateType](AttributeAggregateType.md)
`measurement` | [Measurement](Measurement.md)

## Example

```typescript
import type { AttributeAggregateDef } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "columnName": null,
  "aggregateType": null,
  "measurement": null,
} satisfies AttributeAggregateDef

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AttributeAggregateDef
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


