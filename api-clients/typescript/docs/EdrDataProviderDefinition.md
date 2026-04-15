
# EdrDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`baseUrl` | string
`cacheTtl` | number
`description` | string
`discreteVrs` | Array&lt;string&gt;
`id` | string
`name` | string
`priority` | number
`provenance` | [Array&lt;Provenance&gt;](Provenance.md)
`type` | string
`vectorSpec` | [EdrVectorSpec](EdrVectorSpec.md)

## Example

```typescript
import type { EdrDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "baseUrl": null,
  "cacheTtl": null,
  "description": null,
  "discreteVrs": null,
  "id": null,
  "name": null,
  "priority": null,
  "provenance": null,
  "type": null,
  "vectorSpec": null,
} satisfies EdrDataProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as EdrDataProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


