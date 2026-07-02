
# EdrDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`name` | string
`description` | string
`priority` | number
`id` | string
`baseUrl` | string
`vectorSpec` | [EdrVectorSpec](EdrVectorSpec.md)
`cacheTtl` | number
`discreteVrs` | Array&lt;string&gt;
`provenance` | [Array&lt;Provenance&gt;](Provenance.md)

## Example

```typescript
import type { EdrDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "name": null,
  "description": null,
  "priority": null,
  "id": null,
  "baseUrl": null,
  "vectorSpec": null,
  "cacheTtl": null,
  "discreteVrs": null,
  "provenance": null,
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


