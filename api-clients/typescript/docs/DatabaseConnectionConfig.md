
# DatabaseConnectionConfig


## Properties

Name | Type
------------ | -------------
`database` | string
`host` | string
`password` | string
`port` | number
`schema` | string
`user` | string

## Example

```typescript
import type { DatabaseConnectionConfig } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "database": null,
  "host": null,
  "password": null,
  "port": null,
  "schema": null,
  "user": null,
} satisfies DatabaseConnectionConfig

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DatabaseConnectionConfig
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


