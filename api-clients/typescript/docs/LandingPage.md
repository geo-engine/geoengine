
# LandingPage

The Landing page is the entry point of a OGC API.  The Landing page provides links to: * the API definition (link relations `service-desc` and `service-doc`), * the Conformance declaration (path `/conformance`, link relation `conformance`), and * the Collections (path `/collections`, link relation `data`).

## Properties

Name | Type
------------ | -------------
`title` | string
`description` | string
`attribution` | string
`links` | [Array&lt;Link&gt;](Link.md)

## Example

```typescript
import type { LandingPage } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "title": null,
  "description": null,
  "attribution": null,
  "links": null,
} satisfies LandingPage

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as LandingPage
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


