// Tests for the generated TypeScript client that guard the post-processing in
// `.generation/post-process/typescript.py`.
//
// Run with `just api-clients test-typescript` (after building the client).

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { test } from "node:test";

const require = createRequire(import.meta.url);
const client = require("../../typescript/dist/index.js");
const schemas = JSON.parse(
  readFileSync(new URL("../../../openapi.json", import.meta.url), "utf8"),
).components.schemas;

const refName = (ref) => ref.split("/").pop();

/** The required properties of a schema, following `allOf`. */
function requiredProperties(schema) {
  const required = [...(schema.required ?? [])];
  for (const part of schema.allOf ?? []) {
    required.push(
      ...requiredProperties(part.$ref ? schemas[refName(part.$ref)] : part),
    );
  }
  return required;
}

/** A minimal object of a variant of a `oneOf` with a discriminator. */
function sampleVariant(union, discriminatorValue, variantRef) {
  const sample = {};
  for (const property of requiredProperties(schemas[refName(variantRef)])) {
    sample[property] = {};
  }
  sample[union.discriminator.propertyName] = discriminatorValue;
  return sample;
}

/** The `oneOf` schemas with a discriminator for which the client exports an `instanceOf` check. */
const discriminatedUnions = Object.entries(schemas).filter(
  ([name, schema]) =>
    schema.oneOf &&
    schema.discriminator?.mapping &&
    typeof client[`instanceOf${name}`] === "function",
);

test("the client exports instanceOf checks for the operator unions", () => {
  const names = discriminatedUnions.map(([name]) => name);
  assert.ok(names.includes("RasterOperator"), names.join(", "));
  assert.ok(names.includes("VectorOperator"), names.join(", "));
});

for (const [name, union] of discriminatedUnions) {
  test(`instanceOf${name} accepts every variant`, () => {
    const instanceOfUnion = client[`instanceOf${name}`];
    for (const [discriminatorValue, variantRef] of Object.entries(
      union.discriminator.mapping,
    )) {
      const sample = sampleVariant(union, discriminatorValue, variantRef);
      assert.ok(
        instanceOfUnion(sample),
        `instanceOf${name} rejects ${discriminatorValue}: ${JSON.stringify(sample)}`,
      );
    }
  });
}

// `SingleRasterOrVectorOperator` is untagged, so it relies on `instanceOfRasterOperator` and
// `instanceOfVectorOperator`. A source operator missing there was serialized as `{}`.
test("a ClassHistogram over a RasterStacker serializes completely", () => {
  const processingGraph = {
    type: "Plot",
    operator: {
      type: "ClassHistogram",
      params: {},
      sources: {
        source: {
          type: "RasterStacker",
          params: { renameBands: { type: "default" } },
          sources: {
            rasters: [{ type: "GdalSource", params: { data: "ndvi" } }],
          },
        },
      },
    },
  };

  assert.deepEqual(
    JSON.parse(JSON.stringify(client.TypedPlotOperatorToJSON(processingGraph))),
    processingGraph,
  );
});

test("a Histogram over an OgrSource serializes completely", () => {
  const processingGraph = {
    type: "Plot",
    operator: {
      type: "Histogram",
      params: {
        columnName: "height",
        bounds: "data",
        buckets: { type: "number", value: 10 },
      },
      sources: { source: { type: "OgrSource", params: { data: "trees" } } },
    },
  };

  assert.deepEqual(
    JSON.parse(JSON.stringify(client.TypedPlotOperatorToJSON(processingGraph))),
    processingGraph,
  );
});
