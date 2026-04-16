# Geo Engine API Clients

This directory contains multiple API client libraries.

## Clients

All clients are generated from the OpenAPI specification of the Geo Engine API, which is located in the [`openapi.json`](../openapi.json) file.

### Python

The Python API client is available on [PyPI](https://pypi.org/project/geoengine-api-client/) and can be installed with:

```bash
pip install geoengine-api-client
```

You can find the code in the [`python`](python) directory.

### TypeScript

The TypeScript API client is available on [npm](https://www.npmjs.com/package/@geoengine/api-client) and can be added to your project with:

```bash
npm install @geoengine/api-client
```

You can find the code in the [`typescript`](typescript) directory.

### Rust

The Rust API client is available on [crates.io](https://crates.io/crates/geoengine-api-client) and can be added to your project with:

```bash
cargo add geoengine-api-client
```

## Code Generation

For information on how to generate the packages out of an OpenAPI spec, please refer to the [README](.generation/README.md) file in the [`.generation`](.generation) directory.
