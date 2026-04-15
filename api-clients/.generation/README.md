# geoengine-openapi-client - Code Generation

Code for auto-generating API clients for Geo Engine

## Dependencies

- Python
- Java Runtime

## Generation

From the root of the repository run:

```bash
just api-clients build
```

For specific languages, run:

```bash
just api-clients build-python
just api-clients build-typescript
```

## Dev-Mode

You can verify the OpenAPI spec with:

```bash
just lint-openapi-spec
```

## Update config.ini

To update the config.ini file, run:

```bash
just api-clients update-config --version <VERSION>
```

This will set a new version number.

Note, that you still need to build the clients after updating the config.ini file, otherwise the generated code will not be updated. You can do this with:

```bash
just api-clients build
```
