---
title: REST API
sidebar:
    order: 1
---

The REST API is served under the path `/api` of the server, e.g., `http://localhost:3030/api` for a local instance.
`[web] api_prefix` changes the path, and `[web] external_address` sets the public address if the server runs behind a proxy.
Request and response bodies are JSON with `camelCase` field names.

## OpenAPI specification

The API is described by an OpenAPI specification.
A running server serves it at `/api/api-docs/openapi.json` and an interactive Swagger UI at `/api/swagger-ui/`.
The specification is also part of the repository as [`openapi.json`](https://github.com/geo-engine/geoengine/blob/main/openapi.json).

API clients for Python, Rust and TypeScript are generated from the specification and published as packages.
The [`geoengine` Python library](/docs/python/) builds on the generated Python client.

## Authentication

Almost every endpoint requires a session.
Create one with one of these endpoints:

| Endpoint                            | Session for                                                      |
| ----------------------------------- | ---------------------------------------------------------------- |
| `POST /anonymous`                   | A new anonymous user, if `[session] anonymous_access` is enabled |
| `POST /login`                       | A registered user, with `email` and `password`                   |
| `POST /oidcInit`, `POST /oidcLogin` | A user of an OpenID Connect identity provider                    |

```bash
curl -X POST http://localhost:3030/api/login \
    -H "Content-Type: application/json" \
    -d '{"email": "admin@localhost", "password": "adminadmin"}'
```

The response describes the session:

```json
{
    "id": "<session id>",
    "user": {
        "id": "<user id>",
        "email": "admin@localhost",
        "realName": "admin"
    },
    "created": "2026-01-01T00:00:00.000Z",
    "validUntil": "2026-01-31T00:00:00.000Z",
    "project": null,
    "view": null,
    "roles": ["<role id>", "<role id>"]
}
```

`roles` lists the ids of the user's roles, including the user's individual role.
Sessions are valid for 30 days.

Send the session `id` as a bearer token in the `Authorization` header of all further requests, including OGC requests:

```http
Authorization: Bearer <session id>
```

`GET /session` returns the current session, and `POST /logout` ends it.
See [Users and Permissions](/docs/the-geo-engine/users-and-permissions) for users, roles and permissions.

## Typical workflow

1. **Find data.**
   Browse layer collections (`GET /layers/collections`, then `GET /layers/collections/{provider}/{collection}`) or list datasets (`GET /datasets`).
2. **Register a processing graph.**
   `POST /processingGraphs` with the processing graph as body returns its id.
   For a layer, `POST /layers/{provider}/{layer}/processingGraphId` does the same.
   See [Processing Graphs](/docs/the-geo-engine/processing-graphs).
3. **Inspect the result.**
   `GET /processingGraphs/{id}/metadata` returns the result descriptor.
4. **Query the result.**
   Use the [OGC services](/docs/api/ogc) for maps, features and coverages, `GET /plot/{id}` for plots, or the WebSocket `GET /processingGraphs/{id}/rasterStream` for a stream of raster tiles in Apache Arrow format.
   See [Queries and Streams](/docs/the-geo-engine/queries-and-streams).

Long-running operations, such as creating a dataset from a processing graph, run as tasks.
They return a task id, whose progress is available at `GET /tasks/{id}/status`.

## Endpoints

The [Endpoints](/docs/api/endpoints) page lists all endpoints with a short description.
It is generated from the OpenAPI specification, so it always matches the current API.
For request and response bodies, use the Swagger UI of the server or the specification itself.

## Errors

Errors are returned with a matching HTTP status code and a JSON body:

```json
{
    "error": "Unauthorized",
    "message": "Authorization error: Header with authorization token not provided."
}
```
