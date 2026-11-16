---
title: Users and Permissions
sidebar:
    order: 10
---

Every request to Geo Engine runs in a session, and every session belongs to a user.
Permissions decide which datasets, layers and other resources a user can see and change.

## Users and sessions

There are three kinds of users:

- **Anonymous users.**
  `POST /anonymous` creates a new anonymous user and a session for it, without any credentials.
  Anonymous users have no email address or password.
  This is enabled by default and can be turned off with `[session] anonymous_access`.
- **Registered users** log in with email and password (`POST /login`).
  Users can register themselves with `POST /user`, unless `[user] registration` is disabled.
  The server creates an initial administrator from `[user] admin_email` and `admin_password` on startup.
- **OIDC users** log in through an OpenID Connect identity provider (`POST /oidcInit`, then `POST /oidcLogin`), if `[oidc]` is configured.

Each login returns a session.
Its `id` is the token for all further requests, sent as `Authorization: Bearer <session id>`.
A session is valid until its `validUntil` time; `POST /logout` ends it early.
See [REST API](/docs/api/rest-api#authentication).

## Roles

Permissions are granted to roles, not to users directly.
Every user has an individual role with the same id as the user, and can have further roles:

| Role            | Members                                                                                               |
| --------------- | ----------------------------------------------------------------------------------------------------- |
| individual role | Only the user                                                                                         |
| `admin`         | Administrators; they can manage roles, quotas, data connectors and datasets from volumes              |
| `user`          | All registered and OIDC users                                                                         |
| `anonymous`     | All anonymous users                                                                                   |
| custom roles    | Groups that administrators create with `PUT /roles` and assign with `POST /users/{user}/roles/{role}` |

`GET /user/roles/descriptions` lists the roles of the current user.

## Permissions

There are two permissions:

- **Read** allows a role to see and use a resource, e.g., to load a dataset in a processing graph.
- **Owner** includes Read and additionally allows changing and deleting the resource and managing its permissions.

Permissions apply to these resources:

- datasets,
- layers and layer collections,
- projects,
- ML models,
- data connectors.

The creator of a resource becomes its owner.
To share a resource, an owner adds a permission for another role:

```bash
curl -X PUT http://localhost:3030/api/permissions \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
        "resource": {"type": "dataset", "id": "<dataset name>"},
        "roleId": "<role id>",
        "permission": "Read"
    }'
```

In Python, use `ge.add_permission`, `ge.remove_permission` and `ge.list_permissions`.

Processing graphs themselves are not protected: anyone who knows the id of a processing graph can query it.
The query only succeeds, however, if the user has Read permission on all datasets and data connectors that the processing graph uses.

## Quotas

Quotas limit how much computation a user can use.
Geo Engine counts computation units for the tiles and chunks that operators produce.
`[quota] mode` controls the behavior:

| Mode       | Behavior                                                               |
| ---------- | ---------------------------------------------------------------------- |
| `disabled` | No tracking (default)                                                  |
| `track`    | Usage is recorded, but queries are not limited                         |
| `check`    | Usage is recorded, and queries fail when the user's quota is exhausted |

New users start with `[quota] initial_credits`.
Users see their quota and usage with `GET /quota` and the `/quota/…` endpoints, and administrators change quotas with `POST /quotas/{user}`.
