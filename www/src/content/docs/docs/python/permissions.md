---
sidebar_label: permissions
title: permissions
---

A wrapper for the GeoEngine permissions API.

## RoleId Objects

```python
class RoleId()
```

A wrapper for a role id

#### from_response

```python
@classmethod
def from_response(cls, response: dict[str, str]) -> RoleId
```

Parse a http response to an `RoleId`

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Checks if two role ids are equal

## Role Objects

```python
class Role()
```

A wrapper for a role

#### \_\_init\_\_

```python
def __init__(role_id: UUID | RoleId | str, role_name: str)
```

Create a role with name and id

#### from_response

```python
@classmethod
def from_response(cls,
                  response: geoengine_api_client.models.role.Role) -> Role
```

Parse a http response to an `RoleId`

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Checks if two role ids are equal

#### role_id

```python
def role_id() -> RoleId
```

get the role id

## UserId Objects

```python
class UserId()
```

A wrapper for a role id

#### from_response

```python
@classmethod
def from_response(cls, response: dict[str, str]) -> UserId
```

Parse a http response to an `UserId`

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Checks if two role ids are equal

## PermissionListing Objects

```python
class PermissionListing()
```

PermissionListing

#### \_\_init\_\_

```python
def __init__(permission: Permission, resource: Resource, role: Role)
```

Create a PermissionListing

#### from_response

```python
@classmethod
def from_response(
    cls, response: geoengine_api_client.models.PermissionListing
) -> PermissionListing
```

Transforms a response PermissionListing to a PermissionListing

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Checks if two listings are equal

## Permission Objects

```python
class Permission(str, Enum)
```

A permission

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.Permission
```

Convert to a dict for the API

#### add_permission

```python
def add_permission(role: RoleId,
                   resource: Resource,
                   permission: Permission,
                   timeout: int = 60)
```

Add a permission to a resource for a role. Requires admin role.

#### remove_permission

```python
def remove_permission(role: RoleId,
                      resource: Resource,
                      permission: Permission,
                      timeout: int = 60)
```

Removes a permission to a resource from a role. Requires admin role.

#### list_permissions

```python
def list_permissions(resource: Resource,
                     timeout: int = 60,
                     offset=0,
                     limit=20) -> list[PermissionListing]
```

Lists the roles and permissions assigned to a ressource

#### add_role

```python
def add_role(name: str, timeout: int = 60) -> RoleId
```

Add a new role. Requires admin role.

#### remove_role

```python
def remove_role(role: RoleId, timeout: int = 60)
```

Remove a role. Requires admin role.

#### assign_role

```python
def assign_role(role: RoleId, user: UserId, timeout: int = 60)
```

Assign a role to a user. Requires admin role.

#### revoke_role

```python
def revoke_role(role: RoleId, user: UserId, timeout: int = 60)
```

Assign a role to a user. Requires admin role.
