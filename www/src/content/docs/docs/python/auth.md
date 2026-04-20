---
sidebar_label: auth
title: auth
---

Module for encapsulating Geo Engine authentication

## BearerAuth Objects

```python
class BearerAuth(AuthBase)
```

A bearer token authentication for `requests`

## Session Objects

```python
class Session()
```

A Geo Engine session

#### \_\_init\_\_

```python
def __init__(server_url: str,
             credentials: tuple[str, str] | None = None,
             token: str | None = None) -> None
```

Initialize communication between this library and a Geo Engine instance

If credentials or a token are provided, the session will be authenticated.
Credentials and token must not be provided at the same time.

optional arguments:

- `(email, password)` as tuple
- `token` as a string

optional environment variables:

- `GEOENGINE_EMAIL`
- `GEOENGINE_PASSWORD`
- `GEOENGINE_TOKEN`

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Display representation of a session

#### auth_header

```python
@property
def auth_header() -> dict[str, str]
```

Create an authentication header for the current session

#### server_url

```python
@property
def server_url() -> str
```

Return the server url of the current session

#### configuration

```python
@property
def configuration() -> geoengine_api_client.Configuration
```

Return the current http configuration

#### user_id

```python
@property
def user_id() -> UUID
```

Return the user id. Only works in Geo Engine Pro.

#### requests_bearer_auth

```python
def requests_bearer_auth() -> BearerAuth
```

Return a Bearer authentication object for the current session

#### logout

```python
def logout()
```

Logout the current session

#### get_session

```python
def get_session() -> Session
```

Return the global session if it exists

Raises an exception otherwise.

#### initialize

```python
def initialize(server_url: str,
               credentials: tuple[str, str] | None = None,
               token: str | None = None) -> None
```

Initialize communication between this library and a Geo Engine instance

If credentials or a token are provided, the session will be authenticated.
Credentials and token must not be provided at the same time.

optional arugments: (email, password) as tuple or token as a string
optional environment variables: GEOENGINE_EMAIL, GEOENGINE_PASSWORD, GEOENGINE_TOKEN
optional .env file defining: GEOENGINE_EMAIL, GEOENGINE_PASSWORD, GEOENGINE_TOKEN

#### reset

```python
def reset(logout: bool = True) -> None
```

Resets the current session
