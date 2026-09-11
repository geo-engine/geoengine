CREATE TYPE "StacProviderAuthentication" AS (
    endpoint text,
    client_id text,
    username text,
    password text
);

ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE authentication "StacProviderAuthentication";
