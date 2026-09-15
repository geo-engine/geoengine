CREATE TYPE "StacProviderAuthentication" AS (
    endpoint text,
    client_id text,
    username text,
    password bytea,
    password_encryption_nonce bytea
);

ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE authentication "StacProviderAuthentication";
