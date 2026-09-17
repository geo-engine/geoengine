ALTER TYPE "StacProviderS3Config" ADD ATTRIBUTE access_key_encryption_nonce bytea;
ALTER TYPE "StacProviderS3Config" ADD ATTRIBUTE secret_key_encryption_nonce bytea;

CREATE TYPE "StacProviderAuthentication" AS (
    endpoint text,
    client_id text,
    username text,
    password bytea,
    password_encryption_nonce bytea
);

ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE authentication "StacProviderAuthentication";
