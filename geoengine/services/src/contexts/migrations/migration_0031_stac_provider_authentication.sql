CREATE TYPE "StacProviderAuthentication" AS (
    endpoint text,
    username text,
    password text
);

ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE authentication "StacProviderAuthentication";
