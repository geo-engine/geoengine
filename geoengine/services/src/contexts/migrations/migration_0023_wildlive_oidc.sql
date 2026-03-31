CREATE TYPE "WildliveDataConnectorAuth" AS (
    "user" text,
    refresh_token text,
    expiry_date TIMESTAMPTZ
);


ALTER TYPE "WildliveDataConnectorDefinition" DROP ATTRIBUTE api_key;
ALTER TYPE "WildliveDataConnectorDefinition"
ADD ATTRIBUTE auth "WildliveDataConnectorAuth";
