-- Preserve existing plaintext credentials while replacing text attributes.
ALTER TYPE "StacProviderS3Config"
RENAME ATTRIBUTE access_key TO legacy_access_key;
ALTER TYPE "StacProviderS3Config"
RENAME ATTRIBUTE secret_key TO legacy_secret_key;
ALTER TYPE "StacProviderS3Config" ADD ATTRIBUTE access_key bytea;
ALTER TYPE "StacProviderS3Config" ADD ATTRIBUTE secret_key bytea;

UPDATE layer_providers
SET
    definition.stac_data_provider_definition.s3_config.access_key = convert_to(
        (
            ((definition).stac_data_provider_definition).s3_config
        ).legacy_access_key,
        'UTF8'
    ),
    definition.stac_data_provider_definition.s3_config.secret_key = convert_to(
        (
            ((definition).stac_data_provider_definition).s3_config
        ).legacy_secret_key,
        'UTF8'
    )
WHERE
    (
        (definition).stac_data_provider_definition
    ).s3_config IS DISTINCT FROM NULL;

ALTER TYPE "StacProviderS3Config" DROP ATTRIBUTE legacy_access_key;
ALTER TYPE "StacProviderS3Config" DROP ATTRIBUTE legacy_secret_key;

ALTER TYPE "StacProviderS3Config"
ADD ATTRIBUTE access_key_encryption_nonce bytea;
ALTER TYPE "StacProviderS3Config"
ADD ATTRIBUTE secret_key_encryption_nonce bytea;

CREATE TYPE "StacProviderAuthentication" AS (
    endpoint text,
    client_id text,
    username text,
    password bytea,
    password_encryption_nonce bytea
);

ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE authentication "StacProviderAuthentication";
