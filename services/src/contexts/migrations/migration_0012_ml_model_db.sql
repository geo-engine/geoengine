CREATE TYPE "MlModelMetadata" AS (
    file_name text,
    input_type "RasterDataType",
    num_input_bands OID,
    output_type "RasterDataType"
);

CREATE TYPE "MlModelName" AS (namespace text, name text);

CREATE TABLE ml_models ( -- noqa: 
    id uuid PRIMARY KEY,
    "name" "MlModelName" UNIQUE NOT NULL,
    display_name text NOT NULL,
    description text NOT NULL,
    upload uuid REFERENCES uploads (id) ON DELETE CASCADE NOT NULL,
    metadata "MlModelMetadata"
);
