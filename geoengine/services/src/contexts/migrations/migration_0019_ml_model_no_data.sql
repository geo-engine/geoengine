CREATE TYPE "MlModelInputNoDataHandlingVariant" AS ENUM (
    'EncodedNoData',
    'SkipIfNoData'
);

CREATE TYPE "MlModelInputNoDataHandling" AS (
    variant "MlModelInputNoDataHandlingVariant",
    no_data_value real
);

CREATE TYPE "MlModelOutputNoDataHandlingVariant" AS ENUM (
    'EncodedNoData',
    'NanIsNoData'
);

CREATE TYPE "MlModelOutputNoDataHandling" AS (
    variant "MlModelOutputNoDataHandlingVariant",
    no_data_value real
);

ALTER TYPE "MlModelMetadata" ADD ATTRIBUTE
input_no_data_handling "MlModelInputNoDataHandling";

ALTER TYPE "MlModelMetadata" ADD ATTRIBUTE
output_no_data_handling "MlModelOutputNoDataHandling";

ALTER TABLE ml_models ADD file_name text;

WITH qqqq AS (
    SELECT
        id,
        metadata,
        file_name
    FROM ml_models
    WHERE (metadata).file_name IS NOT NULL
)

UPDATE ml_models
SET
    metadata.input_no_data_handling = ('SkipIfNoData', NULL),
    metadata.output_no_data_handling = ('NanIsNoData', NULL),
    file_name = (qqqq.metadata).file_name
FROM qqqq
WHERE ml_models.id = qqqq.id;

ALTER TYPE "MlModelMetadata" DROP ATTRIBUTE file_name;
