-- `NULL` means that the time of the datasets comes from the temporal
-- extent of the collection, so existing providers need no value
ALTER TYPE "StacDataProviderDefinition"
ADD ATTRIBUTE time_bounds "TimeInterval";
