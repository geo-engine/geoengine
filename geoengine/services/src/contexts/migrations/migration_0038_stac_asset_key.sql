-- NULL keeps requesting `assets.*`, as existing provider definitions do.
ALTER TYPE "StacAssetBand" ADD ATTRIBUTE asset_key text;
