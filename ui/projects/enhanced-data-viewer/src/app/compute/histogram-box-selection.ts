import OlGeomPolygon from 'ol/geom/Polygon';

export interface MapSelectionLike {
    overlayLayer: () =>
        | {
              getSource?: () =>
                  | {
                        getFeatures: () => Array<{getGeometry: () => unknown}>;
                    }
                  | null
                  | undefined;
          }
        | undefined;
}

export const hasDrawnBoxSelection = (map?: MapSelectionLike): boolean => {
    const overlayLayer = map?.overlayLayer();

    if (!overlayLayer) {
        return false;
    }

    const source = overlayLayer.getSource?.();

    if (!source) {
        return false;
    }

    return source.getFeatures().some((feature) => feature.getGeometry() instanceof OlGeomPolygon);
};
