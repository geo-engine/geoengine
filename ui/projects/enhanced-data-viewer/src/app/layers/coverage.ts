import GeoJSON from 'ol/format/GeoJSON';
import Polygon from 'ol/geom/Polygon';
import MultiPolygon from 'ol/geom/MultiPolygon';
import {equals, squaredDistance} from 'ol/coordinate';
import {containsExtent} from 'ol/extent';
import {toLonLat} from 'ol/proj';

export interface GeographicCenter {
    longitude: number;
    latitude: number;
}

/** Coverage geometries remain in GeoJSON's WGS84 longitude/latitude coordinates. */
export type GeographicCoverage = Polygon | MultiPolygon;

const geoJSON = new GeoJSON();

/** Read the bootstrapper's edv:coverage metadata, ignoring unusable geometries. */
export function parseCoverage(value: string | undefined): GeographicCoverage | undefined {
    if (!value) return undefined;
    try {
        const geometry = geoJSON.readGeometry(value, {dataProjection: 'EPSG:4326', featureProjection: 'EPSG:4326'});
        if (!(geometry instanceof Polygon || geometry instanceof MultiPolygon)) return undefined;
        // OpenLayers reads GeoJSON but is not a validator. Keep basic guards for catalogue metadata.
        if (!geometry.getFlatCoordinates().every(Number.isFinite) || !containsExtent([-180, -90, 180, 90], geometry.getExtent()))
            return undefined;
        const polygons = geometry instanceof Polygon ? [geometry] : geometry.getPolygons();
        return polygons.length && polygons.every(isUsablePolygon) ? geometry : undefined;
    } catch {
        return undefined;
    }
}

/** Boundary points count as covered; holes exclude their interior but include their boundary. */
export function coverageContains(coverage: GeographicCoverage, center: GeographicCenter): boolean {
    if (!Number.isFinite(center.longitude) || !Number.isFinite(center.latitude) || center.latitude < -90 || center.latitude > 90)
        return false;
    const coordinate = toLonLat([center.longitude, center.latitude], 'EPSG:4326');
    // Both representations of the antimeridian describe the same map center.
    const coordinates = Math.abs(coordinate[0]) === 180 ? [coordinate, [-coordinate[0], coordinate[1]]] : [coordinate];
    return coordinates.some(
        (point) =>
            coverage.intersectsCoordinate(point) ||
            // intersectsCoordinate excludes boundaries; getClosestPoint also covers hole boundaries.
            squaredDistance(point, coverage.getClosestPoint(point)) <= 1e-20,
    );
}

function isUsablePolygon(polygon: Polygon): boolean {
    const rings = polygon.getLinearRings();
    return (
        rings.length > 0 &&
        rings.every(
            (ring) =>
                ring.getCoordinates().length >= 4 &&
                equals(ring.getFirstCoordinate(), ring.getLastCoordinate()) &&
                Math.abs(ring.getArea()) > 5e-13,
        )
    );
}
