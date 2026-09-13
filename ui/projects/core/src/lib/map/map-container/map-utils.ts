import OlMap from 'ol/Map';
import {DoubleClickZoom} from 'ol/interaction';

export function setMapDoubleClickZoom(map: OlMap, enabled: boolean): void {
    map.getInteractions()
        .getArray()
        .filter((interaction) => interaction instanceof DoubleClickZoom)
        .forEach((interaction) => interaction.setActive(enabled));
}

export function setMapsDoubleClickZoom(maps: OlMap[], enabled: boolean): void {
    maps.forEach((map) => setMapDoubleClickZoom(map, enabled));
}
