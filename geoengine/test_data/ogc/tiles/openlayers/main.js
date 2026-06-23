import Map from "ol/Map.js";
import View from "ol/View.js";
import TileLayer from "ol/layer/Tile.js";
import VectorLayer from "ol/layer/Vector.js";
import VectorSource from "ol/source/Vector.js";
import OGCMapTile from "ol/source/OGCMapTile.js";
import OSM from "ol/source/OSM.js";
import TileDebug from "ol/source/TileDebug.js";
import Feature from "ol/Feature.js";
import Point from "ol/geom/Point.js";
import Style from "ol/style/Style.js";
import Circle from "ol/style/Circle.js";
import Fill from "ol/style/Fill.js";
import Text from "ol/style/Text.js";
import Stroke from "ol/style/Stroke.js";

const SERVER_URL = "http://localhost:3030";

async function getSessionToken() {
  try {
    const response = await fetch("http://localhost:3030/api/anonymous", {
      method: "POST",
    });
    const data = await response.json();
    return data.id;
  } catch (error) {
    // do nothing, we will use the default token which should work for testing
    console.warn("Failed to get session token, using default token");
    return "00000000-0000-0000-a000-000000000000";
  }
}

async function addWgs84TileLayer(map) {
  const collections = await (
    await fetch(
      `${SERVER_URL}/api/layers/collections/cbb21ee3-d15d-45c5-a175-66964adf4e85/tags%3A%2A?offset=0&limit=20`,
    )
  ).json();
  const collection = collections.items.find((c) => c.name === "NDVI");
  const dataConnectorId = collection.id.providerId;
  const layerId = collection.id.layerId;

  const tms = "GeoEngineCustomTMS";

  const tileUrl = `${SERVER_URL}/api/ogc/${dataConnectorId}/${layerId}/collections/${layerId}/map/tiles/${tms}`;
  console.log("Layer WGS84:", dataConnectorId, layerId, "\n" + tileUrl);

  map.addLayer(
    new TileLayer({
      source: new OGCMapTile({
        url: tileUrl,
        context: {
          datetime: "2014-04-01T00:00:00Z",
        },
        wrapX: false, // CRITICAL: Stops OpenLayers from forcing standard world-wrapping math
      }),
    }),
  );
  map.addLayer(
    new TileLayer({
      source: new TileDebug({
        source: new OGCMapTile({
          url: tileUrl,
          context: {
            datetime: "2014-04-01T00:00:00Z",
          },
          wrapX: false, // CRITICAL: Stops OpenLayers from forcing standard world-wrapping math
        }),
      }),
    }),
  );
}

async function addWebMercatorTileLayer(map) {
  const collections = await (
    await fetch(
      `${SERVER_URL}/api/layers/collections/cbb21ee3-d15d-45c5-a175-66964adf4e85/tags%3A%2A?offset=0&limit=20`,
    )
  ).json();
  const collection = collections.items.find((c) => c.name === "NDVI3857");
  const dataConnectorId = collection.id.providerId;
  const layerId = collection.id.layerId;

  const tms = "GeoEngineCustomTMS";

  const tileUrl = `${SERVER_URL}/api/ogc/${dataConnectorId}/${layerId}/collections/${layerId}/map/tiles/${tms}`;
  console.log("Layer WebMercator:", dataConnectorId, layerId, "\n" + tileUrl);

  map.addLayer(
    new TileLayer({
      source: new OGCMapTile({
        url: tileUrl,
        context: {
          datetime: "2014-04-01T00:00:00Z",
        },
        wrapX: false, // CRITICAL: Stops OpenLayers from forcing standard world-wrapping math
      }),
    }),
  );
  map.addLayer(
    new TileLayer({
      source: new TileDebug({
        source: new OGCMapTile({
          url: tileUrl,
          context: {
            datetime: "2014-04-01T00:00:00Z",
          },
          wrapX: false, // CRITICAL: Stops OpenLayers from forcing standard world-wrapping math
        }),
      }),
    }),
  );
}

async function addCitiesLayer(map) {
  const sourceProjection = "EPSG:4326";
  const targetProjection = map.getView().getProjection().getCode();
  const features = [
    new Feature({
      geometry: new Point([0, 0]),
      properties: { name: "Origin" },
    }),
    new Feature({
      geometry: new Point([6.960162, 50.93804]),
      properties: { name: "Cologne" },
    }),
    new Feature({
      geometry: new Point([13.404954, 52.520008]),
      properties: { name: "Berlin" },
    }),
    new Feature({
      geometry: new Point([2.352222, 48.856613]),
      properties: { name: "Paris" },
    }),
    new Feature({
      geometry: new Point([-0.127758, 51.507351]),
      properties: { name: "London" },
    }),
  ].map((feature) => {
    const geom = feature.getGeometry();
    geom.transform(sourceProjection, targetProjection);
    return feature;
  });
  map.addLayer(
    new VectorLayer({
      source: new VectorSource({
        projection: targetProjection,
        features,
      }),
      style: function (feature) {
        const iconStyle = new Style({
          image: new Circle({
            radius: 5,
            fill: new Fill({ color: "red" }),
          }),
        });
        const labelStyle = new Style({
          text: new Text({
            font: "12px Calibri,sans-serif",
            overflow: true,
            fill: new Fill({ color: "black" }),
            stroke: new Stroke({
              color: "white",
              width: 3,
            }),
            textBaseline: "top",
            offsetY: 16,
          }),
        });

        labelStyle.getText().setText(feature.getProperties().properties.name);
        return [iconStyle, labelStyle];
      },
    }),
  );
}

const wgs84Map = new Map({
  target: "wgs84Map",
  layers: [
    // new TileLayer({
    //   source: new OSM(),
    // }),
  ],
  view: new View({
    center: [0, 0],
    extent: [-180, -85, 180, 85],
    zoom: 0,
    projection: "EPSG:4326",
    showFullExtent: true,
  }),
});

const webMercatorMap = new Map({
  target: "webMercatorMap",
  layers: [
    // new TileLayer({
    //   source: new OSM(),
    // }),
  ],
  view: new View({
    center: [0, 0],
    extent: [
      -20037508.342789244, -20037508.342789244, 20037508.342789244,
      20037508.342789244,
    ],
    zoom: 0,
    projection: "EPSG:3857",
    showFullExtent: true,
  }),
});

const sessionToken = await getSessionToken();
await Promise.all([
  addWgs84TileLayer(wgs84Map),
  addWebMercatorTileLayer(webMercatorMap),
]);
await Promise.all([addCitiesLayer(wgs84Map), addCitiesLayer(webMercatorMap)]);
