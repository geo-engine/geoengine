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

const sessionToken = "00000000-0000-0000-a000-000000000000";

const collections = await (
  await fetch(
    "http://localhost:3030/api/layers/collections/cbb21ee3-d15d-45c5-a175-66964adf4e85/tags%3A%2A?offset=0&limit=20",
  )
).json();
const collection = collections.items.find((c) => c.name === "NDVI");
const dataConnectorId = collection.id.providerId;
const layerId = collection.id.layerId;

console.log("Layer:", dataConnectorId, layerId);

const tms = "GeoEngineCustomTMS";

const tileUrl = `http://localhost:3030/api/ogc/${dataConnectorId}/${layerId}/collections/${layerId}/map/tiles/${tms}`;
console.log(tileUrl);

const map = new Map({
  target: "map",
  layers: [
    // new TileLayer({
    //   source: new OSM(),
    // }),
    // new TileLayer({
    //   source: new OGCMapTile({
    //     url: tileUrl,
    //     context: {
    //       datetime: "2014-06-01T00:00:00Z",
    //     },
    //   }),
    // }),
    // new TileLayer({
    //   source: new OGCMapTile({
    //     url: "https://maps.gnosis.earth/ogcapi/collections/blueMarble/map/tiles/WorldCRS84Quad",
    //   }),
    // }),
    // new TileLayer({
    //   source: new OGCMapTile({
    //     url: "http://localhost:8080/tiles/gebco",
    //   }),
    // }),
    new TileLayer({
      source: new OGCMapTile({
        url: tileUrl,
        context: {
          datetime: "2014-06-01T00:00:00Z",
        },
      }),
    }),
    new TileLayer({
      source: new TileDebug({
        source: new OGCMapTile({
          url: tileUrl,
          context: {
            datetime: "2014-06-01T00:00:00Z",
          },
        }),
      }),
    }),

    // new TileLayer({
    //   source: new OGCMapTile({
    //     url: "http://localhost:5000/collections/nasa-world-crs84/tiles/WorldCRS84Quad",
    //   }),
    // }),

    new VectorLayer({
      source: new VectorSource({
        crs: "EPSG:4326",
        features: [
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
        ],
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
  ],
  view: new View({
    center: [0, 0],
    extent: [-180, -85, 180, 85],
    zoom: 0,
    projection: "EPSG:4326",
  }),
});
