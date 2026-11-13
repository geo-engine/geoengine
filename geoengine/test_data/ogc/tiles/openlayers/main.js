import Map from "ol/Map.js";
import View from "ol/View.js";
import TileState from "ol/TileState.js";
import TileLayer from "ol/layer/Tile.js";
import WebGLTileLayer from "ol/layer/WebGLTile.js";
import VectorLayer from "ol/layer/Vector.js";
import VectorSource from "ol/source/Vector.js";
import DataTile from "ol/source/DataTile.js";
import OGCMapTile from "ol/source/OGCMapTile.js";
import TileDebug from "ol/source/TileDebug.js";
import { getTileSetInfo } from "ol/source/ogcTileUtil.js";
import { toSize } from "ol/size.js";
import { unByKey } from "ol/Observable.js";
import Feature from "ol/Feature.js";
import Point from "ol/geom/Point.js";
import Style from "ol/style/Style.js";
import Circle from "ol/style/Circle.js";
import Fill from "ol/style/Fill.js";
import Text from "ol/style/Text.js";
import Stroke from "ol/style/Stroke.js";
import { fromArrayBuffer } from "geotiff";

const SERVER_URL = `${window.location.protocol}//${window.location.hostname}:${window.location.port}`; // proxied by vite
const ACTUAL_SERVER_URL = "http://localhost:3030";

const DATA_CONNECTOR_ID = "cbb21ee3-d15d-45c5-a175-66964adf4e85";
const DATETIME = "2014-04-01T00:00:00Z";

/** Media type of coverage tiles */
const GEOTIFF = "image/tiff; application=geotiff";

/** `NewSubfileType` of the internal mask that GDAL writes into a GeoTIFF */
const FILETYPE_MASK = 4;

/**
 * Colors for the raw NDVI values (bytes) of coverage tiles.
 * Band 1 is the value, band 2 the validity mask.
 */
const COVERAGE_STYLE = {
  color: [
    "case",
    ["==", ["band", 2], 0],
    [0, 0, 0, 0],
    [
      "interpolate",
      ["linear"],
      ["band", 1],
      0,
      [140, 81, 10, 1],
      128,
      [246, 232, 195, 1],
      255,
      [1, 102, 94, 1],
    ],
  ],
};

async function getSessionToken() {
  try {
    const response = await fetch("/api/anonymous", {
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

/**
 * Fetches the tileset metadata with the session token and returns it as a blob URL,
 * with all links pointing to the vite proxy.
 *
 * @param {"map" | "coverage"} tileType
 */
async function tileSetBlobUrl(
  dataConnectorId,
  layerId,
  tileType,
  tms,
  sessionToken,
) {
  const tileSetUrl = `${SERVER_URL}/api/ogc/${dataConnectorId}/${layerId}/collections/${layerId}/${tileType}/tiles/${tms}`;

  return await urlToBlobUrl(tileSetUrl, sessionToken, async (metadata) => {
    if (metadata.dataType === "coverage") {
      // OpenLayers only parses tilesets of the data types `map` and `vector`.
      // We use it for the tile grid and the URL template and decode the GeoTIFF tiles ourselves.
      metadata.dataType = "map";
    }

    for (const link of metadata.links) {
      console.log("Tileset link:", link.rel, link.href, link.type);

      // replace urls
      if (link.href.startsWith(ACTUAL_SERVER_URL)) {
        link.href = link.href.replace(ACTUAL_SERVER_URL, SERVER_URL);
      }

      if (link.rel === "http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme") {
        link.href = await urlToBlobUrl(link.href, sessionToken);
      }
    }
  });
}

async function urlToBlobUrl(url, sessionToken, interceptor) {
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${sessionToken}`,
    },
  });
  const metadata = await response.json();

  if (interceptor) {
    await interceptor(metadata);
  }

  const blob = new Blob([JSON.stringify(metadata)], {
    type: "application/json",
  });
  return URL.createObjectURL(blob);
}

async function findLayer(sessionToken, layerName) {
  const collections = await (
    await fetch(
      `${SERVER_URL}/api/layers/collections/${DATA_CONNECTOR_ID}/tags%3A%2A?offset=0&limit=20`,
      {
        headers: {
          Authorization: `Bearer ${sessionToken}`,
        },
      },
    )
  ).json();
  const collection = collections.items.find((c) => c.name === layerName);
  return {
    dataConnectorId: collection.id.providerId,
    layerId: collection.id.layerId,
  };
}

/**
 * Adds the tiles of the layer `layerName` and a tile debug layer to `map`.
 *
 * @param {"map" | "coverage"} tileType
 * @returns the layer with the tiles
 */
async function addTileLayer(map, sessionToken, layerName, tms, tileType) {
  const { dataConnectorId, layerId } = await findLayer(sessionToken, layerName);

  const tileSetUrl = `${SERVER_URL}/api/ogc/${dataConnectorId}/${layerId}/collections/${layerId}/${tileType}/tiles/${tms}`;
  console.log(
    `Layer ${layerName}:`,
    dataConnectorId,
    layerId,
    "\n" + tileSetUrl,
  );

  const url = await tileSetBlobUrl(
    dataConnectorId,
    layerId,
    tileType,
    tms,
    sessionToken,
  );

  const layer =
    tileType === "coverage"
      ? new WebGLTileLayer({
          source: await coverageTileSource(url, sessionToken),
          style: COVERAGE_STYLE,
        })
      : new TileLayer({ source: mapTileSource(url, sessionToken) });

  map.addLayer(layer);
  map.addLayer(
    new TileLayer({
      source: new TileDebug({
        source: mapTileSource(url, sessionToken),
      }),
    }),
  );

  return layer;
}

/** A source of map tiles (PNG) */
function mapTileSource(url, sessionToken) {
  return new OGCMapTile({
    url,
    context: {
      datetime: DATETIME,
    },
    wrapX: false, // CRITICAL: Stops OpenLayers from forcing standard world-wrapping math
    tileLoadFunction: tileLoadFunction(sessionToken),
    interpolate: false,
  });
}

/**
 * A source of coverage tiles (GeoTIFF) with two bands: the raw value and the validity mask (0 or 1)
 */
async function coverageTileSource(url, sessionToken) {
  const tileSetInfo = await getTileSetInfo({
    url,
    mediaType: GEOTIFF,
    context: {
      datetime: DATETIME,
    },
  });

  return new DataTile({
    projection: tileSetInfo.projection,
    tileGrid: tileSetInfo.grid,
    bandCount: 2,
    wrapX: false,
    interpolate: false,
    loader: async (z, x, y) => {
      const [width, height] = toSize(tileSetInfo.grid.getTileSize(z));
      const data = new Float32Array(width * height * 2); // everything invalid

      const tileUrl = tileSetInfo.urlFunction([z, x, y]);
      if (!tileUrl) {
        return data;
      }

      const response = await fetch(tileUrl, {
        headers: {
          Authorization: `Bearer ${sessionToken}`,
        },
      });
      if (!response.ok) {
        throw new Error(`Tile fetch failed with status: ${response.status}`);
      }

      const { values, mask } = await readCoverageTile(
        await response.arrayBuffer(),
      );

      for (let i = 0; i < width * height; ++i) {
        data[2 * i] = values[i];
        data[2 * i + 1] = mask ? mask[i] : 1;
      }

      return data;
    },
  });
}

/**
 * Reads the first band of a coverage tile and its validity mask, if any.
 */
async function readCoverageTile(arrayBuffer) {
  const tiff = await fromArrayBuffer(arrayBuffer);

  const [values] = await (await tiff.getImage(0)).readRasters();

  let mask;
  for (let i = 1; i < (await tiff.getImageCount()); ++i) {
    const image = await tiff.getImage(i);
    if (image.fileDirectory.getValue("NewSubfileType") & FILETYPE_MASK) {
      [mask] = await image.readRasters();
      break;
    }
  }

  return { values, mask };
}

function tileLoadFunction(sessionToken) {
  return async function (olTile, src) {
    try {
      const response = await fetch(src, {
        headers: {
          Authorization: `Bearer ${sessionToken}`,
        },
      });

      if (!response.ok) {
        throw new Error(`Tile fetch failed with status: ${response.status}`);
      }

      const blob = await response.blob();

      // Successfully assign the object URL to the image element
      olTile.getImage().src = URL.createObjectURL(blob);
    } catch (error) {
      console.error("Error loading OGC tile:", error);

      // CRITICAL: You must explicitly catch errors and notify OpenLayers,
      // otherwise the map will wait indefinitely for this tile to resolve.
      olTile.setState(TileState.ERROR);
    }
  };
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

function createView(tms, isWgs84) {
  if (tms === "Custom") {
    // For Custom: WGS84 uses 4326, WebMercator uses 3857
    const projection = isWgs84 ? "EPSG:4326" : "EPSG:3857";
    const extent = isWgs84
      ? [-180, -85, 180, 85]
      : [
          -20037508.342789244, -20037508.342789244, 20037508.342789244,
          20037508.342789244,
        ];

    return new View({
      center: [0, 0],
      extent: extent,
      zoom: 0,
      minZoom: 0,
      projection: projection,
      showFullExtent: true,
      multiWorld: true,
    });
  } else {
    // For WebMercatorQuad: both use 3857
    return new View({
      center: [0, 0],
      extent: [
        -20037508.342789244, -20037508.342789244, 20037508.342789244,
        20037508.342789244,
      ],
      zoom: 0,
      minZoom: 0,
      projection: "EPSG:3857",
      showFullExtent: true,
      multiWorld: true,
    });
  }
}

/**
 * Shows the raw value of the coverage tile below the pointer in `element`
 *
 * @returns the key of the listener
 */
function showValueOnHover(map, layer, element) {
  element.textContent = "Value: move the pointer over the map";

  return map.on("pointermove", (event) => {
    if (event.dragging) {
      return;
    }

    const data = layer.getData(event.pixel);
    element.textContent =
      data && data[1] > 0 ? `Value: ${data[0]}` : "Value: no data";
  });
}

const maps = [
  {
    map: new Map({
      target: "wgs84Map",
      layers: [],
      view: createView("Custom", true),
    }),
    layerName: "NDVI",
    isWgs84: true,
    valueElement: document.getElementById("wgs84Value"),
  },
  {
    map: new Map({
      target: "webMercatorMap",
      layers: [],
      view: createView("Custom", false),
    }),
    layerName: "NDVI3857",
    isWgs84: false,
    valueElement: document.getElementById("webMercatorValue"),
  },
];

const sessionToken = await getSessionToken();

const tmsSelector = document.getElementById("tms-selector");
const tileTypeSelector = document.getElementById("tile-type-selector");

let hoverListenerKeys = [];

async function loadMaps() {
  const tms = tmsSelector.value;
  const tileType = tileTypeSelector.value;

  unByKey(hoverListenerKeys);
  hoverListenerKeys = [];

  await Promise.all(
    maps.map(async ({ map, layerName, isWgs84, valueElement }) => {
      map.getLayers().clear();
      map.setView(createView(tms, isWgs84));

      const layer = await addTileLayer(
        map,
        sessionToken,
        layerName,
        tms,
        tileType,
      );
      await addCitiesLayer(map);

      if (tileType === "coverage") {
        hoverListenerKeys.push(showValueOnHover(map, layer, valueElement));
      } else {
        valueElement.textContent = "";
      }
    }),
  );
}

await loadMaps();

for (const selector of [tmsSelector, tileTypeSelector]) {
  selector.addEventListener("change", loadMaps);
}
