# OpenLayers OGC API Tiles Example

This directory contains an example of how to use OpenLayers to display OGC API Tiles served by Geo Engine.
The example is based on the [OpenLayers OGC API Map Tiles example](https://openlayers.org/en/latest/examples/ogc-api-map-tiles.html) and has been adapted to work with Geo Engine's OGC API Tiles implementation.

It shows two kinds of tiles, selectable in the "Tiles" dropdown:

- **Map (PNG)**: rendered map tiles from `collections/{collectionId}/map/tiles`, colorized by the layer's symbology.
- **Coverage (GeoTIFF)**: coverage tiles with the raw values from `collections/{collectionId}/coverage/tiles`.
  The example decodes them with [geotiff.js](https://geotiffjs.github.io/), colorizes them in the browser with a WebGL tile layer, and shows the value below the pointer.
  OpenLayers only parses tilesets of the data types `map` and `vector`, so the example passes the coverage tileset as `map` to OpenLayers to reuse its tile grid and URL template.

To run the example, follow these steps:

1. Start the Geo Engine server with `just backend run`.
   The example expects it at `http://localhost:3030` and proxies `/api` to it.

2. Run `npm install` in this directory to install the necessary dependencies.

3. Run `npm run start` to start the development server.
   This will open a browser window displaying the maps with the OGC API Tiles.

Alternatively, run `just backend example-ogc-tiles` from the repository root for steps 2 and 3.
