# OpenLayers OGC API Map Tiles Example

This directory contains an example of how to use OpenLayers to display OGC API Map Tiles served by Geo Engine.
The example is based on the [OpenLayers OGC API Map Tiles example](https://openlayers.org/en/latest/examples/ogc-api-map-tiles.html) and has been adapted to work with Geo Engine's OGC API Map Tiles implementation.

To run the example, follow these steps:

1. Start the Geo Engine server with the appropriate configuration to serve OGC API Map Tiles. Make sure to note the URL where the tiles are being served (e.g., `http://localhost:3030/ogc/tiles`).

2. Run `npm install` in this directory to install the necessary dependencies.

3. Run `npm run start` to start the development server.
   This will open a browser window displaying the map with the OGC API Map Tiles.
