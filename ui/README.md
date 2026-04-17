[![CI](https://github.com/geo-engine/geoengine-ui/actions/workflows/ci.yml/badge.svg?event=merge_group)](https://github.com/geo-engine/geoengine-ui/actions/workflows/ci.yml?query=event%3Amerge_group)

# Geo Engine UI

This repository contains the official UI of Geo Engine.
It is a web application that is built via Node.js.
You can serve it via any HTTP server, e.g. Apache.

## Requirements

You need to have [Node.js](https://nodejs.org) installed.
Verify that you are running at least `node v18.13.0` and `npm 8.x.x`.
You can check this by running `node -v` and `npm -v` in a terminal or console window.

### Ubuntu 22.04 LTS and higher

```
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
```

## How to build

### Production

```
rm -rf node_modules
npm ci
npm run build-prod:common && build-prod:core && npm run build-prod:gis
```

You can find the output in the `dist` directory.

### Development Live Server

You have to choose the app to use and the correct proxy configuration.

For instance, here is what you need to do to run the default app with a locally running Geo Engine backend:

For the first time:

```sh
rm -rf node_modules
npm ci
```

Then:

```sh
npm run serve:gis:local
```

When the server is started, you can visit `http://localhost:4200/`.

#### `dist` folder

If the `dist` folder contains a `common` or `core` folder, they will be used during compilation.
Thus, during development, make sure that this folder is empty or does not exist.

```sh
rm -r dist
rm -r .angular/cache
```

### Apps

- The default app is the GIS app. It is located in `projects/gis`.
- Dashboards are located under `projects/dashboards`.

## Configuration

Under `assets/config.json` can be an (optional) configuration file.
You can override any of these default settings by specifying them.

```
    API_URL: '/api',
    WMS: {
        VERSION: '1.3.0',
        FORMAT: 'image/png',
    },
    WFS: {
        VERSION: '2.0.0',
        FORMAT: 'application/json',
    },
    WCS: {
        SERVICE: 'WCS',
        VERSION: '2.0.1',
    },
    DELAYS: {
        LOADING: {
            MIN: 500,
        },
        TOOLTIP: 400,
        DEBOUNCE: 400,
        STORAGE_DEBOUNCE: 1500,
        GUEST_LOGIN_HINT: 5000,
    },
    PROJECT: 'GFBio',
    DEFAULTS: {
        PROJECT: {
            NAME: 'Default',
            TIME: {
                start: '2014-04-01T12:00:00.000Z',
                end: '2014-05-01T12:00:00.000Z'
            },
            TIMESTEP: '1 month',
            PROJECTION: 'EPSG:4326',
        },
    },
    MAP: {
        BACKGROUND_LAYER: 'eumetview',
        BACKGROUND_LAYER_URL: '',
        HOSTED_BACKGROUND_SERVICE: '/mapcache/',
        HOSTED_BACKGROUND_LAYER_NAME: 'osm',
        HOSTED_BACKGROUND_SERVICE_VERSION: '1.1.1',
        REFRESH_LAYERS_ON_CHANGE: false,
    },
    TIME: {
        ALLOW_RANGES: true,
    }
```

Note:

- For dates before the year 0, 6-digit years must be specified., e.g. '-001000-01-01T12:00:00.000Z'

_TODO: specify the options of each parameter in a tabular form._

## Code-Style and CI

We format our code with [prettier](https://prettier.io/) and lint our code with [ESLint](https://eslint.org/).
This is also checked in our CI process.

You can check your PR beforehand by calling `npm run check`.

## Testing

You need to have either `Chrome` or `Chromium` installed.

Run tests with `npm test`.

## Troubleshooting

### Header Fields/Request URI length

Problem: Requests fail with HTTP Error 431 `Request Header Fields Too Large`.

Solution: Make sure you are using a Node version >= 14 where the HTTP header size was increased, or increase it manually using the `--max-http-header-size=` argument of `node`.
