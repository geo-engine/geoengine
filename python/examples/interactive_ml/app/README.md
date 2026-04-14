# Interactive ML App

This app demonstrates a complete workflow for binary classification using Sentinel-2 satellite imagery and a Random Forest classifier. The workflow includes data acquisition, preprocessing, model training, and result visualization. Initially, the environment is set up and necessary libraries are imported. The spatial and temporal bounds for the data query are then defined through a query rectangle. A workflow is created to fetch and preprocess Sentinel-2 data, which is followed by the use of a labeling tool to create training data for water and non-water classes.

## Local setup

To run the app locally, you need to install the dependencies and start the app. You can install the dependencies with:

```bash
pip install --disable-pip-version-check -e .[dev,test,examples]

GEOENGINE_INSTANCE_URL=https://zentrale.app.geoengine.io/api \
GEOENGINE_SESSION_TOKEN=<SESSION_TOKEN> \
./examples/interactive_ml/app/app.sh
```

The app will be available at [http://localhost:8866](http://localhost:8866).

## Container setup

To run the app in a container, you need to build the container image and start the container.
You can build the container image with:

```bash
./examples/interactive_ml/app/build.sh

podman run --rm \
    -p 8866:8866 \
    -e GEOENGINE_INSTANCE_URL=https://zentrale.app.geoengine.io/api \
    -e GEOENGINE_SESSION_TOKEN=<SESSION_TOKEN> \
    geoengine-interactive-ml:latest
```

### Upload to quay.io

To upload the container image to quay.io, you need to log in and push the image.
You can do this with:

```bash
podman login quay.io
podman push geoengine-interactive-ml:latest quay.io/geoengine/geoengine-interactive-ml:latest
```
