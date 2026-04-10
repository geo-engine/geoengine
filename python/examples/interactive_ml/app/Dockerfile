# Setup a Debian 12 based image with a Python virtualenv
FROM debian:12-slim AS build
RUN apt-get update && \
    apt-get install --no-install-suggests --no-install-recommends --yes python3-venv gcc libpython3-dev && \
    python3 -m venv /venv && \
    /venv/bin/pip install --upgrade pip setuptools wheel

# Install Geo Engine library and its dependencies
FROM build AS build-venv
COPY pyproject.toml setup.cfg setup.py /library/
COPY geoengine /library/geoengine
WORKDIR /library
RUN /venv/bin/pip install --disable-pip-version-check .[dev,test,examples]

# Copy the virtualenv into a distroless image
# Hint: Use the `:debug` tag to get a shell in the image
FROM gcr.io/distroless/python3-debian12
COPY --from=build-venv /venv /venv

# Copy the example notebook to run
COPY examples/interactive_ml /app

WORKDIR /app

ENV GEOENGINE_INSTANCE_URL=https://zentrale.app.geoengine.io/api
ENV GEOENGINE_SESSION_TOKEN=<SESSION_TOKEN>

EXPOSE 8866

ENTRYPOINT [ \
    "/venv/bin/python3", \
    "-m", \
    "voila", \
    "--no-browser", \
    "--Voila.ip='0.0.0.0'", \
    "app/Simple Random Forest Two-Class Classifier on Sentinel-2 Images.ipynb" \
    ]
