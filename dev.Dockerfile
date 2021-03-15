FROM phusion/baseimage:focal-1.0.0-alpha1-amd64

# Update the system
RUN apt-get update && apt-get upgrade -y -o Dpkg::Options::="--force-confold"

# Use baseimage-docker's init system.
CMD ["/sbin/my_init"]

# Install dependencies
RUN install_clean \
    build-essential \
    libgdal-dev gdal-bin \
    ocl-icd-opencl-dev \
    pocl-opencl-icd \
    cmake sqlite3 libtiff-dev libclang-dev \
    && \
    curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain nightly -y

WORKDIR /app

# Copy source files
COPY /datatypes /app/datatypes
COPY /operators /app/operators
COPY /services /app/services
COPY Settings-* /app/
COPY Cargo.toml /app/

# Build application and copy binary to `/usr/bin/geoengine`
RUN RUSTFLAGS='-C target-cpu=native' $HOME/.cargo/bin/cargo build --release \
    && \
    mv target/release/main /usr/bin/geoengine \
    && \
    mkdir /etc/service/geoengine

# Setup service
COPY docker/dev/Settings-dev.toml /app/Settings.toml
COPY docker/dev/service.sh /etc/service/geoengine/run
RUN chmod +x /etc/service/geoengine/run \
    && \
    adduser --disabled-password --gecos "" geoengine

EXPOSE 8080

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
