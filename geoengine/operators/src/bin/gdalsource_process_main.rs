use gdal::raster::GdalType;
use geoengine_datatypes::raster::Pixel;
use geoengine_operators::source::gdal_worker_process::{
    GdalDatasetParameters,
    process_common::{
        GdalHeaderKind, GdalIpcBytePayload, GdalIpcMdBytePayload, GdalReadAdvise, GdalReadKind,
        GdalRequest, IpcChannelMessage, IpcChannelMessagePayload, IpcChannelResult,
        IpcProcessError, IpcProcessRasterResult,
    },
    process_impl::{GdalDatasetHolder, GdalHandling, setup_client},
    process_runtime::{Token, exit_with_error, run_helper_process},
};
use ipc_channel::ipc::IpcSender;
use num::FromPrimitive;

fn main() {
    run_helper_process("gdalsource-process", run);
}

fn handle_request(
    IpcChannelMessagePayload {
        dataset_params,
        read_id: _,
        request,
    }: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetHolder,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<(), IpcProcessError> {
    match request {
        GdalRequest::Read {
            read_advise,
            data_type,
            kind,
        } => raster_type_dispatch(
            &dataset_params,
            read_advise,
            data_type,
            kind,
            dataset_cache,
            sender,
        ),
        GdalRequest::Header(GdalHeaderKind::Raster) => send_result(
            GdalHandling::load_raster_info_with_dataset_retry(dataset_cache, &dataset_params)
                .map(IpcChannelResult::RasterInfo),
            sender,
        ),
        GdalRequest::Header(GdalHeaderKind::MdArray { group, array_name }) => send_result(
            GdalHandling::load_md_array_info_with_dataset_retry(
                dataset_cache,
                &dataset_params,
                group.as_deref(),
                &array_name,
            )
            .map(IpcChannelResult::MdArrayInfo),
            sender,
        ),
    }
}

fn raster_type_dispatch(
    dataset_params: &GdalDatasetParameters,
    read_advise: GdalReadAdvise,
    data_type: geoengine_datatypes::raster::RasterDataType,
    kind: GdalReadKind,
    dataset_cache: &mut GdalDatasetHolder,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<(), IpcProcessError> {
    use geoengine_datatypes::raster::RasterDataType as T;
    macro_rules! dispatch {
        ($t:ty) => {
            read_and_send::<$t>(dataset_params, read_advise, kind, dataset_cache, sender)
        };
    }
    match data_type {
        T::U8 => dispatch!(u8),
        T::U16 => dispatch!(u16),
        T::U32 => dispatch!(u32),
        T::U64 => dispatch!(u64),
        T::I8 => dispatch!(i8),
        T::I16 => dispatch!(i16),
        T::I32 => dispatch!(i32),
        T::I64 => dispatch!(i64),
        T::F32 => dispatch!(f32),
        T::F64 => dispatch!(f64),
    }
}

#[allow(clippy::print_stderr)]
fn run(token: Token) {
    let (sender, receiver) = match setup_client::<IpcChannelMessage, IpcProcessRasterResult>(token)
    {
        Ok(pair) => pair,
        Err(err) => exit_with_error(err),
    };

    let mut dataset_cache = GdalDatasetHolder::new();

    // Loop runs indefinitely, reusing the process and its GDAL dataset cache
    while let Ok(message) = receiver.recv() {
        let payload = message.0;

        let _span = tracing::debug_span!(
            "gdal_worker_read_tile",
            read_id = payload.read_id.as_deref().unwrap_or(""),
            dataset = %payload.dataset_params.file_path.display(),
            band = payload.dataset_params.rasterband_channel,
        )
        .entered();

        // If helper returns an error, it means the underlying IPC channel is completely broken
        if let Err(err) = handle_request(payload, &mut dataset_cache, &sender) {
            eprintln!("Fatal IPC channel error: {err:?}. Exiting worker thread.");
            break;
        }
    }
}

fn read_and_send<T: GdalType + Pixel + FromPrimitive>(
    dataset_params: &GdalDatasetParameters,
    read_advise: GdalReadAdvise,
    kind: GdalReadKind,
    dataset_cache: &mut GdalDatasetHolder,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<(), IpcProcessError> {
    let result = match kind {
        GdalReadKind::Raster => GdalHandling::load_tile_data_with_dataset_retry::<T>(
            dataset_cache,
            dataset_params,
            read_advise,
        )
        .and_then(|p| GdalIpcBytePayload::try_from(p).map_err(Into::into))
        .map(IpcChannelResult::Raster),
        GdalReadKind::MdArray {
            group,
            array_name,
            z_index,
            axis_mapping,
        } => GdalHandling::load_md_tile_data_with_dataset_retry::<T>(
            dataset_cache,
            dataset_params,
            read_advise,
            group.as_deref(),
            &array_name,
            z_index,
            &axis_mapping,
        )
        .and_then(|payload| GdalIpcMdBytePayload::try_from(payload).map_err(Into::into))
        .map(IpcChannelResult::MdSlice),
    };

    send_result(result, sender)
}

/// Sends a worker result back to the server.
fn send_result(
    result: IpcProcessRasterResult,
    sender: &IpcSender<IpcProcessRasterResult>,
) -> Result<(), IpcProcessError> {
    // Propagate channel send errors directly up out of the handler
    match result {
        Ok(result) => sender.send(Ok(result)),
        Err(err) => {
            // This is the only place the real cause survives. The parent wraps it into
            // `GdalProcessPoolError`, whose `Display` is just the variant name, so the error that
            // reaches the client says nothing about *why* the read failed - and the caller's span
            // (`gdal_worker_read_tile`) carries the read id, dataset and band this happened on.
            tracing::error!(error = ?err, "gdal worker read failed");
            sender.send(Err(err))
        }
    }?;

    Ok(())
}
