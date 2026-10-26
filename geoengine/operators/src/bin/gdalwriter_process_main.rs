use geoengine_operators::source::gdal_worker_process::{
    process_impl::setup_client,
    process_runtime::{exit_with_error, run_helper_process},
};
use geoengine_operators::util::gdal_writer_process::{
    WriterRequest, WriterResponse, writer::run_writer,
};

fn main() {
    run_helper_process("gdalwriter-process", |token| {
        match setup_client::<WriterRequest, WriterResponse>(token) {
            Ok((sender, receiver)) => run_writer(&receiver, &sender),
            Err(err) => exit_with_error(err),
        }
    });
}
