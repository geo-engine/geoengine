pub fn hide_gdal_errors() {
    gdal::config::set_error_handler(|_, _, _| {});
}
