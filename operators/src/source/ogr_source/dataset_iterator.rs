// generated code of `_OgrDatasetIterator` needs this lint for the `peeked` field
#![allow(clippy::option_option)]

use super::{AttributeFilter, CsvHeader, FeaturesProvider, FormatSpecifics, OgrSourceDataset};
use crate::error::{self};
use crate::util::gdal::gdal_open_dataset_ex;
use crate::util::Result;
use gdal::vector::sql::Dialect;
use gdal::vector::{Feature, LayerAccess};
use gdal::{Dataset, DatasetOptions, GdalOpenFlags};
use geoengine_datatypes::primitives::{SpatialBounded, SpatialQuery, VectorQueryRectangle};
use log::debug;
use ouroboros::self_referencing;
use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::iter::FusedIterator;

/// An iterator over features from a OGR dataset.
/// This iterator contains the dataset and one of its layers.
pub struct OgrDatasetIterator {
    dataset_iterator: _OgrDatasetIterator,
    // must be cell since we borrow self in the iterator for emitting the output value
    // and thus cannot mutably borrow this value
    has_ended: Cell<bool>,
    use_ogr_spatial_filter: bool,
}

// We can implement `Send` for the combination of OGR dataset and layer
// as long we have a one-to-one relation. The layer mutates the dataset.
// So it is not `Send` if there is more than one layer.
unsafe impl Send for OgrDatasetIterator {}

/// Store a dataset and one of its layers.
/// Allows to iterate over features via accessing the layer only.
/// We must ensure to not access it from the outside.
#[self_referencing]
struct _OgrDatasetIterator {
    dataset: gdal::Dataset,
    #[borrows(mut dataset)]
    #[covariant]
    features_provider: FeaturesProvider<'this>,
}

impl OgrDatasetIterator {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        dataset_information: &OgrSourceDataset,
        query_rectangle: &VectorQueryRectangle,
        attribute_filters: Vec<AttributeFilter>,
    ) -> Result<OgrDatasetIterator> {
        let adjusted_filters =
            Self::adjust_filters_to_column_renaming(dataset_information, attribute_filters);

        let dataset_iterator = _OgrDatasetIteratorTryBuilder {
            dataset: Self::open_gdal_dataset(dataset_information)?,
            features_provider_builder: |dataset| {
                Self::create_features_provider(
                    dataset,
                    dataset_information,
                    query_rectangle,
                    &adjusted_filters,
                )
            },
        }
        .try_build()?;

        let use_ogr_spatial_filter = dataset_information.force_ogr_spatial_filter
            || dataset_iterator
                .borrow_features_provider()
                .has_gdal_capability(gdal::vector::LayerCaps::OLCFastSpatialFilter);

        Ok(Self {
            dataset_iterator,
            has_ended: Cell::new(false),
            use_ogr_spatial_filter,
        })
    }

    /// Undo the column renaming to let OGR apply the filters
    fn adjust_filters_to_column_renaming(
        dataset_information: &OgrSourceDataset,
        attribute_filters: Vec<AttributeFilter>,
    ) -> Vec<AttributeFilter> {
        match &dataset_information.columns {
            Some(cspec) => {
                match &cspec.rename {
                    Some(mapping) => {
                        // Build reverse mapping
                        let r_mapping = mapping
                            .iter()
                            .map(|(k, v)| (v.to_string(), k.to_string()))
                            .collect::<HashMap<_, _>>();

                        attribute_filters
                            .into_iter()
                            .map(|f| match r_mapping.get(&f.attribute) {
                                Some(name) => AttributeFilter {
                                    attribute: name.to_string(),
                                    ranges: f.ranges,
                                    keep_nulls: f.keep_nulls,
                                },
                                None => f,
                            })
                            .collect::<Vec<_>>()
                    }
                    // No renaming
                    None => attribute_filters,
                }
            }
            // No column spec
            None => attribute_filters,
        }
    }

    fn create_features_provider<'d>(
        dataset: &'d Dataset,
        dataset_information: &OgrSourceDataset,
        query_rectangle: &VectorQueryRectangle,
        attribute_filters: &[AttributeFilter],
    ) -> Result<FeaturesProvider<'d>> {
        // TODO: add OGR time filter if forced

        let mut features_provider = if let Some(sql) = dataset_information.sql_query.as_ref() {
            FeaturesProvider::ResultSet(
                dataset
                    .execute_sql(sql, None, Dialect::DEFAULT)?
                    .ok_or(error::Error::OgrSqlQuery)?,
            )
        } else {
            FeaturesProvider::Layer(dataset.layer_by_name(&dataset_information.layer_name)?)
        };

        let use_ogr_spatial_filter = dataset_information.force_ogr_spatial_filter
            || features_provider.has_gdal_capability(gdal::vector::LayerCaps::OLCFastSpatialFilter);

        if use_ogr_spatial_filter {
            debug!(
                "using spatial filter {:?} for layer {:?}",
                query_rectangle.spatial_bounds, &dataset_information.layer_name
            );
            // NOTE: the OGR-filter may be inaccurately allowing more features that should be returned in a "strict" fashion.
            features_provider.set_spatial_filter(&query_rectangle.spatial_query().spatial_bounds());
        }

        let filter_string = if dataset.driver().short_name() == "CSV" {
            FeaturesProvider::create_attribute_filter_string_cast(attribute_filters)
        } else {
            FeaturesProvider::create_attribute_filter_string(attribute_filters)
        };

        let final_filter = filter_string
            .map(|f| match &dataset_information.attribute_query {
                Some(a) => format!("({a}) AND {f}"),
                None => f,
            })
            .or_else(|| dataset_information.attribute_query.clone());

        if let Some(filter) = final_filter {
            debug!(
                "using attribute filter {:?} for layer {:?}",
                &filter, &dataset_information.layer_name
            );
            features_provider.set_attribute_filter(filter.as_str())?;
        }
        Ok(features_provider)
    }

    fn open_gdal_dataset(dataset_info: &OgrSourceDataset) -> Result<Dataset> {
        if Self::is_csv(dataset_info) {
            Self::open_csv_dataset(dataset_info)
        } else {
            gdal_open_dataset_ex(
                &dataset_info.file_name,
                DatasetOptions {
                    open_flags: GdalOpenFlags::GDAL_OF_VECTOR,
                    ..Default::default()
                },
            )
        }
    }

    fn open_csv_dataset(dataset_info: &OgrSourceDataset) -> Result<Dataset> {
        let columns = dataset_info
            .columns
            .as_ref()
            .ok_or(error::Error::OgrSourceColumnsSpecMissing)?;

        let allowed_drivers = Some(vec!["CSV"]);

        let mut dataset_options = DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_VECTOR,
            allowed_drivers: allowed_drivers.as_deref(),
            ..DatasetOptions::default()
        };

        let headers = if let Some(FormatSpecifics::Csv { header }) = &columns.format_specifics {
            header.as_gdal_param()
        } else {
            CsvHeader::Auto.as_gdal_param()
        };

        // TODO: make column x optional or allow other indication for data collection
        if columns.x.is_empty() {
            let open_opts = &[
                headers.as_str(),
                // "AUTODETECT_TYPE=YES", // This breaks tests
            ];
            dataset_options.open_options = Some(open_opts);
            return gdal_open_dataset_ex(&dataset_info.file_name, dataset_options);
        }

        if let Some(y) = &columns.y {
            let open_opts = &[
                &format!("X_POSSIBLE_NAMES={}", columns.x),
                &format!("Y_POSSIBLE_NAMES={y}"),
                headers.as_str(),
                "AUTODETECT_TYPE=YES",
            ];
            dataset_options.open_options = Some(open_opts);
            return gdal_open_dataset_ex(&dataset_info.file_name, dataset_options);
        }

        let open_opts = &[
            &format!("GEOM_POSSIBLE_NAMES={}", columns.x),
            headers.as_str(),
            "AUTODETECT_TYPE=YES",
        ];
        dataset_options.open_options = Some(open_opts);
        gdal_open_dataset_ex(&dataset_info.file_name, dataset_options)
    }

    fn is_csv(dataset_info: &OgrSourceDataset) -> bool {
        if let Some("csv" | "tsv") = dataset_info.file_name.extension().and_then(OsStr::to_str) {
            return true;
        }

        dataset_info.file_name.as_path().starts_with("CSV:")
    }

    pub fn was_spatial_filtered_by_ogr(&self) -> bool {
        self.use_ogr_spatial_filter
    }
}

#[allow(clippy::copy_iterator)]
impl<'f> Iterator for &'f mut OgrDatasetIterator {
    type Item = Feature<'f>;

    fn next(&mut self) -> Option<Self::Item> {
        // fuse
        if self.has_ended.get() {
            return None;
        }

        let features_provider = self.dataset_iterator.borrow_features_provider();

        // We somehow have to tell the reference to adhere to the lifetime `'f`
        // On the other hand, we could implement this for `&'f _` instead of `&'f mut _` and get rid of the transmute.
        // However, it makes more sense to require a mutable reference here.
        let features_provider = unsafe { std::mem::transmute::<&'_ _, &'f _>(features_provider) };

        let next = feature_iterator_next(features_provider);

        if next.is_none() {
            self.has_ended.set(true);
        }

        next
    }
}

impl<'f> FusedIterator for &'f mut OgrDatasetIterator {}

// TODO: add this to the `gdal` crate
#[inline]
fn feature_iterator_next<'f>(features_provider: &'f FeaturesProvider) -> Option<Feature<'f>> {
    let layer_ref = features_provider.layer_ref();

    let c_feature = unsafe { gdal_sys::OGR_L_GetNextFeature(layer_ref.c_layer()) };
    if c_feature.is_null() {
        None
    } else {
        Some(unsafe { Feature::from_c_feature(layer_ref.defn(), c_feature) })
    }
}
