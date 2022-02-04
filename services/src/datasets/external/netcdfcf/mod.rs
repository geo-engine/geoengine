use std::collections::VecDeque;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::datasets::listing::DatasetListOptions;
use crate::datasets::listing::{ExternalDatasetProvider, ProvenanceOutput};
use crate::error::Error;
use crate::{
    datasets::{listing::DatasetListing, storage::ExternalDatasetProviderDefinition},
    util::user_input::Validated,
};
use async_trait::async_trait;
use chrono::NaiveDate;
use gdal::cpl::CslStringList;
use gdal::errors::GdalError;
use gdal::{Dataset, DatasetOptions, GdalOpenFlags};
use gdal_sys::{
    CPLErr, CSLDestroy, GDALAttributeReadAsString, GDALAttributeRelease, GDALDataType,
    GDALDatasetGetRootGroup, GDALDimensionGetSize, GDALExtendedDataTypeGetNumericDataType,
    GDALExtendedDataTypeRelease, GDALGroupGetAttribute, GDALGroupGetGroupNames, GDALGroupHS,
    GDALGroupOpenGroup, GDALGroupOpenMDArray, GDALGroupRelease, GDALMDArrayGetAttribute,
    GDALMDArrayGetDataType, GDALMDArrayGetDimensions, GDALMDArrayGetNoDataValueAsDouble,
    GDALMDArrayGetSpatialRef, GDALMDArrayH, GDALMDArrayRelease, GDALReleaseDimensions,
    OSRDestroySpatialReference, VSIFree,
};
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::{
    Measurement, RasterQueryRectangle, TimeGranularity, TimeInstance, TimeInterval, TimeStep,
    VectorQueryRectangle,
};
use geoengine_datatypes::raster::{GdalGeoTransform, RasterDataType};
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
use geoengine_operators::engine::TypedResultDescriptor;
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetadataNetCdfCf,
};
use geoengine_operators::util::gdal::gdal_open_dataset_ex;
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

pub use self::error::NetCdfCf4DProviderError;

mod error;

type Result<T, E = NetCdfCf4DProviderError> = std::result::Result<T, E>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetCdfCfDataProviderDefinition {
    pub id: DatasetProviderId,
    pub name: String,
    pub path: PathBuf,
}

#[typetag::serde]
#[async_trait]
impl ExternalDatasetProviderDefinition for NetCdfCfDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn ExternalDatasetProvider>> {
        Ok(Box::new(NetCdfCfDataProvider {
            id: self.id,
            path: self.path,
        }))
    }

    fn type_name(&self) -> String {
        "NetCdfCfProviderDefinition".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DatasetProviderId {
        self.id
    }
}

pub struct NetCdfCfDataProvider {
    id: DatasetProviderId,
    path: PathBuf,
}

/// TODO: This should be part of the GDAL crate
#[derive(Debug)]
struct MdGroup<'d> {
    c_group: *mut GDALGroupHS,
    _dataset: &'d Dataset,
    name: String,
}

impl Drop for MdGroup<'_> {
    fn drop(&mut self) {
        unsafe {
            GDALGroupRelease(self.c_group);
        }
    }
}

/// TODO: This should be part of the GDAL crate
#[derive(Debug)]
struct MdArray<'g> {
    c_mdarray: GDALMDArrayH,
    _group: &'g MdGroup<'g>,
}

impl Drop for MdArray<'_> {
    fn drop(&mut self) {
        unsafe {
            GDALMDArrayRelease(self.c_mdarray);
        }
    }
}

/// Always check new implementations with `RUSTFLAGS="-Z sanitizer=leak"`
impl<'d> MdGroup<'d> {
    unsafe fn _string(raw_ptr: *const c_char) -> String {
        let c_str = CStr::from_ptr(raw_ptr);
        c_str.to_string_lossy().into_owned()
    }

    unsafe fn _string_array(raw_ptr: *mut *mut c_char) -> Vec<String> {
        let mut ret_val: Vec<String> = vec![];
        let mut i = 0;
        loop {
            let ptr = raw_ptr.add(i);
            if ptr.is_null() {
                break;
            }
            let next = ptr.read();
            if next.is_null() {
                break;
            }
            let value = Self::_string(next);
            i += 1;
            ret_val.push(value);
        }
        ret_val
    }

    unsafe fn _last_null_pointer_err(method_name: &'static str) -> GdalError {
        let last_err_msg = Self::_string(gdal_sys::CPLGetLastErrorMsg());
        gdal_sys::CPLErrorReset();
        GdalError::NullPointer {
            method_name,
            msg: last_err_msg,
        }
    }

    unsafe fn _last_cpl_err(cpl_err_class: CPLErr::Type) -> GdalError {
        let last_err_no = gdal_sys::CPLGetLastErrorNo();
        let last_err_msg = Self::_string(gdal_sys::CPLGetLastErrorMsg());
        gdal_sys::CPLErrorReset();
        GdalError::CplError {
            class: cpl_err_class,
            number: last_err_no,
            msg: last_err_msg,
        }
    }

    fn from_dataset(dataset: &'d Dataset) -> Result<Self> {
        let c_group = unsafe {
            let c_group = GDALDatasetGetRootGroup(dataset.c_dataset());

            if c_group.is_null() {
                return Err(NetCdfCf4DProviderError::GdalMd {
                    source: Self::_last_null_pointer_err("GDALGetRasterBand"),
                });
            }

            c_group
        };

        Ok(Self {
            c_group,
            _dataset: dataset,
            name: "".to_owned(),
        })
    }

    /// Don't put a NULL-byte in the name!!!
    fn attribute_as_string(&self, name: &str) -> Result<String, GdalError> {
        let name = CString::new(name).expect("no null-byte in name");

        let value = unsafe {
            let c_attribute = GDALGroupGetAttribute(self.c_group, name.as_ptr());

            if c_attribute.is_null() {
                return Err(Self::_last_null_pointer_err("GDALGroupGetAttribute"));
            }

            let value = Self::_string(GDALAttributeReadAsString(c_attribute));

            GDALAttributeRelease(c_attribute);

            value
        };

        Ok(value)
    }

    /// Don't put a NULL-byte in the name!!!
    fn dimension_as_string_array(&self, name: &str) -> Result<Vec<String>, GdalError> {
        let name = CString::new(name).expect("no null-byte in name");
        let options = CslStringList::new();

        let value = unsafe {
            let c_mdarray = GDALGroupOpenMDArray(self.c_group, name.as_ptr(), options.as_ptr());

            if c_mdarray.is_null() {
                return Err(Self::_last_null_pointer_err("GDALGroupOpenMDArray"));
            }

            let mut dim_count = 0;

            let c_dimensions =
                GDALMDArrayGetDimensions(c_mdarray, std::ptr::addr_of_mut!(dim_count));
            let dimensions = std::slice::from_raw_parts_mut(c_dimensions, dim_count);

            let mut count = Vec::<usize>::with_capacity(dim_count);
            for dim in dimensions {
                let dim_size = GDALDimensionGetSize(*dim);
                count.push(dim_size as usize);
            }

            if count.len() != 1 {
                return Err(GdalError::BadArgument(format!(
                    "Dimension must be 1D, but is {}D",
                    count.len()
                )));
            }

            let mut string_pointers: Vec<*const c_char> = vec![std::ptr::null(); count[0] as usize];
            let array_start_index: Vec<u64> = vec![0; dim_count];

            let array_step: *const i64 = std::ptr::null(); // default value
            let buffer_stride: *const i64 = std::ptr::null(); // default value
            let data_type = GDALMDArrayGetDataType(c_mdarray);
            let p_dst_buffer_alloc_start: *mut c_void = std::ptr::null_mut();
            let n_dst_buffer_alloc_size = 0;

            let rv = gdal_sys::GDALMDArrayRead(
                c_mdarray,
                array_start_index.as_ptr(),
                count.as_ptr(),
                array_step,
                buffer_stride,
                data_type,
                string_pointers.as_mut_ptr().cast::<std::ffi::c_void>(),
                p_dst_buffer_alloc_start,
                n_dst_buffer_alloc_size,
            );

            if rv == 0 {
                return Err(GdalError::BadArgument("GDALMDArrayRead failed".to_string()));
            }

            let strings = string_pointers
                .into_iter()
                .map(|string_ptr| {
                    let string = Self::_string(string_ptr);

                    VSIFree(string_ptr as *mut c_void);

                    string
                })
                .collect();

            GDALExtendedDataTypeRelease(data_type);

            GDALMDArrayRelease(c_mdarray);

            GDALReleaseDimensions(c_dimensions, dim_count);

            strings
        };

        Ok(value)
    }

    fn group_names(&self) -> Vec<String> {
        let options = CslStringList::new();

        unsafe {
            let c_group_names = GDALGroupGetGroupNames(self.c_group, options.as_ptr());

            let strings = Self::_string_array(c_group_names);

            CSLDestroy(c_group_names);

            strings
        }
    }

    fn open_group(&self, name: &str) -> Result<Self> {
        let c_name = CString::new(name).expect("no null-byte in name");
        let options = CslStringList::new();

        let c_group = unsafe {
            let c_group = GDALGroupOpenGroup(self.c_group, c_name.as_ptr(), options.as_ptr());

            if c_group.is_null() {
                return Err(NetCdfCf4DProviderError::GdalMd {
                    source: Self::_last_null_pointer_err("GDALGroupOpenGroup"),
                });
            }

            c_group
        };

        #[allow(clippy::used_underscore_binding)]
        Ok(Self {
            c_group,
            _dataset: self._dataset,
            name: name.to_string(),
        })
    }

    fn datatype_of_numeric_array(&self, name: &str) -> Result<RasterDataType> {
        let name = CString::new(name).expect("no null-byte in name");

        let gdal_data_type = unsafe {
            let options = CslStringList::new();
            let c_mdarray = GDALGroupOpenMDArray(self.c_group, name.as_ptr(), options.as_ptr());

            if c_mdarray.is_null() {
                return Err(NetCdfCf4DProviderError::GdalMd {
                    source: Self::_last_null_pointer_err("GDALGroupOpenMDArray"),
                });
            }

            let c_data_type = GDALMDArrayGetDataType(c_mdarray);

            let data_type = GDALExtendedDataTypeGetNumericDataType(c_data_type);

            GDALMDArrayRelease(c_mdarray);

            GDALExtendedDataTypeRelease(c_data_type);

            data_type
        };

        Ok(match gdal_data_type {
            GDALDataType::GDT_Byte => RasterDataType::U8,
            GDALDataType::GDT_UInt16 => RasterDataType::U16,
            GDALDataType::GDT_Int16 => RasterDataType::I16,
            GDALDataType::GDT_UInt32 => RasterDataType::U32,
            GDALDataType::GDT_Int32 => RasterDataType::I32,
            GDALDataType::GDT_Float32 => RasterDataType::F32,
            GDALDataType::GDT_Float64 => RasterDataType::F64,
            _ => {
                return Err(NetCdfCf4DProviderError::UnknownGdalDatatype {
                    type_number: gdal_data_type,
                })
            }
        })
    }

    fn open_array(&self, name: &str) -> Result<MdArray> {
        let name = CString::new(name).expect("no null-byte in name");
        let options = CslStringList::new();

        Ok(unsafe {
            let c_mdarray = GDALGroupOpenMDArray(self.c_group, name.as_ptr(), options.as_ptr());

            if c_mdarray.is_null() {
                return Err(NetCdfCf4DProviderError::GdalMd {
                    source: Self::_last_null_pointer_err("GDALGroupOpenMDArray"),
                });
            }

            MdArray {
                c_mdarray,
                _group: self,
            }
        })
    }
}
/// Always check new implementations with `RUSTFLAGS="-Z sanitizer=leak"`
impl<'g> MdArray<'g> {
    fn spatial_reference(&self) -> Result<SpatialReferenceOption> {
        let gdal_spatial_ref = unsafe {
            let c_gdal_spatial_ref = GDALMDArrayGetSpatialRef(self.c_mdarray);

            let gdal_spatial_ref = gdal::spatial_ref::SpatialRef::from_c_obj(c_gdal_spatial_ref);

            OSRDestroySpatialReference(c_gdal_spatial_ref);

            gdal_spatial_ref
        }
        .context(error::GdalMd)?;

        SpatialReference::try_from(gdal_spatial_ref)
            .map(Into::into)
            .context(error::CannotParseCrs)
    }

    fn data_type(&self) -> Result<RasterDataType> {
        let gdal_data_type = unsafe {
            let c_data_type = GDALMDArrayGetDataType(self.c_mdarray);

            let data_type = GDALExtendedDataTypeGetNumericDataType(c_data_type);

            GDALExtendedDataTypeRelease(c_data_type);

            data_type
        };

        Ok(match gdal_data_type {
            GDALDataType::GDT_Byte => RasterDataType::U8,
            GDALDataType::GDT_UInt16 => RasterDataType::U16,
            GDALDataType::GDT_Int16 => RasterDataType::I16,
            GDALDataType::GDT_UInt32 => RasterDataType::U32,
            GDALDataType::GDT_Int32 => RasterDataType::I32,
            GDALDataType::GDT_Float32 => RasterDataType::F32,
            GDALDataType::GDT_Float64 => RasterDataType::F64,
            _ => {
                return Err(NetCdfCf4DProviderError::UnknownGdalDatatype {
                    type_number: gdal_data_type,
                })
            }
        })
    }

    fn no_data_value(&self) -> Option<f64> {
        let mut has_nodata = 0;

        let no_data_value = unsafe {
            GDALMDArrayGetNoDataValueAsDouble(self.c_mdarray, std::ptr::addr_of_mut!(has_nodata))
        };

        if has_nodata == 0 {
            None
        } else {
            Some(no_data_value)
        }
    }

    fn dimensions(&self) -> Result<DimensionSizes> {
        let mut number_of_dimensions = 0;

        Ok(unsafe {
            let c_dimensions = GDALMDArrayGetDimensions(
                self.c_mdarray,
                std::ptr::addr_of_mut!(number_of_dimensions),
            );

            if number_of_dimensions != 4 {
                return Err(NetCdfCf4DProviderError::MustBe4DDataset {
                    number_of_dimensions,
                });
            }

            let dimensions = std::slice::from_raw_parts_mut(c_dimensions, number_of_dimensions);

            let sizes = DimensionSizes {
                _entity: GDALDimensionGetSize(dimensions[0]) as usize,
                time: GDALDimensionGetSize(dimensions[1]) as usize,
                lat: GDALDimensionGetSize(dimensions[2]) as usize,
                lon: GDALDimensionGetSize(dimensions[3]) as usize,
            };

            GDALReleaseDimensions(c_dimensions, number_of_dimensions);

            sizes
        })
    }

    /// Don't put a NULL-byte in the name!!!
    fn attribute_as_string(&self, name: &str) -> Result<String, GdalError> {
        let name = CString::new(name).expect("no null-byte in name");

        let value = unsafe {
            let c_attribute = GDALMDArrayGetAttribute(self.c_mdarray, name.as_ptr());

            if c_attribute.is_null() {
                return Err(MdGroup::_last_null_pointer_err("GDALGroupGetAttribute"));
            }

            let value = MdGroup::_string(GDALAttributeReadAsString(c_attribute));

            GDALAttributeRelease(c_attribute);

            value
        };

        Ok(value)
    }
}

#[derive(Debug, Clone, Copy)]
struct DimensionSizes {
    pub _entity: usize,
    pub time: usize,
    pub lat: usize,
    pub lon: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NetCdfOverview {
    pub file_name: String,
    pub title: String,
    pub spatial_reference: SpatialReferenceOption,
    pub subgroups: Vec<NetCdfSubgroup>,
    pub entities: Vec<NetCdfArrayDataset>,
    pub time: TimeInterval,
    pub time_step: TimeStep,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NetCdfSubgroup {
    pub name: String,
    pub title: String,
    pub description: String,
    // TODO: would actually be nice if it were inside dataset/entity
    pub data_type: Option<RasterDataType>,
    pub subgroups: Vec<NetCdfSubgroup>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NetCdfArrayDataset {
    pub id: usize,
    pub name: String,
}

trait ToNetCdfSubgroup {
    fn to_net_cdf_subgroup(&self) -> Result<NetCdfSubgroup>;
}

impl<'a> ToNetCdfSubgroup for MdGroup<'a> {
    fn to_net_cdf_subgroup(&self) -> Result<NetCdfSubgroup> {
        let name = self.name.clone();
        let title = self
            .attribute_as_string("standard_name")
            .unwrap_or_default();
        // TODO: how to get that?
        let description = "".to_string();

        let subgroup_names = self.group_names();

        if subgroup_names.is_empty() {
            let data_type = Some(self.datatype_of_numeric_array("ebv_cube")?);

            return Ok(NetCdfSubgroup {
                name,
                title,
                description,
                data_type,
                subgroups: Vec::new(),
            });
        }

        let mut subgroups = Vec::with_capacity(subgroup_names.len());

        for subgroup in subgroup_names {
            subgroups.push(self.open_group(&subgroup)?.to_net_cdf_subgroup()?);
        }

        Ok(NetCdfSubgroup {
            name,
            title,
            description,
            data_type: None,
            subgroups,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NetCdfCf4DDatasetId {
    pub file_name: String,
    pub group_names: Vec<String>,
    pub entity: usize,
}

impl NetCdfCfDataProvider {
    pub(crate) fn build_netcdf_tree(path: &Path) -> Result<NetCdfOverview> {
        let ds = gdal_open_dataset_ex(
            path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
                allowed_drivers: Some(&["netCDF"]),
                open_options: None,
                sibling_files: None,
            },
        )
        .context(error::InvalidDatasetIdFile)?;

        let root_group = MdGroup::from_dataset(&ds)?;

        let file_name = path
            .file_name()
            .ok_or(NetCdfCf4DProviderError::MissingFileName)?
            .to_string_lossy();

        let title = root_group
            .attribute_as_string("title")
            .context(error::MissingTitle)?;

        let spatial_reference = root_group
            .attribute_as_string("geospatial_bounds_crs")
            .context(error::MissingCrs)?;
        let spatial_reference: SpatialReferenceOption =
            SpatialReference::from_str(&spatial_reference)
                .context(error::CannotParseCrs)?
                .into();

        let entities = root_group
            .dimension_as_string_array("entities")
            .context(error::MissingEntities)?
            .into_iter()
            .enumerate()
            .map(|(id, name)| NetCdfArrayDataset { id, name })
            .collect::<Vec<_>>();

        let subgroups = root_group
            .group_names()
            .iter()
            .map(|name| root_group.open_group(name)?.to_net_cdf_subgroup())
            .collect::<Result<Vec<_>>>()?;

        let start = root_group
            .attribute_as_string("time_coverage_start")
            .context(error::MissingTimeCoverageStart)?;
        let end = root_group
            .attribute_as_string("time_coverage_end")
            .context(error::MissingTimeCoverageEnd)?;
        let step = root_group
            .attribute_as_string("time_coverage_resolution")
            .context(error::MissingTimeCoverageResolution)?;

        let (time_start, time_end, time_step) = parse_time_coverage(&start, &end, &step)?;

        Ok(NetCdfOverview {
            file_name: file_name.to_string(),
            title,
            spatial_reference,
            subgroups,
            entities,
            time: TimeInterval::new(time_start, time_end)
                .unwrap_or_else(|_| TimeInterval::from(time_start)),
            time_step,
        })
    }

    pub(crate) fn listing_from_netcdf(
        id: DatasetProviderId,
        path: &Path,
    ) -> Result<Vec<DatasetListing>> {
        let tree = Self::build_netcdf_tree(path)?;

        let mut paths: VecDeque<Vec<&NetCdfSubgroup>> =
            tree.subgroups.iter().map(|s| vec![s]).collect();

        let mut listings = Vec::new();

        while let Some(path) = paths.pop_front() {
            let tail = path.last().expect("our lists aren't empty");

            if !tail.subgroups.is_empty() {
                for subgroup in &tail.subgroups {
                    let mut updated_path = path.clone();
                    updated_path.push(subgroup);
                    paths.push_back(updated_path);
                }

                continue;
            }

            // emit datasets

            let group_title_path = path
                .iter()
                .map(|s| s.title.as_str())
                .collect::<Vec<&str>>()
                .join(" > ");

            let group_names = path.iter().map(|s| s.name.clone()).collect::<Vec<String>>();

            let data_type = tail.data_type.unwrap_or(RasterDataType::F32);

            for entity in &tree.entities {
                let dataset_id = NetCdfCf4DDatasetId {
                    file_name: tree.file_name.clone(),
                    group_names: group_names.clone(),
                    entity: entity.id,
                };

                listings.push(DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: id,
                        dataset_id: serde_json::to_string(&dataset_id).unwrap_or_default(),
                    }),
                    name: format!(
                        "{title}: {group_title_path} > {entity_name}",
                        title = tree.title,
                        entity_name = entity.name
                    ),
                    description: "".to_owned(), // TODO: where to get from file?
                    tags: vec![],               // TODO: where to get from file?
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type,
                        spatial_reference: tree.spatial_reference,
                        measurement: Measurement::Unitless, // TODO: where to get from file?
                        no_data_value: None, // we don't want to open the dataset at this point. We should get rid of the result descriptor in the listing in general
                    }),
                    symbology: None,
                });
            }
        }

        Ok(listings)
    }

    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let dataset = dataset
            .external()
            .ok_or(NetCdfCf4DProviderError::InvalidExternalDatasetId { provider: self.id })?;

        let dataset_id: NetCdfCf4DDatasetId =
            serde_json::from_str(&dataset.dataset_id).context(error::CannotParseDatasetId)?;

        let path = self.path.join(dataset_id.file_name);

        let gdal_path = format!(
            "NETCDF:{path}:/{group}/ebv_cube",
            path = path.to_string_lossy(),
            group = dataset_id.group_names.join("/")
        );

        let dataset = gdal_open_dataset_ex(
            &path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
                allowed_drivers: Some(&["netCDF"]),
                open_options: None,
                sibling_files: None,
            },
        )
        .context(error::InvalidDatasetIdFile)?;

        let root_group = MdGroup::from_dataset(&dataset)?;

        let start = root_group
            .attribute_as_string("time_coverage_start")
            .context(error::MissingTimeCoverageStart)?;
        let end = root_group
            .attribute_as_string("time_coverage_end")
            .context(error::MissingTimeCoverageEnd)?;
        let step = root_group
            .attribute_as_string("time_coverage_resolution")
            .context(error::MissingTimeCoverageResolution)?;

        let (start, end, step) = parse_time_coverage(&start, &end, &step)?;

        let geo_transform = {
            let crs_array = root_group.open_array("crs")?;
            let geo_transform = crs_array
                .attribute_as_string("GeoTransform")
                .context(error::CannotGetGeoTransform)?;
            parse_geo_transform(&geo_transform)?
        };

        // traverse groups
        let mut group_stack = vec![root_group];

        // let mut group = root_group;
        for group_name in &dataset_id.group_names {
            group_stack.push(
                group_stack
                    .last()
                    .expect("at least root group in here")
                    .open_group(group_name)?,
            );
            // group = group.open_group(group_name)?;
        }

        let data_array = group_stack
            .last()
            .expect("at least root group in here")
            .open_array("ebv_cube")?;

        let dimensions = data_array.dimensions()?;

        let result_descriptor = RasterResultDescriptor {
            data_type: data_array.data_type()?,
            spatial_reference: data_array.spatial_reference()?,
            measurement: Measurement::Unitless, // TODO: where to get from file?
            no_data_value: data_array.no_data_value(),
        };

        let params = GdalDatasetParameters {
            file_path: gdal_path.into(),
            rasterband_channel: 0, // we calculate offsets in our source
            geo_transform,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: result_descriptor.no_data_value,
            properties_mapping: None,
            width: dimensions.lon,
            height: dimensions.lat,
            gdal_open_options: None,
            gdal_config_options: None,
        };

        Ok(Box::new(GdalMetadataNetCdfCf {
            params,
            result_descriptor,
            start,
            end, // TODO: Use this or time dimension size (number of steps)?
            step,
            band_offset: dataset_id.entity as usize * dimensions.time,
        }))
    }
}

fn parse_geo_transform(input: &str) -> Result<GdalDatasetGeoTransform> {
    let numbers: Vec<f64> = input
        .split_whitespace()
        .map(|s| s.parse().context(error::InvalidGeoTransformNumbers))
        .collect::<Result<Vec<_>>>()?;

    if numbers.len() != 6 {
        return Err(NetCdfCf4DProviderError::InvalidGeoTransformLength {
            length: numbers.len(),
        });
    }

    let gdal_geo_transform: GdalGeoTransform = [
        numbers[0], numbers[1], numbers[2], numbers[3], numbers[4], numbers[5],
    ];

    Ok(gdal_geo_transform.into())
}

fn parse_time_coverage(
    start: &str,
    end: &str,
    resolution: &str,
) -> Result<(TimeInstance, TimeInstance, TimeStep)> {
    let start = start
        .parse::<i32>()
        .context(error::CannotConvertTimeCoverageToInt)?;
    let end = end
        .parse::<i32>()
        .context(error::CannotConvertTimeCoverageToInt)?;

    Ok(match resolution {
        "Yearly" | "every 1 year" => {
            let start = TimeInstance::from(NaiveDate::from_ymd(start, 1, 1).and_hms(0, 0, 0));
            // end + 1 because it is exclusive for us but inclusive in the metadata
            let end = TimeInstance::from(NaiveDate::from_ymd(end + 1, 1, 1).and_hms(0, 0, 0));
            let step = TimeStep {
                granularity: TimeGranularity::Years,
                step: 1,
            };
            (start, end, step)
        }
        "decade" => {
            let start = TimeInstance::from(NaiveDate::from_ymd(start, 1, 1).and_hms(0, 0, 0));
            // end + 1 because it is exclusive for us but inclusive in the metadata
            let end = TimeInstance::from(NaiveDate::from_ymd(end + 1, 1, 1).and_hms(0, 0, 0));
            let step = TimeStep {
                granularity: TimeGranularity::Years,
                step: 10,
            };
            (start, end, step)
        }
        _ => return Err(NetCdfCf4DProviderError::NotYetImplemented), // TODO: fix format and parse other options
    })
}

#[async_trait]
impl ExternalDatasetProvider for NetCdfCfDataProvider {
    async fn list(
        &self,
        options: Validated<DatasetListOptions>,
    ) -> crate::error::Result<Vec<DatasetListing>> {
        // TODO: user right management
        // TODO: options

        let mut dir = tokio::fs::read_dir(&self.path).await?;

        let mut datasets = vec![];
        while let Some(entry) = dir.next_entry().await? {
            if !entry.path().is_file() {
                continue;
            }

            let id = self.id;
            let listing =
                tokio::task::spawn_blocking(move || Self::listing_from_netcdf(id, &entry.path()))
                    .await?;

            match listing {
                Ok(listing) => datasets.extend(listing),
                Err(e) => debug!("Failed to list dataset: {}", e),
            }
        }

        // TODO: react to filter and sort options
        // TODO: don't compute everything and filter then
        let datasets = datasets
            .into_iter()
            .skip(options.user_input.offset as usize)
            .take(options.user_input.limit as usize)
            .collect();

        Ok(datasets)
    }

    async fn provenance(&self, dataset: &DatasetId) -> crate::error::Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: None,
        })
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for NetCdfCfDataProvider
{
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        // TODO spawn blocking
        self.meta_data(dataset)
            .await
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(Error::InvalidExternalDatasetId { provider: self.id }),
            })
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for NetCdfCfDataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for NetCdfCfDataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{SpatialPartition2D, SpatialResolution, TimeInterval},
        spatial_reference::SpatialReferenceAuthority,
        test_data,
    };
    use geoengine_operators::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfoPart,
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_listing_from_netcdf_m() {
        let provider_id =
            DatasetProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap();

        let listing = NetCdfCfDataProvider::listing_from_netcdf(
            provider_id,
            test_data!("netcdf4d/dataset_m.nc"),
        )
        .unwrap();

        assert_eq!(listing.len(), 6);

        let result_descriptor: TypedResultDescriptor = RasterResultDescriptor {
            data_type: RasterDataType::I16,
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 4326).into(),
            measurement: Measurement::Unitless,
            no_data_value: None,
        }
        .into();

        assert_eq!(
            listing[0],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id:
                        "{\"fileName\":\"dataset_m.nc\",\"groupNames\":[\"metric_1\"],\"entity\":0}"
                            .into(),
                }),
                name: "Test dataset metric: Random metric 1 > entity01".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[1],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id:
                        "{\"fileName\":\"dataset_m.nc\",\"groupNames\":[\"metric_1\"],\"entity\":1}"
                            .into(),
                }),
                name: "Test dataset metric: Random metric 1 > entity02".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[2],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id:
                        "{\"fileName\":\"dataset_m.nc\",\"groupNames\":[\"metric_1\"],\"entity\":2}"
                            .into(),
                }),
                name: "Test dataset metric: Random metric 1 > entity03".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[3],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id:
                        "{\"fileName\":\"dataset_m.nc\",\"groupNames\":[\"metric_2\"],\"entity\":0}"
                            .into(),
                }),
                name: "Test dataset metric: Random metric 2 > entity01".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[4],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id:
                        "{\"fileName\":\"dataset_m.nc\",\"groupNames\":[\"metric_2\"],\"entity\":1}"
                            .into(),
                }),
                name: "Test dataset metric: Random metric 2 > entity02".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[5],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id:
                        "{\"fileName\":\"dataset_m.nc\",\"groupNames\":[\"metric_2\"],\"entity\":2}"
                            .into(),
                }),
                name: "Test dataset metric: Random metric 2 > entity03".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor,
                symbology: None
            }
        );
    }

    #[tokio::test]
    async fn test_listing_from_netcdf_sm() {
        let provider_id =
            DatasetProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap();

        let listing = NetCdfCfDataProvider::listing_from_netcdf(
            provider_id,
            test_data!("netcdf4d/dataset_sm.nc"),
        )
        .unwrap();

        assert_eq!(listing.len(), 20);

        let result_descriptor: TypedResultDescriptor = RasterResultDescriptor {
            data_type: RasterDataType::I16,
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3035).into(),
            measurement: Measurement::Unitless,
            no_data_value: None,
        }
        .into();

        assert_eq!(
            listing[0],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "{\"fileName\":\"dataset_sm.nc\",\"groupNames\":[\"scenario_1\",\"metric_1\"],\"entity\":0}".into(),
                }),
                name:
                    "Test dataset metric and scenario: Sustainability > Random metric 1 > entity01"
                        .into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor: result_descriptor.clone(),
                symbology: None
            }
        );
        assert_eq!(
            listing[19],
            DatasetListing {
                id: DatasetId::External(ExternalDatasetId {
                    provider_id,
                    dataset_id: "{\"fileName\":\"dataset_sm.nc\",\"groupNames\":[\"scenario_5\",\"metric_2\"],\"entity\":1}".into(),
                }),
                name: "Test dataset metric and scenario: Fossil-fueled Development > Random metric 2 > entity02".into(),
                description: "".into(),
                tags: vec![],
                source_operator: "GdalSource".into(),
                result_descriptor,
                symbology: None
            }
        );
    }

    // TODO: verify
    // TODO: do the samme for `dataset_m.nc`
    #[tokio::test]
    async fn test_metadata_from_netcdf_sm() {
        let provider = NetCdfCfDataProvider {
            id: DatasetProviderId::from_str("bf6bb6ea-5d5d-467d-bad1-267bf3a54470").unwrap(),
            path: test_data!("netcdf4d/").to_path_buf(),
        };

        let metadata = provider
            .meta_data(&DatasetId::External(ExternalDatasetId {
                provider_id: provider.id,
                dataset_id: "{\"fileName\":\"dataset_sm.nc\",\"groupNames\":[\"scenario_5\",\"metric_2\"],\"entity\":1}".into(),
            }))
            .await
            .unwrap();

        assert_eq!(
            metadata.result_descriptor().await.unwrap(),
            RasterResultDescriptor {
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3035)
                    .into(),
                measurement: Measurement::Unitless,
                no_data_value: Some(-9999.),
            }
        );

        let loading_info = metadata
            .loading_info(RasterQueryRectangle {
                spatial_bounds: SpatialPartition2D::new(
                    (43.945_312_5, 0.791_015_625_25).into(),
                    (44.033_203_125, 0.703_125_25).into(),
                )
                .unwrap(),
                time_interval: TimeInstance::from(NaiveDate::from_ymd(2001, 4, 1).and_hms(0, 0, 0))
                    .into(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    0.000_343_322_7, // 256 pixel
                    0.000_343_322_7, // 256 pixel
                ),
            })
            .await
            .unwrap();

        let mut loading_info_parts = Vec::<GdalLoadingInfoPart>::new();
        for part in loading_info.info {
            loading_info_parts.push(part.unwrap());
        }

        assert_eq!(loading_info_parts.len(), 1);

        let file_path = format!(
            "NETCDF:{absolute_file_path}:/scenario_5/metric_2/ebv_cube",
            absolute_file_path = test_data!("netcdf4d/dataset_sm.nc")
                .canonicalize()
                .unwrap()
                .to_string_lossy()
        )
        .into();

        assert_eq!(
            loading_info_parts[0],
            GdalLoadingInfoPart {
                time: TimeInterval::new_unchecked(946_684_800_000, 1_262_304_000_000),
                params: Some(GdalDatasetParameters {
                    file_path,
                    rasterband_channel: 4,
                    geo_transform: GdalDatasetGeoTransform {
                        origin_coordinate: (3_580_000.0, 2_370_000.0).into(),
                        x_pixel_size: 1000.0,
                        y_pixel_size: -1000.0,
                    },
                    width: 10,
                    height: 10,
                    file_not_found_handling: FileNotFoundHandling::Error,
                    no_data_value: Some(-9999.),
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None
                })
            }
        );
    }
}
