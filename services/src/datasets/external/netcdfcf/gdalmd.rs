//! TODO: This should be part of the GDAL crate

use gdal::cpl::CslStringList;
use gdal::errors::GdalError;
use gdal::Dataset;
use gdal_sys::{
    CPLErr, CSLDestroy, GDALAttributeReadAsString, GDALAttributeRelease, GDALDataType,
    GDALDatasetGetRootGroup, GDALDimensionGetName, GDALDimensionGetSize,
    GDALExtendedDataTypeGetNumericDataType, GDALExtendedDataTypeRelease, GDALGroupGetAttribute,
    GDALGroupGetDimensions, GDALGroupGetGroupNames, GDALGroupGetMDArrayNames, GDALGroupHS,
    GDALGroupOpenGroup, GDALGroupOpenMDArray, GDALGroupRelease, GDALMDArrayGetAttribute,
    GDALMDArrayGetDataType, GDALMDArrayGetDimensions, GDALMDArrayGetNoDataValueAsDouble,
    GDALMDArrayGetSpatialRef, GDALMDArrayGetUnit, GDALMDArrayH, GDALMDArrayRelease,
    GDALReleaseDimensions, OSRDestroySpatialReference, VSIFree,
};
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
use log::debug;
use snafu::ResultExt;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;

use super::{error, NetCdfCf4DProviderError};

type Result<T, E = NetCdfCf4DProviderError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct MdGroup<'d> {
    c_group: *mut GDALGroupHS,
    _dataset: &'d Dataset,
    pub name: String,
}

impl Drop for MdGroup<'_> {
    fn drop(&mut self) {
        unsafe {
            GDALGroupRelease(self.c_group);
        }
    }
}

#[derive(Debug)]
pub struct MdArray<'g> {
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

#[derive(Debug, Clone, Copy)]
pub struct DimensionSizes {
    pub _entity: usize,
    pub time: usize,
    pub lat: usize,
    pub lon: usize,
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

    pub fn from_dataset(dataset: &'d Dataset) -> Result<Self> {
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
    pub fn attribute_as_string(&self, name: &str) -> Result<String, GdalError> {
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
    pub fn dimension_as_string_array(&self, name: &str) -> Result<Vec<String>, GdalError> {
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

    /// Don't put a NULL-byte in the name!!!
    pub fn dimension_as_double_array(&self, name: &str) -> Result<Vec<f64>, GdalError> {
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

            let mut values: Vec<f64> = vec![0.; count[0] as usize];
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
                values.as_mut_ptr().cast::<std::ffi::c_void>(),
                p_dst_buffer_alloc_start,
                n_dst_buffer_alloc_size,
            );

            if rv == 0 {
                return Err(GdalError::BadArgument("GDALMDArrayRead failed".to_string()));
            }

            GDALExtendedDataTypeRelease(data_type);

            GDALMDArrayRelease(c_mdarray);

            GDALReleaseDimensions(c_dimensions, dim_count);

            values
        };

        Ok(value)
    }

    pub fn group_names(&self) -> Vec<String> {
        let options = CslStringList::new();

        unsafe {
            let c_group_names = GDALGroupGetGroupNames(self.c_group, options.as_ptr());

            let strings = Self::_string_array(c_group_names);

            CSLDestroy(c_group_names);

            strings
        }
    }

    pub fn array_names(&self) -> Vec<String> {
        let mut options = CslStringList::new();
        options
            .set_name_value("SHOW_ZERO_DIM", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("SHOW_COORDINATES", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("SHOW_INDEXING", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("SHOW_BOUNDS", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("SHOW_TIME", "NO")
            .unwrap_or_else(|e| debug!("{}", e));
        options
            .set_name_value("GROUP_BY", "SAME_DIMENSION")
            .unwrap_or_else(|e| debug!("{}", e));

        unsafe {
            let c_group_names = GDALGroupGetMDArrayNames(self.c_group, options.as_ptr());

            let strings = Self::_string_array(c_group_names);

            CSLDestroy(c_group_names);

            strings
        }
    }

    pub fn open_group(&self, name: &str) -> Result<Self> {
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

    pub fn datatype_of_numeric_array(&self, name: &str) -> Result<RasterDataType> {
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

    pub fn open_array(&self, name: &str) -> Result<MdArray> {
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

    pub fn dimension_names(&self) -> Result<Vec<String>, GdalError> {
        let options = CslStringList::new();
        let mut number_of_dimensions = 0;
        let mut dimension_names = Vec::new();

        unsafe {
            let c_dimensions = GDALGroupGetDimensions(
                self.c_group,
                std::ptr::addr_of_mut!(number_of_dimensions),
                options.as_ptr(),
            );

            let dimensions = std::slice::from_raw_parts_mut(c_dimensions, number_of_dimensions);

            for dimension in dimensions {
                let c_name = GDALDimensionGetName(*dimension);

                if c_name.is_null() {
                    return Err(MdGroup::_last_null_pointer_err("GDALDimensionGetName"));
                }

                let name = MdGroup::_string(c_name);

                dimension_names.push(name);
            }

            GDALReleaseDimensions(c_dimensions, number_of_dimensions);
        };

        Ok(dimension_names)
    }
}

/// Always check new implementations with `RUSTFLAGS="-Z sanitizer=leak"`
impl<'g> MdArray<'g> {
    pub fn spatial_reference(&self) -> Result<SpatialReferenceOption> {
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

    pub fn data_type(&self) -> Result<RasterDataType> {
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

    pub fn no_data_value(&self) -> Option<f64> {
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

    pub fn dimensions(&self) -> Result<DimensionSizes> {
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
    pub fn attribute_as_string(&self, name: &str) -> Result<String, GdalError> {
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

    pub fn unit(&self) -> Result<String, GdalError> {
        let value = unsafe {
            let c_attribute = GDALMDArrayGetUnit(self.c_mdarray);

            if c_attribute.is_null() {
                return Err(MdGroup::_last_null_pointer_err("GDALMDArrayGetUnit"));
            }

            MdGroup::_string(c_attribute)
        };

        Ok(value)
    }
}
