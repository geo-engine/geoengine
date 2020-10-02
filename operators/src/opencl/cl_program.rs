use crate::error;
use crate::util::Result;
use arrow::buffer::MutableBuffer;
use arrow::datatypes::{Float64Type, Int64Type};
use arrow::util::bit_util;
use geoengine_datatypes::collections::{
    FeatureCollectionBatchBuilder, TypedFeatureCollection, VectorDataType,
};
use geoengine_datatypes::primitives::{FeatureDataRef, FeatureDataType, NullableDataRef};
use geoengine_datatypes::raster::Raster;
use geoengine_datatypes::raster::{
    DynamicRasterDataType, GridDimension, Pixel, Raster2D, RasterDataType, TypedRaster2D,
};
use geoengine_datatypes::{
    call_generic_features, call_generic_raster2d, call_generic_raster2d_ext,
};
use lazy_static::lazy_static;
use num_traits::AsPrimitive;
use ocl::builders::{KernelBuilder, ProgramBuilder};
use ocl::prm::{cl_double, cl_uint, cl_ushort, Double2};
use ocl::{
    Buffer, Context, Device, Kernel, MemFlags, OclPrm, Platform, Program, Queue, SpatialDims,
};
use snafu::ensure;

// workaround for concurrency issue, see <https://github.com/cogciprocate/ocl/issues/189>
lazy_static! {
    static ref DEVICE: Device = Device::first(Platform::default()).expect("Device has to exist");
}

/// Whether the kernel iterates over pixels or features
#[derive(PartialEq, Clone, Copy, Debug)]
pub enum IterationType {
    Raster,            // 2D Kernel, width x height
    VectorFeatures,    // 1d kernel width = number of features
    VectorCoordinates, // 1d kernel width = number of coordinates
}

/// Specification of raster argument for a `CLProgram`
#[derive(PartialEq, Clone, Copy, Debug)]
pub struct RasterArgument {
    pub data_type: RasterDataType,
}

impl RasterArgument {
    pub fn new(data_type: RasterDataType) -> Self {
        Self { data_type }
    }
}

/// Specification of vector argument for a `CLProgram`
#[derive(PartialEq, Clone, Debug)]
pub struct VectorArgument {
    pub vector_type: VectorDataType,
    pub columns: Vec<ColumnArgument>,
    pub include_geo: bool,
    pub include_time: bool,
}

// Specification of a column of a feature collection
#[derive(PartialEq, Clone, Debug)]
pub struct ColumnArgument {
    pub name: String,
    pub data_type: FeatureDataType,
}

impl ColumnArgument {
    pub fn new(name: String, data_type: FeatureDataType) -> Self {
        Self { name, data_type }
    }
}

impl VectorArgument {
    pub fn new(
        vector_type: VectorDataType,
        columns: Vec<ColumnArgument>,
        include_geo: bool,
        include_time: bool,
    ) -> Self {
        Self {
            vector_type,
            columns,
            include_geo,
            include_time,
        }
    }
}

/// Specifies input and output types of CL program and compiles the source into a reusable `CompiledCLProgram`
pub struct CLProgram {
    input_rasters: Vec<RasterArgument>,
    output_rasters: Vec<RasterArgument>,
    input_features: Vec<VectorArgument>,
    output_features: Vec<VectorArgument>,
    iteration_type: IterationType,
}

impl CLProgram {
    pub fn new(iteration_type: IterationType) -> Self {
        Self {
            input_rasters: vec![],
            output_rasters: vec![],
            input_features: vec![],
            output_features: vec![],
            iteration_type,
        }
    }

    pub fn add_input_raster(&mut self, raster: RasterArgument) {
        self.input_rasters.push(raster);
    }

    pub fn add_output_raster(&mut self, raster: RasterArgument) {
        self.output_rasters.push(raster);
    }

    pub fn add_input_features(&mut self, vector_type: VectorArgument) {
        self.input_features.push(vector_type);
    }

    pub fn add_output_features(&mut self, vector_type: VectorArgument) {
        self.output_features.push(vector_type);
    }

    fn create_type_definitions(&self) -> String {
        let mut s = String::new();

        if self.input_rasters.is_empty() && self.output_rasters.is_empty() {
            return s;
        }

        s.push_str(
            r####"
typedef struct {
	uint size[3];
	double origin[3];
	double scale[3];
	double min, max, no_data;
	ushort crs_code;
	ushort has_no_data;
} RasterInfo;

#define R(t,x,y) t ## _data[y * t ## _info->size[0] + x]
"####,
        );

        for (idx, raster) in self.input_rasters.iter().enumerate() {
            s += &format!("typedef {} IN_TYPE{};\n", raster.data_type.ocl_type(), idx);

            if raster.data_type == RasterDataType::F32 || raster.data_type == RasterDataType::F64 {
                s += &format!(
                    "#define ISNODATA{}(v,i) (i->has_no_data && (isnan(v) || v == i->no_data))\n",
                    idx
                );
            } else {
                s += &format!(
                    "#define ISNODATA{}(v,i) (i->has_no_data && v == i->no_data)\n",
                    idx
                );
            }
        }

        for (idx, raster) in self.output_rasters.iter().enumerate() {
            s += &format!("typedef {} OUT_TYPE{};\n", raster.data_type.ocl_type(), idx);
        }

        s
    }

    pub fn compile(self, source: &str, kernel_name: &str) -> Result<CompiledCLProgram> {
        match self.iteration_type {
            IterationType::Raster => ensure!(
                !self.input_rasters.is_empty() && !self.output_rasters.is_empty(),
                error::CLInvalidInputsForIterationType,
            ),
            IterationType::VectorFeatures | IterationType::VectorCoordinates => ensure!(
                !self.input_features.is_empty() && !self.output_features.is_empty(),
                error::CLInvalidInputsForIterationType
            ),
        }

        let typedefs = self.create_type_definitions();

        let platform = Platform::default(); // TODO: make configurable

        // the following fails for concurrent access, see <https://github.com/cogciprocate/ocl/issues/189>
        // let device = Device::first(platform)?;
        let device = *DEVICE; // TODO: make configurable

        let ctx = Context::builder()
            .platform(platform)
            .devices(device)
            .build()?; // TODO: make configurable

        let program = ProgramBuilder::new()
            .src(typedefs)
            .src(source)
            .build(&ctx)?;

        // TODO: create kernel builder here once it is cloneable <https://github.com/cogciprocate/ocl/issues/190>

        Ok(CompiledCLProgram::new(
            ctx,
            program,
            kernel_name.to_string(),
            self.iteration_type,
            self.input_rasters,
            self.output_rasters,
            self.input_features,
            self.output_features,
        ))
    }
}

#[derive(Clone)]
enum RasterOutputBuffer {
    U8(Buffer<u8>),
    U16(Buffer<u16>),
    U32(Buffer<u32>),
    U64(Buffer<u64>),
    I8(Buffer<i8>),
    I16(Buffer<i16>),
    I32(Buffer<i32>),
    I64(Buffer<i64>),
    F32(Buffer<f32>),
    F64(Buffer<f64>),
}

enum FeatureGeoOutputBuffer {
    Points(PointBuffers),
    _Lines(LineBuffers),
    _Polygons(PolygonBuffers),
}

struct PointBuffers {
    coords: Buffer<Double2>,
    offsets: Buffer<i32>,
}

struct LineBuffers {
    _coords: Buffer<Double2>,
    _line_offsets: Buffer<i32>,
    _feature_offsets: Buffer<i32>,
}

struct PolygonBuffers {
    _coords: Buffer<Double2>,
    _ring_offsets: Buffer<i32>,
    _polygon_offsets: Buffer<i32>,
    _feature_offets: Buffer<i32>,
}

struct ColumnBuffer<T: OclPrm> {
    column_name: String,
    values: Buffer<T>,
    nulls: Option<Buffer<i32>>, // OpenCl does not support bool for host device transfer
}

struct FeatureOutputBuffers {
    geo: Option<FeatureGeoOutputBuffer>,
    numbers: Vec<ColumnBuffer<f64>>,
    decimals: Vec<ColumnBuffer<i64>>,
    // TODO: categories, strings
    // TODO: time
}

/// This struct accepts concrete raster and vector data that correspond to the specified arguments.
/// It can be executed by the `CompiledCLProgram`s `run` method, holds the output buffers and
/// is consumed once the kernel execution is completed
pub struct CLProgramRunnable<'a> {
    input_raster_types: Vec<RasterArgument>,
    output_raster_types: Vec<RasterArgument>,
    input_rasters: Vec<Option<&'a TypedRaster2D>>,
    output_rasters: Vec<Option<&'a mut TypedRaster2D>>,
    input_feature_types: Vec<VectorArgument>,
    output_feature_types: Vec<VectorArgument>,
    input_features: Vec<Option<&'a TypedFeatureCollection>>,
    output_features: Vec<Option<&'a mut FeatureCollectionBatchBuilder>>,
    raster_output_buffers: Vec<RasterOutputBuffer>,
    feature_output_buffers: Vec<FeatureOutputBuffers>,
}

impl<'a> CLProgramRunnable<'a> {
    fn new(
        input_raster_types: Vec<RasterArgument>,
        output_raster_types: Vec<RasterArgument>,
        input_feature_types: Vec<VectorArgument>,
        output_feature_types: Vec<VectorArgument>,
    ) -> Self {
        let mut output_rasters = Vec::new();
        output_rasters.resize_with(output_raster_types.len(), || None);

        let mut output_features = Vec::new();
        output_features.resize_with(output_feature_types.len(), || None);

        Self {
            input_rasters: vec![None; input_raster_types.len()],
            input_features: vec![None; input_feature_types.len()],
            output_rasters,
            input_raster_types,
            output_raster_types,
            input_feature_types,
            output_feature_types,
            output_features,
            raster_output_buffers: vec![],
            feature_output_buffers: vec![],
        }
    }

    pub fn set_input_raster(&mut self, idx: usize, raster: &'a TypedRaster2D) -> Result<()> {
        ensure!(
            idx < self.input_raster_types.len(),
            error::CLProgramInvalidRasterIndex
        );
        ensure!(
            raster.raster_data_type() == self.input_raster_types[idx].data_type,
            error::CLProgramInvalidRasterDataType
        );
        self.input_rasters[idx] = Some(raster);
        Ok(())
    }

    pub fn set_output_raster(&mut self, idx: usize, raster: &'a mut TypedRaster2D) -> Result<()> {
        ensure!(
            idx < self.input_raster_types.len(),
            error::CLProgramInvalidRasterIndex
        );
        ensure!(
            raster.raster_data_type() == self.output_raster_types[idx].data_type,
            error::CLProgramInvalidRasterDataType
        );
        self.output_rasters[idx] = Some(raster);
        Ok(())
    }

    pub fn set_input_features(
        &mut self,
        idx: usize,
        features: &'a TypedFeatureCollection,
    ) -> Result<()> {
        ensure!(
            idx < self.input_feature_types.len(),
            error::CLProgramInvalidFeaturesIndex
        );
        ensure!(
            features.vector_data_type() == self.input_feature_types[idx].vector_type,
            error::CLProgramInvalidVectorDataType
        );

        let columns_ok = call_generic_features!(features, f =>  self.input_feature_types[idx]
            .columns
            .iter().all(|c| f.column_type(&c.name).map_or(false, |to| to == c.data_type)));
        ensure!(columns_ok, error::CLProgramInvalidColumn);

        self.input_features[idx] = Some(features);
        Ok(())
    }

    pub fn set_output_features(
        &mut self,
        idx: usize,
        features: &'a mut FeatureCollectionBatchBuilder,
    ) -> Result<()> {
        ensure!(
            idx < self.output_feature_types.len(),
            error::CLProgramInvalidFeaturesIndex
        );
        ensure!(
            features.output_type == self.output_feature_types[idx].vector_type,
            error::CLProgramInvalidVectorDataType
        );

        let input_types = features.column_types();
        ensure!(
            self.output_feature_types[idx].columns.iter().all(|column| {
                input_types
                    .get(&column.name)
                    .map_or(false, |input_type| input_type == &column.data_type)
            }),
            error::CLProgramInvalidColumn
        );

        self.output_features[idx] = Some(features);
        Ok(())
    }

    fn set_feature_input_arguments(&mut self, kernel: &Kernel) -> Result<()> {
        ensure!(
            self.input_features.iter().all(Option::is_some),
            error::CLProgramUnspecifiedFeatures
        );

        for (idx, (features, argument)) in self
            .input_features
            .iter()
            .zip(self.input_feature_types.iter())
            .enumerate()
        {
            let features = features.expect("checked");

            if argument.include_geo {
                match features {
                    TypedFeatureCollection::Data(_) => {
                        // no geo
                    }
                    TypedFeatureCollection::MultiPoint(points) => {
                        let coordinates = points.coordinates();
                        let buffer = Buffer::<Double2>::builder()
                            .queue(kernel.default_queue().expect("expect").clone())
                            .len(coordinates.len())
                            .copy_host_slice(unsafe {
                                std::slice::from_raw_parts(
                                    coordinates.as_ptr() as *const Double2,
                                    coordinates.len(),
                                )
                            })
                            .build()?;

                        kernel.set_arg(format!("IN_POINT_COORDS{}", idx), &buffer)?;

                        let coordinates_offsets = points.multipoint_offsets();
                        let buffer = Buffer::builder()
                            .queue(kernel.default_queue().expect("expect").clone())
                            .len(coordinates_offsets.len())
                            .copy_host_slice(coordinates_offsets)
                            .build()?;

                        kernel.set_arg(format!("IN_POINT_OFFSETS{}", idx), &buffer)?;
                    }
                    TypedFeatureCollection::MultiLineString(_)
                    | TypedFeatureCollection::MultiPolygon(_) => todo!(), // TODO: lines, polygons
                }
            }

            let column_types = call_generic_features!(features, features => {
                features.column_types()
            });

            let len = call_generic_features!(features, features => {
                features.len()
            });

            for (column, _) in column_types {
                let data = call_generic_features!(features, features => {
                    features.data(&column)?
                });
                match data {
                    FeatureDataRef::NullableNumber(numbers) => {
                        Self::set_feature_column_input_argument(
                            kernel,
                            idx,
                            &column,
                            len,
                            numbers.as_ref(),
                            Some(numbers.nulls().as_ref()),
                        )?
                    }
                    FeatureDataRef::Number(numbers) => Self::set_feature_column_input_argument(
                        kernel,
                        idx,
                        &column,
                        len,
                        numbers.as_ref(),
                        None,
                    )?,
                    FeatureDataRef::Decimal(decimals) => Self::set_feature_column_input_argument(
                        kernel,
                        idx,
                        &column,
                        len,
                        decimals.as_ref(),
                        None,
                    )?,
                    FeatureDataRef::NullableDecimal(decimals) => {
                        Self::set_feature_column_input_argument(
                            kernel,
                            idx,
                            &column,
                            len,
                            decimals.as_ref(),
                            Some(decimals.nulls().as_ref()),
                        )?
                    }
                    _ => todo!(), // TODO: strings, categories
                }
            }

            if argument.include_time {
                // TODO time
            }
        }
        Ok(())
    }

    fn set_feature_output_arguments(&mut self, kernel: &Kernel) -> Result<()> {
        for (idx, (builder, argument)) in self
            .output_features
            .iter()
            .zip(self.output_feature_types.iter())
            .enumerate()
        {
            let features = builder.as_ref().expect("checked");

            let geo_buffers = if argument.include_geo {
                Some(match features.output_type {
                    VectorDataType::MultiPoint => {
                        let coords = Buffer::<Double2>::builder()
                            .queue(kernel.default_queue().expect("expect").clone())
                            .len(features.num_coords())
                            .build()?;
                        kernel.set_arg(format!("OUT_POINT_COORDS{}", idx), &coords)?;

                        let offsets = Buffer::<i32>::builder()
                            .queue(kernel.default_queue().expect("expect").clone())
                            .len(features.num_features() + 1)
                            .build()?;
                        kernel.set_arg(format!("OUT_POINT_OFFSETS{}", idx), &offsets)?;

                        FeatureGeoOutputBuffer::Points(PointBuffers { coords, offsets })
                    }
                    _ => todo!(), // TODO: lines, polygons
                })
            } else {
                None
            };

            let mut numbers = Vec::new();
            let mut decimals = Vec::new();
            for column in &argument.columns {
                match column.data_type {
                    FeatureDataType::Number => Self::set_feature_column_output_argument::<f64>(
                        &mut numbers,
                        kernel,
                        idx,
                        &column.name,
                        features.num_features(),
                        false,
                    ),
                    FeatureDataType::NullableNumber => {
                        Self::set_feature_column_output_argument::<f64>(
                            &mut numbers,
                            kernel,
                            idx,
                            &column.name,
                            features.num_features(),
                            true,
                        )
                    }
                    FeatureDataType::Decimal => Self::set_feature_column_output_argument::<i64>(
                        &mut decimals,
                        kernel,
                        idx,
                        &column.name,
                        features.num_features(),
                        false,
                    ),
                    FeatureDataType::NullableDecimal => {
                        Self::set_feature_column_output_argument::<i64>(
                            &mut decimals,
                            kernel,
                            idx,
                            &column.name,
                            features.num_features(),
                            true,
                        )
                    }
                    _ => todo!(), // TODO: strings, categories
                }?;
            }

            // TODO: time buffer

            self.feature_output_buffers.push(FeatureOutputBuffers {
                geo: geo_buffers,
                numbers,
                decimals,
            })
        }

        Ok(())
    }

    fn set_feature_column_input_argument<T: OclPrm>(
        kernel: &Kernel,
        idx: usize,
        column: &str,
        len: usize,
        data: &[T],
        nulls: Option<&[bool]>,
    ) -> Result<()> {
        let buffer = Buffer::<T>::builder()
            .queue(kernel.default_queue().expect("expect").clone())
            .len(len)
            .copy_host_slice(data)
            .build()?;
        kernel.set_arg(format!("IN_POINT{}_COLUMN_{}", idx, column), &buffer)?;

        if let Some(nulls) = nulls {
            // TODO: convert booleans to i32 for host device transfer more efficiently
            let nulls: Vec<i32> = nulls.iter().map(|n| n.as_()).collect();
            let buffer = Buffer::<i32>::builder()
                .queue(kernel.default_queue().expect("expect").clone())
                .len(len)
                .copy_host_slice(nulls.as_slice())
                .build()?;
            kernel.set_arg(format!("IN_POINT{}_NULLS_{}", idx, column), &buffer)?;
        }

        Ok(())
    }

    fn set_feature_column_output_argument<T: OclPrm>(
        buffers: &mut Vec<ColumnBuffer<T>>,
        kernel: &Kernel,
        idx: usize,
        column: &str,
        len: usize,
        nullable: bool,
    ) -> Result<()> {
        let values = Buffer::<T>::builder()
            .queue(kernel.default_queue().expect("expect").clone())
            .len(len)
            .build()?;
        kernel.set_arg(format!("OUT_POINT{}_COLUMN_{}", idx, column), &values)?;

        let nulls = if nullable {
            let buffer = Buffer::<i32>::builder()
                .queue(kernel.default_queue().expect("expect").clone())
                .len(len)
                .build()?;
            kernel.set_arg(format!("OUT_POINT{}_NULLS_{}", idx, column), &buffer)?;

            Some(buffer)
        } else {
            None
        };

        buffers.push(ColumnBuffer {
            column_name: column.to_owned(),
            values,
            nulls,
        });
        Ok(())
    }

    fn set_raster_arguments(&mut self, kernel: &Kernel) -> Result<()> {
        ensure!(
            self.input_rasters.iter().all(Option::is_some),
            error::CLProgramUnspecifiedRaster
        );

        for (idx, raster) in self.input_rasters.iter().enumerate() {
            let raster = raster.expect("checked");
            call_generic_raster2d!(raster, raster => {
                let data_buffer = Buffer::builder()
                .queue(kernel.default_queue().expect("checked").clone())
                .flags(MemFlags::new().read_only())
                .len(raster.data_container.len())
                .copy_host_slice(&raster.data_container)
                .build()?;
                kernel.set_arg(format!("IN{}",idx), data_buffer)?;

                let info_buffer = Buffer::builder()
                .queue(kernel.default_queue().expect("checked").clone())
                .flags(MemFlags::new().read_only())
                .len(1)
                .copy_host_slice(&[RasterInfo::from_raster(&raster)])
                .build()?;
                kernel.set_arg(format!("IN_INFO{}",idx), info_buffer)?;
            });
        }

        for (idx, raster) in self.output_rasters.iter().enumerate() {
            let raster = raster.as_ref().expect("checked");
            call_generic_raster2d_ext!(raster, RasterOutputBuffer, (raster, e) => {
                let buffer = Buffer::builder()
                    .queue(kernel.default_queue().expect("expect").clone())
                    .len(raster.data_container.len())
                    .build()?;

                kernel.set_arg(format!("OUT{}", idx), &buffer)?;

                self.raster_output_buffers.push(e(buffer));

                let info_buffer = Buffer::builder()
                    .queue(kernel.default_queue().expect("checked").clone())
                    .flags(MemFlags::new().read_only())
                    .len(1)
                    .copy_host_slice(&[RasterInfo::from_raster(&raster)])
                    .build()?;
                kernel.set_arg(format!("OUT_INFO{}", idx), info_buffer)?;
            })
        }

        Ok(())
    }

    fn read_raster_output_buffers(&mut self) -> Result<()> {
        for (output_buffer, output_raster) in self
            .raster_output_buffers
            .drain(..)
            .zip(self.output_rasters.iter_mut())
        {
            match (output_buffer, output_raster) {
                (RasterOutputBuffer::U8(ref buffer), Some(TypedRaster2D::U8(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (RasterOutputBuffer::U16(ref buffer), Some(TypedRaster2D::U16(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (RasterOutputBuffer::U32(ref buffer), Some(TypedRaster2D::U32(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (RasterOutputBuffer::U64(ref buffer), Some(TypedRaster2D::U64(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (RasterOutputBuffer::I8(ref buffer), Some(TypedRaster2D::I8(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (RasterOutputBuffer::I16(ref buffer), Some(TypedRaster2D::I16(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (RasterOutputBuffer::I32(ref buffer), Some(TypedRaster2D::I32(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (RasterOutputBuffer::I64(ref buffer), Some(TypedRaster2D::I64(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (RasterOutputBuffer::F32(ref buffer), Some(TypedRaster2D::F32(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (RasterOutputBuffer::F64(ref buffer), Some(TypedRaster2D::F64(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                _ => unreachable!(),
            };
        }

        Ok(())
    }

    fn read_feature_output_buffers(&mut self) -> Result<()> {
        for (output_buffers, builder) in self
            .feature_output_buffers
            .drain(..)
            .zip(self.output_features.drain(..))
        {
            let builder = builder.expect("checked");

            match output_buffers.geo {
                Some(FeatureGeoOutputBuffer::Points(buffers)) => {
                    let offsets_buffer = Self::read_ocl_to_arrow_buffer(
                        &buffers.offsets,
                        builder.num_features() + 1,
                    )?;
                    let coords_buffer =
                        Self::read_ocl_to_arrow_buffer(&buffers.coords, builder.num_coords())?;
                    builder.set_points(coords_buffer, offsets_buffer)?;
                }
                None => {}
                Some(_) => todo!(), // TODO: lines, polygons
            }

            for column_buffer in output_buffers.numbers {
                let values_buffer =
                    Self::read_ocl_to_arrow_buffer(&column_buffer.values, builder.num_features())?;

                let nulls_buffer = if let Some(nulls_buffer) = column_buffer.nulls {
                    // TODO: read i32 into null buffers as bool more efficiently
                    let mut nulls = vec![0_i32; builder.num_features()];
                    nulls_buffer.read(&mut nulls).enq()?;

                    let num_bytes = bit_util::ceil(nulls.len(), 8);
                    let mut arrow_buffer =
                        MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
                    let null_slice = arrow_buffer.data_mut();

                    for (i, null) in nulls.iter().enumerate() {
                        if *null != 0 {
                            bit_util::set_bit(null_slice, i);
                        }
                    }
                    Some(arrow_buffer.freeze())
                } else {
                    None
                };

                builder.set_column::<Float64Type>(
                    &column_buffer.column_name,
                    values_buffer,
                    nulls_buffer,
                )?;
            }

            for column_buffer in output_buffers.decimals {
                let values_buffer =
                    Self::read_ocl_to_arrow_buffer(&column_buffer.values, builder.num_features())?;

                let nulls_buffer = if let Some(nulls_buffer) = column_buffer.nulls {
                    // TODO: read i32 into null buffers as bool more efficiently
                    let mut nulls = vec![0_i32; builder.num_features()];
                    nulls_buffer.read(&mut nulls).enq()?;

                    let num_bytes = bit_util::ceil(nulls.len(), 8);
                    let mut arrow_buffer =
                        MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
                    let null_slice = arrow_buffer.data_mut();

                    for (i, null) in nulls.iter().enumerate() {
                        if *null != 0 {
                            bit_util::set_bit(null_slice, i);
                        }
                    }
                    Some(arrow_buffer.freeze())
                } else {
                    None
                };

                builder.set_column::<Int64Type>(
                    &column_buffer.column_name,
                    values_buffer,
                    nulls_buffer,
                )?;
            }

            // TODO: string, category columns

            // TODO: time
            builder.set_default_time_intervals()?;

            builder.finish()?;
        }
        Ok(())
    }

    fn read_ocl_to_arrow_buffer<T: OclPrm>(
        ocl_buffer: &Buffer<T>,
        len: usize,
    ) -> Result<arrow::buffer::Buffer> {
        let mut arrow_buffer = MutableBuffer::new(len * std::mem::size_of::<T>());
        arrow_buffer.resize(len * std::mem::size_of::<T>()).unwrap();

        let dest = unsafe {
            std::slice::from_raw_parts_mut(arrow_buffer.data_mut().as_ptr() as *mut T, len)
        };

        ocl_buffer.read(dest).enq()?;

        Ok(arrow_buffer.freeze())
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct RasterInfo {
    pub size: [cl_uint; 3],
    pub origin: [cl_double; 3],
    pub scale: [cl_double; 3],

    pub min: cl_double,
    pub max: cl_double,
    pub no_data: cl_double,

    pub crs_code: cl_ushort,
    pub has_no_data: cl_ushort,
}

unsafe impl Send for RasterInfo {}
unsafe impl Sync for RasterInfo {}
unsafe impl OclPrm for RasterInfo {}

impl RasterInfo {
    pub fn from_raster<T: Pixel>(raster: &Raster2D<T>) -> Self {
        // TODO: extract missing information from raster
        Self {
            size: [
                raster.dimension().size_of_x_axis().as_(),
                raster.dimension().size_of_y_axis().as_(),
                1, // TODO
            ],
            origin: [0., 0., 0.],
            scale: [0., 0., 0.],
            min: 0.,
            max: 0.,
            no_data: raster.no_data_value.map_or(0., AsPrimitive::as_),
            crs_code: 0,
            has_no_data: u16::from(raster.no_data_value.is_some()),
        }
    }
}

/// Allows running kernels on different inputs and outputs
#[derive(Clone)]
pub struct CompiledCLProgram {
    ctx: Context,
    program: Program,
    kernel_name: String,
    iteration_type: IterationType,
    input_raster_types: Vec<RasterArgument>,
    output_raster_types: Vec<RasterArgument>,
    input_feature_types: Vec<VectorArgument>,
    output_feature_types: Vec<VectorArgument>,
}

unsafe impl Send for CompiledCLProgram {}

impl CompiledCLProgram {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Context,
        program: Program,
        kernel_name: String,
        iteration_type: IterationType,
        input_raster_types: Vec<RasterArgument>,
        output_raster_types: Vec<RasterArgument>,
        input_feature_types: Vec<VectorArgument>,
        output_feature_types: Vec<VectorArgument>,
    ) -> Self {
        Self {
            ctx,
            program,
            kernel_name,
            iteration_type,
            input_raster_types,
            output_raster_types,
            input_feature_types,
            output_feature_types,
        }
    }

    pub fn runnable<'b>(&self) -> CLProgramRunnable<'b> {
        CLProgramRunnable::new(
            self.input_raster_types.clone(),
            self.output_raster_types.clone(),
            self.input_feature_types.clone(),
            self.output_feature_types.clone(),
        )
    }

    fn add_data_buffer_placeholder(
        kernel: &mut KernelBuilder,
        arg_name: String,
        data_type: RasterDataType,
    ) {
        match data_type {
            RasterDataType::U8 => kernel.arg_named(arg_name, None::<&Buffer<u8>>),
            RasterDataType::U16 => kernel.arg_named(arg_name, None::<&Buffer<u16>>),
            RasterDataType::U32 => kernel.arg_named(arg_name, None::<&Buffer<u32>>),
            RasterDataType::U64 => kernel.arg_named(arg_name, None::<&Buffer<u64>>),
            RasterDataType::I8 => kernel.arg_named(arg_name, None::<&Buffer<i8>>),
            RasterDataType::I16 => kernel.arg_named(arg_name, None::<&Buffer<i16>>),
            RasterDataType::I32 => kernel.arg_named(arg_name, None::<&Buffer<i32>>),
            RasterDataType::I64 => kernel.arg_named(arg_name, None::<&Buffer<i64>>),
            RasterDataType::F32 => kernel.arg_named(arg_name, None::<&Buffer<f32>>),
            RasterDataType::F64 => kernel.arg_named(arg_name, None::<&Buffer<f64>>),
        };
    }

    fn work_size(&self, runnable: &CLProgramRunnable) -> SpatialDims {
        match self.iteration_type {
            IterationType::Raster => call_generic_raster2d!(runnable.output_rasters[0].as_ref()
                .expect("checked"), raster => SpatialDims::Two(raster.dimension().size_of_x_axis(), raster.dimension().size_of_y_axis())),
            IterationType::VectorFeatures => SpatialDims::One(
                runnable.output_features[0]
                    .as_ref()
                    .expect("checked")
                    .num_features(),
            ),
            IterationType::VectorCoordinates => SpatialDims::One(
                runnable.output_features[0]
                    .as_ref()
                    .expect("checked")
                    .num_coords(),
            ),
        }
    }

    pub fn run(&mut self, mut runnable: CLProgramRunnable) -> Result<()> {
        // TODO: select correct device
        let queue = Queue::new(&self.ctx, self.ctx.devices()[0], None)?;

        // TODO: create the kernel builder only once in CLProgram once it is cloneable
        let mut kernel = Kernel::builder();
        let program = self.program.clone();
        kernel
            .queue(queue)
            .program(&program)
            .name(&self.kernel_name);

        // TODO: set the arguments either in CLProgram or set them directly instead of placeholders
        self.set_argument_placeholders(&mut kernel);

        let kernel = kernel.build()?;

        runnable.set_raster_arguments(&kernel)?;

        runnable.set_feature_input_arguments(&kernel)?;
        runnable.set_feature_output_arguments(&kernel)?;

        let dims = self.work_size(&runnable);
        unsafe {
            kernel.cmd().global_work_size(dims).enq()?;
        }

        runnable.read_raster_output_buffers()?;
        runnable.read_feature_output_buffers()?;

        Ok(())
    }

    fn set_argument_placeholders(&mut self, mut kernel: &mut KernelBuilder) {
        for (idx, raster) in self.input_raster_types.iter().enumerate() {
            Self::add_data_buffer_placeholder(&mut kernel, format!("IN{}", idx), raster.data_type);
            kernel.arg_named(format!("IN_INFO{}", idx), None::<&Buffer<RasterInfo>>);
        }

        for (idx, raster) in self.output_raster_types.iter().enumerate() {
            Self::add_data_buffer_placeholder(&mut kernel, format!("OUT{}", idx), raster.data_type);
            kernel.arg_named(format!("OUT_INFO{}", idx), None::<&Buffer<RasterInfo>>);
        }

        for (idx, features) in self.input_feature_types.iter().enumerate() {
            if features.include_geo {
                match features.vector_type {
                    VectorDataType::Data => {
                        // no geo
                    }
                    VectorDataType::MultiPoint => {
                        kernel
                            .arg_named(format!("IN_POINT_COORDS{}", idx), None::<&Buffer<Double2>>);
                        kernel.arg_named(format!("IN_POINT_OFFSETS{}", idx), None::<&Buffer<i32>>);
                    }
                    VectorDataType::MultiLineString | VectorDataType::MultiPolygon => todo!(),
                }
            }

            for column in &features.columns {
                let name = format!("IN_POINT{}_COLUMN_{}", idx, column.name);
                let null_name = format!("IN_POINT{}_NULLS_{}", idx, column.name);
                Self::set_column_argument_placeholder(
                    &mut kernel,
                    column.data_type,
                    name,
                    null_name,
                )
            }

            if features.include_time {
                // TODO time
            }
        }

        for (idx, features) in self.output_feature_types.iter().enumerate() {
            if features.include_geo {
                match features.vector_type {
                    VectorDataType::Data => {
                        // no geo
                    }
                    VectorDataType::MultiPoint => {
                        kernel.arg_named(
                            format!("OUT_POINT_COORDS{}", idx),
                            None::<&Buffer<Double2>>,
                        );
                        kernel.arg_named(format!("OUT_POINT_OFFSETS{}", idx), None::<&Buffer<i32>>);
                    }
                    VectorDataType::MultiLineString | VectorDataType::MultiPolygon => todo!(),
                }
            }

            for column in &features.columns {
                let name = format!("OUT_POINT{}_COLUMN_{}", idx, column.name);
                let null_name = format!("OUT_POINT{}_NULLS_{}", idx, column.name);
                Self::set_column_argument_placeholder(
                    &mut kernel,
                    column.data_type,
                    name,
                    null_name,
                )
            }

            if features.include_time {
                // TODO: time
            }
        }
    }

    fn set_column_argument_placeholder(
        kernel: &mut KernelBuilder,
        column_type: FeatureDataType,
        name: String,
        null_name: String,
    ) {
        match column_type {
            FeatureDataType::Number => {
                kernel.arg_named(name, None::<&Buffer<f64>>);
            }
            FeatureDataType::NullableNumber => {
                kernel.arg_named(name, None::<&Buffer<f64>>);
                kernel.arg_named(null_name, None::<&Buffer<i32>>);
            }
            FeatureDataType::Decimal => {
                kernel.arg_named(name, None::<&Buffer<i64>>);
            }
            FeatureDataType::NullableDecimal => {
                kernel.arg_named(name, None::<&Buffer<i64>>);
                kernel.arg_named(null_name, None::<&Buffer<i32>>);
            }

            _ => todo!(), // TODO strings, categories
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayData, PrimitiveArray};
    use arrow::buffer::MutableBuffer;
    use arrow::datatypes::{DataType, Int32Type};
    use geoengine_datatypes::collections::{
        BuilderProvider, DataCollection, FeatureCollection, MultiPointCollection,
    };
    use geoengine_datatypes::primitives::{FeatureData, MultiPoint, NoGeometry, TimeInterval};
    use geoengine_datatypes::raster::Raster2D;
    use ocl::ProQue;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn kernel_reuse() {
        let in0 = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let in1 = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![7, 8, 9, 10, 11, 12],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let mut out = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![-1, -1, -1, -1, -1, -1],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let kernel = r#"
__kernel void add(
            __global const IN_TYPE0 *in_data0,
            __global const RasterInfo *in_info0,
            __global const IN_TYPE1* in_data1,
            __global const RasterInfo *in_info1,
            __global OUT_TYPE0* out_data,
            __global const RasterInfo *out_info)            
{
    uint const idx = get_global_id(0) + get_global_id(1) * in_info0->size[0];
    out_data[idx] = in_data0[idx] + in_data1[idx];
}"#;

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_raster(RasterArgument::new(in0.raster_data_type()));
        cl_program.add_input_raster(RasterArgument::new(in1.raster_data_type()));
        cl_program.add_output_raster(RasterArgument::new(out.raster_data_type()));

        let mut compiled = cl_program.compile(kernel, "add").unwrap();

        let mut runnable = compiled.runnable();
        runnable.set_input_raster(0, &in0).unwrap();
        runnable.set_input_raster(1, &in1).unwrap();
        runnable.set_output_raster(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        assert_eq!(
            out.get_i32_ref().unwrap().data_container,
            vec![8, 10, 12, 14, 16, 18]
        );

        let mut runnable = compiled.runnable();
        runnable.set_input_raster(0, &in0).unwrap();
        runnable.set_input_raster(1, &in0).unwrap();
        runnable.set_output_raster(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        assert_eq!(
            out.get_i32().unwrap().data_container,
            vec![2, 4, 6, 8, 10, 12]
        );
    }

    #[test]
    fn mixed_types() {
        let in0 = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let in1 = TypedRaster2D::U16(
            Raster2D::new(
                [3, 2].into(),
                vec![7, 8, 9, 10, 11, 12],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let mut out = TypedRaster2D::I64(
            Raster2D::new(
                [3, 2].into(),
                vec![-1, -1, -1, -1, -1, -1],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let kernel = r#"
__kernel void add(
            __global const IN_TYPE0 *in_data0,
            __global const RasterInfo *in_info0,
            __global const IN_TYPE1* in_data1,
            __global const RasterInfo *in_info1,
            __global OUT_TYPE0* out_data,
            __global const RasterInfo *out_info)            
{
    uint const idx = get_global_id(0) + get_global_id(1) * in_info0->size[0];
    out_data[idx] = in_data0[idx] + in_data1[idx];
}"#;

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_raster(RasterArgument::new(in0.raster_data_type()));
        cl_program.add_input_raster(RasterArgument::new(in1.raster_data_type()));
        cl_program.add_output_raster(RasterArgument::new(out.raster_data_type()));

        let mut compiled = cl_program.compile(kernel, "add").unwrap();

        let mut runnable = compiled.runnable();
        runnable.set_input_raster(0, &in0).unwrap();
        runnable.set_input_raster(1, &in1).unwrap();
        runnable.set_output_raster(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        assert_eq!(
            out.get_i64_ref().unwrap().data_container,
            vec![8, 10, 12, 14, 16, 18]
        );
    }

    #[test]
    fn raster_info() {
        let in0 = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                Some(1337),
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let mut out = TypedRaster2D::I64(
            Raster2D::new(
                [3, 2].into(),
                vec![-1, -1, -1, -1, -1, -1],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let kernel = r#"
__kernel void no_data( 
            __global const IN_TYPE0 *in_data,
            __global const RasterInfo *in_info,
            __global OUT_TYPE0* out_data,
            __global const RasterInfo *out_info)            
{
    uint const idx = get_global_id(0) + get_global_id(1) * in_info->size[0];
    out_data[idx] = in_info->no_data;
}"#;

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_raster(RasterArgument::new(in0.raster_data_type()));
        cl_program.add_output_raster(RasterArgument::new(out.raster_data_type()));

        let mut compiled = cl_program.compile(kernel, "no_data").unwrap();

        let mut runnable = compiled.runnable();
        runnable.set_input_raster(0, &in0).unwrap();
        runnable.set_output_raster(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        assert_eq!(
            out.get_i64_ref().unwrap().data_container,
            vec![1337, 1337, 1337, 1337, 1337, 1337]
        );
    }

    #[test]
    fn no_data() {
        let in0 = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 1337, 3, 4, 5, 6],
                Some(1337),
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let mut out = TypedRaster2D::I64(
            Raster2D::new(
                [3, 2].into(),
                vec![-1, -1, -1, -1, -1, -1],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let kernel = r#"
__kernel void no_data( 
            __global const IN_TYPE0 *in_data,
            __global const RasterInfo *in_info,
            __global OUT_TYPE0* out_data,
            __global const RasterInfo *out_info)            
{
    uint const idx = get_global_id(0) + get_global_id(1) * in_info->size[0];
    if (ISNODATA0(in_data[idx], in_info)) {    
        out_data[idx] = 1;
    } else {
        out_data[idx] = 0;
    }
}"#;

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_raster(RasterArgument::new(in0.raster_data_type()));
        cl_program.add_output_raster(RasterArgument::new(out.raster_data_type()));

        let mut compiled = cl_program.compile(kernel, "no_data").unwrap();

        let mut runnable = compiled.runnable();
        runnable.set_input_raster(0, &in0).unwrap();
        runnable.set_output_raster(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        assert_eq!(
            out.get_i64_ref().unwrap().data_container,
            vec![0, 1, 0, 0, 0, 0]
        );
    }

    #[test]
    fn no_data_float() {
        let in0 = TypedRaster2D::F32(
            Raster2D::new(
                [3, 2].into(),
                vec![1., 1337., f32::NAN, 4., 5., 6.],
                Some(1337.),
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let mut out = TypedRaster2D::I64(
            Raster2D::new(
                [3, 2].into(),
                vec![-1, -1, -1, -1, -1, -1],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let kernel = r#"
__kernel void no_data( 
            __global const IN_TYPE0 *in_data,
            __global const RasterInfo *in_info,
            __global OUT_TYPE0* out_data,
            __global const RasterInfo *out_info)            
{
    uint const idx = get_global_id(0) + get_global_id(1) * in_info->size[0];
    if (ISNODATA0(in_data[idx], in_info)) {    
        out_data[idx] = 1;
    } else {
        out_data[idx] = 0;
    }
}"#;

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_raster(RasterArgument::new(in0.raster_data_type()));
        cl_program.add_output_raster(RasterArgument::new(out.raster_data_type()));

        let mut compiled = cl_program.compile(kernel, "no_data").unwrap();

        let mut runnable = compiled.runnable();
        runnable.set_input_raster(0, &in0).unwrap();
        runnable.set_output_raster(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        assert_eq!(
            out.get_i64_ref().unwrap().data_container,
            vec![0, 1, 1, 0, 0, 0]
        );
    }

    #[test]
    fn gid_calculation() {
        let in0 = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![0; 6],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let mut out = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![0; 6],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let kernel = r#"
__kernel void gid( 
            __global const IN_TYPE0 *in_data0,
            __global const RasterInfo *in_info0,
            __global OUT_TYPE0* out_data,
            __global const RasterInfo *out_info)            
{
    int idx = get_global_id(0) + get_global_id(1) * in_info0->size[0];
    out_data[idx] = idx;
}"#;

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_raster(RasterArgument::new(in0.raster_data_type()));
        cl_program.add_output_raster(RasterArgument::new(out.raster_data_type()));

        let mut compiled = cl_program.compile(kernel, "gid").unwrap();

        let mut runnable = compiled.runnable();
        runnable.set_input_raster(0, &in0).unwrap();
        runnable.set_output_raster(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        assert_eq!(
            out.get_i32_ref().unwrap().data_container,
            vec![0, 1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn points() {
        let input = TypedFeatureCollection::MultiPoint(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    vec![(0., 0.)],
                    vec![(1., 1.), (2., 2.)],
                    vec![(3., 3.)],
                ])
                .unwrap(),
                vec![
                    TimeInterval::new_unchecked(0, 1),
                    TimeInterval::new_unchecked(1, 2),
                    TimeInterval::new_unchecked(2, 3),
                ],
                HashMap::new(),
            )
            .unwrap(),
        );

        let mut out = FeatureCollection::<MultiPoint>::builder().batch_builder(3, 4);

        let kernel = r#"
__kernel void points( 
            __global const double2 *IN_POINT_COORDS0,
            __global const int *IN_POINT_OFFSETS0,
            __global double2 *OUT_POINT_COORDS0,
            __global int *OUT_POINT_OFFSETS0)            
{
    int idx = get_global_id(0);
    OUT_POINT_COORDS0[idx].x = IN_POINT_COORDS0[idx].x;
    OUT_POINT_COORDS0[idx].y = IN_POINT_COORDS0[idx].y + 1;
    OUT_POINT_OFFSETS0[idx] = IN_POINT_OFFSETS0[idx];
}"#;

        let mut cl_program = CLProgram::new(IterationType::VectorCoordinates);
        cl_program.add_input_features(VectorArgument::new(
            input.vector_data_type(),
            vec![],
            true,
            false,
        ));
        cl_program.add_output_features(VectorArgument::new(
            VectorDataType::MultiPoint,
            vec![],
            true,
            false,
        ));

        let mut compiled = cl_program.compile(kernel, "points").unwrap();

        let mut runnable = compiled.runnable();
        runnable.set_input_features(0, &input).unwrap();
        runnable.set_output_features(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        let collection = out.output.unwrap().get_points().unwrap();
        assert_eq!(
            collection.coordinates(),
            &[
                [0., 1.].into(),
                [1., 2.].into(),
                [2., 3.].into(),
                [3., 4.].into()
            ]
        );

        assert_eq!(collection.multipoint_offsets(), &[0, 1, 3, 4]);
    }

    #[test]
    #[allow(clippy::cast_ptr_alignment)]
    fn read_ocl_to_arrow_buffer() {
        let src = r#"
__kernel void nop(__global int* buffer) {
    buffer[get_global_id(0)] = get_global_id(0);
}
"#;

        let len = 4;

        let pro_que = ProQue::builder()
            .device(*DEVICE)
            .src(src)
            .dims(len)
            .build()
            .unwrap();

        let ocl_buffer = pro_que.create_buffer::<i32>().unwrap();

        let kernel = pro_que
            .kernel_builder("nop")
            .arg(&ocl_buffer)
            .build()
            .unwrap();

        unsafe {
            kernel.enq().unwrap();
        }

        let mut vec = vec![0; ocl_buffer.len()];
        ocl_buffer.read(&mut vec).enq().unwrap();

        assert_eq!(vec, &[0, 1, 2, 3]);

        let mut arrow_buffer = MutableBuffer::new(len * std::mem::size_of::<i32>());
        arrow_buffer
            .resize(len * std::mem::size_of::<i32>())
            .unwrap();

        let dest = unsafe {
            std::slice::from_raw_parts_mut(arrow_buffer.data_mut().as_ptr() as *mut i32, len)
        };

        ocl_buffer.read(dest).enq().unwrap();

        let arrow_buffer = arrow_buffer.freeze();

        let data = ArrayData::builder(DataType::Int32)
            .len(len)
            .add_buffer(arrow_buffer)
            .build();

        let array = Arc::new(PrimitiveArray::<Int32Type>::from(data));

        assert_eq!(array.value_slice(0, len), &[0, 1, 2, 3]);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn columns() {
        let input = TypedFeatureCollection::Data(
            DataCollection::from_data(
                vec![NoGeometry; 3],
                vec![
                    TimeInterval::new_unchecked(0, 1),
                    TimeInterval::new_unchecked(1, 2),
                    TimeInterval::new_unchecked(2, 3),
                ],
                [("foo".to_string(), FeatureData::Number(vec![0., 1., 2.]))]
                    .iter()
                    .cloned()
                    .collect(),
            )
            .unwrap(),
        );

        let mut builder = FeatureCollection::<NoGeometry>::builder();
        builder
            .add_column("foo".into(), FeatureDataType::Number)
            .unwrap();
        let mut out = builder.batch_builder(3, 4);

        let kernel = r#"
__kernel void columns( 
            __global const double *IN_POINT0_COLUMN_foo,
            __global double *OUT_POINT0_COLUMN_foo)            
{
    int idx = get_global_id(0);
    OUT_POINT0_COLUMN_foo[idx] = IN_POINT0_COLUMN_foo[idx] + 1;
}"#;

        let mut cl_program = CLProgram::new(IterationType::VectorFeatures);
        cl_program.add_input_features(VectorArgument::new(
            input.vector_data_type(),
            vec![ColumnArgument::new("foo".into(), FeatureDataType::Number)],
            false,
            false,
        ));
        cl_program.add_output_features(VectorArgument::new(
            VectorDataType::Data,
            vec![ColumnArgument::new("foo".into(), FeatureDataType::Number)],
            false,
            false,
        ));

        let mut compiled = cl_program.compile(kernel, "columns").unwrap();

        let mut runnable = compiled.runnable();
        runnable.set_input_features(0, &input).unwrap();
        runnable.set_output_features(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        match out.output.unwrap().get_data().unwrap().data("foo").unwrap() {
            FeatureDataRef::Number(numbers) => assert_eq!(numbers.as_ref(), &[1., 2., 3.]),
            _ => panic!(),
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn columns_null() {
        let input = TypedFeatureCollection::Data(
            DataCollection::from_data(
                vec![NoGeometry; 3],
                vec![
                    TimeInterval::new_unchecked(0, 1),
                    TimeInterval::new_unchecked(1, 2),
                    TimeInterval::new_unchecked(2, 3),
                ],
                [(
                    "foo".to_string(),
                    FeatureData::NullableNumber(vec![Some(0.), None, Some(2.)]),
                )]
                .iter()
                .cloned()
                .collect(),
            )
            .unwrap(),
        );

        let mut builder = FeatureCollection::<NoGeometry>::builder();
        builder
            .add_column("foo".into(), FeatureDataType::NullableNumber)
            .unwrap();
        let mut out = builder.batch_builder(3, 4);

        let kernel = r#"
__kernel void columns( 
            __global const double *IN_POINT0_COLUMN_foo,
            __global const int *IN_POINT0_NULLS_foo,
            __global double *OUT_POINT0_COLUMN_foo,
            __global int *OUT_POINT0_NULLS_foo)            
{
    int idx = get_global_id(0);
    if (IN_POINT0_NULLS_foo[idx]) {
        OUT_POINT0_COLUMN_foo[idx] = 1337;
        OUT_POINT0_NULLS_foo[idx] = -1;
    } else {
        OUT_POINT0_COLUMN_foo[idx] = 0;
        OUT_POINT0_NULLS_foo[idx] = 0;
    }
}"#;

        let mut cl_program = CLProgram::new(IterationType::VectorFeatures);
        cl_program.add_input_features(VectorArgument::new(
            input.vector_data_type(),
            vec![ColumnArgument::new(
                "foo".into(),
                FeatureDataType::NullableNumber,
            )],
            false,
            false,
        ));
        cl_program.add_output_features(VectorArgument::new(
            VectorDataType::Data,
            vec![ColumnArgument::new(
                "foo".into(),
                FeatureDataType::NullableNumber,
            )],
            false,
            false,
        ));

        let mut compiled = cl_program.compile(kernel, "columns").unwrap();

        let mut runnable = compiled.runnable();
        runnable.set_input_features(0, &input).unwrap();
        runnable.set_output_features(0, &mut out).unwrap();
        compiled.run(runnable).unwrap();

        match out.output.unwrap().get_data().unwrap().data("foo").unwrap() {
            FeatureDataRef::NullableNumber(numbers) => {
                assert_eq!(numbers.as_ref(), &[0., 1337., 0.]);
                assert_eq!(numbers.nulls().as_slice(), &[true, false, true])
            }
            _ => panic!(),
        }
    }
}
