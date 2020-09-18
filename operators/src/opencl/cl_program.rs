use crate::error;
use crate::util::Result;
use geoengine_datatypes::collections::{TypedFeatureCollection, VectorDataType};
use geoengine_datatypes::primitives::{Coordinate2D, FeatureDataType};
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
use ocl::prm::{cl_double, cl_uint, cl_ushort};
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
    Raster,
    Vector,
}

// TODO: remove this struct if only data type is relevant and pass it directly
#[derive(PartialEq, Clone, Copy, Debug)]
pub struct RasterArgument {
    pub data_type: RasterDataType,
}

impl RasterArgument {
    pub fn new(data_type: RasterDataType) -> Self {
        Self { data_type }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct VectorArgument {
    pub vector_type: VectorDataType,
    // TODO: merge columns and types into one type
    pub columns: Vec<String>,
    pub column_types: Vec<FeatureDataType>,
}

impl VectorArgument {
    pub fn new(
        vector_type: VectorDataType,
        columns: Vec<String>,
        column_types: Vec<FeatureDataType>,
    ) -> Self {
        Self {
            vector_type,
            columns,
            column_types,
        }
    }
}

/// Specifies in and output types of CL program and compiles the source into a reusable `CompiledCLProgram`
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

    // fn add_points(points: &MultiPointCollection) {}

    fn raster_data_type_to_cl(data_type: RasterDataType) -> String {
        // TODO: maybe attach this info to raster data type together with gdal data type etc
        match data_type {
            RasterDataType::U8 => "uchar",
            RasterDataType::U16 => "ushort",
            RasterDataType::U32 => "uint",
            RasterDataType::U64 => "ulong",
            RasterDataType::I8 => "char",
            RasterDataType::I16 => "short",
            RasterDataType::I32 => "int",
            RasterDataType::I64 => "long",
            RasterDataType::F32 => "float",
            RasterDataType::F64 => "double",
        }
        .into()
    }

    pub fn add_input_features(&mut self, vector_type: VectorArgument) {
        self.input_features.push(vector_type);
    }

    pub fn add_output_features(&mut self, vector_type: VectorArgument) {
        self.output_features.push(vector_type);
    }

    fn create_type_definitions(&self) -> String {
        let mut s = String::new();

        if self.input_rasters.len() + self.output_rasters.len() == 0 {
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
            s += &format!(
                "typedef {} IN_TYPE{};\n",
                Self::raster_data_type_to_cl(raster.data_type),
                idx
            );

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
            s += &format!(
                "typedef {} OUT_TYPE{};\n",
                Self::raster_data_type_to_cl(raster.data_type),
                idx
            );
        }

        s
    }

    pub fn compile(self, source: &str, kernel_name: &str) -> Result<CompiledCLProgram> {
        ensure!(
            (self.iteration_type == IterationType::Vector && !self.input_features.is_empty())
                || (self.iteration_type == IterationType::Raster
                    && !self.input_rasters.is_empty()
                    && !self.output_rasters.is_empty()),
            error::CLInvalidInputsForIterationType
        );

        let typedefs = self.create_type_definitions();

        // TODO: add code for pixel to world

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

        // TODO: create kernel builder here once it is cloneable

        // TODO: feature collections

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
enum OutputBuffer {
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

pub struct CLProgramParameters<'a> {
    input_raster_types: Vec<RasterArgument>,
    output_raster_types: Vec<RasterArgument>,
    input_rasters: Vec<Option<&'a TypedRaster2D>>,
    output_rasters: Vec<Option<&'a mut TypedRaster2D>>,
    input_feature_types: Vec<VectorArgument>,
    output_feature_types: Vec<VectorArgument>,
    input_features: Vec<Option<&'a TypedFeatureCollection>>,
    output_features: Vec<Option<&'a mut TypedFeatureCollection>>,
}

impl<'a> CLProgramParameters<'a> {
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

        let mut iter = self.input_feature_types[idx]
            .columns
            .iter()
            .zip(self.input_feature_types[idx].column_types.iter());
        call_generic_features!(features, f => iter.all(|(n, t)| f.column_type(n).map_or(false, |to| to == *t)));

        self.input_features[idx] = Some(features);
        Ok(())
    }

    pub fn set_output_features(
        &mut self,
        idx: usize,
        features: &'a mut TypedFeatureCollection,
    ) -> Result<()> {
        ensure!(
            idx < self.output_feature_types.len(),
            error::CLProgramInvalidFeaturesIndex
        );
        ensure!(
            features.vector_data_type() == self.output_feature_types[idx].vector_type,
            error::CLProgramInvalidVectorDataType
        );

        let mut iter = self.output_feature_types[idx]
            .columns
            .iter()
            .zip(self.output_feature_types[idx].column_types.iter());
        call_generic_features!(features, f => iter.all(|(n, t)| f.column_type(n).map_or(false, |to| to == *t)));

        self.output_features[idx] = Some(features);
        Ok(())
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
    raster_output_buffers: Vec<OutputBuffer>,
    input_feature_types: Vec<VectorArgument>,
    output_feature_types: Vec<VectorArgument>,
    coordinates_output_buffers: Vec<Buffer<Coordinate2D>>,
    point_offsets_output_buffers: Vec<Buffer<i32>>,
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
            raster_output_buffers: vec![],
            input_feature_types,
            output_feature_types,
            coordinates_output_buffers: vec![],
            point_offsets_output_buffers: vec![],
        }
    }

    pub fn params<'b>(&self) -> CLProgramParameters<'b> {
        CLProgramParameters::new(
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

    fn set_feature_arguments(
        &mut self,
        kernel: &Kernel,
        params: &CLProgramParameters,
    ) -> Result<()> {
        ensure!(
            params.input_features.iter().all(Option::is_some),
            error::CLProgramUnspecifiedFeatures
        );

        for (idx, features) in params.input_features.iter().enumerate() {
            let features = features.expect("checked");

            match features {
                TypedFeatureCollection::Data(_) => {
                    // no geo
                }
                TypedFeatureCollection::MultiPoint(points) => {
                    let coordinates = points.coordinates();
                    let buffer = Buffer::builder()
                        .queue(kernel.default_queue().expect("expect").clone())
                        .len(coordinates.len())
                        .copy_host_slice(coordinates)
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
                | TypedFeatureCollection::MultiPolygon(_) => todo!(),
            }

            call_generic_features!(features, features => {
                // TODO: columns buffers
            });
        }

        for (idx, features) in params.output_features.iter().enumerate() {
            let features = features.as_ref().expect("checked");

            match features {
                TypedFeatureCollection::Data(_) => {
                    // no geo
                }
                TypedFeatureCollection::MultiPoint(points) => {
                    let coordinates = points.coordinates();
                    let buffer = Buffer::<Coordinate2D>::builder()
                        .queue(kernel.default_queue().expect("expect").clone())
                        .len(coordinates.len())
                        .build()?;

                    kernel.set_arg(format!("OUT_POINT_COORDS{}", idx), &buffer)?;
                    self.coordinates_output_buffers.push(buffer);

                    let coordinates_offsets = points.multipoint_offsets();
                    let buffer = Buffer::<i32>::builder()
                        .queue(kernel.default_queue().expect("expect").clone())
                        .len(coordinates_offsets.len())
                        .build()?;

                    kernel.set_arg(format!("OUT_POINT_OFFSETS{}", idx), &buffer)?;
                    self.point_offsets_output_buffers.push(buffer);
                }
                TypedFeatureCollection::MultiLineString(_)
                | TypedFeatureCollection::MultiPolygon(_) => todo!(),
            }

            call_generic_features!(features, features => {
                // TODO: columns buffers
            });
        }

        Ok(())
    }

    fn set_raster_arguments(
        &mut self,
        kernel: &Kernel,
        params: &CLProgramParameters,
    ) -> Result<()> {
        ensure!(
            params.input_rasters.iter().all(Option::is_some),
            error::CLProgramUnspecifiedRaster
        );
        self.raster_output_buffers.clear();

        for (idx, raster) in params.input_rasters.iter().enumerate() {
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

        for (idx, raster) in params.output_rasters.iter().enumerate() {
            let raster = raster.as_ref().expect("checked");
            call_generic_raster2d_ext!(raster, OutputBuffer, (raster, e) => {
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

    fn work_size(&self, params: &CLProgramParameters) -> SpatialDims {
        match self.iteration_type {
            IterationType::Raster => call_generic_raster2d!(params.output_rasters[0].as_ref()
                .expect("checked"), raster => SpatialDims::Two(raster.dimension().size_of_x_axis(), raster.dimension().size_of_y_axis())),
            IterationType::Vector => unimplemented!(),
        }
    }

    pub fn run(&mut self, params: CLProgramParameters) -> Result<()> {
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

        self.set_raster_arguments(&kernel, &params)?;

        self.set_feature_arguments(&kernel, &params)?;

        let dims = self.work_size(&params);
        unsafe {
            kernel.cmd().global_work_size(dims).enq()?;
        }

        self.read_output_buffers(params)?;

        Ok(())
    }

    fn read_output_buffers(&mut self, mut params: CLProgramParameters) -> Result<()> {
        for (output_buffer, output_raster) in self
            .raster_output_buffers
            .iter()
            .zip(params.output_rasters.iter_mut())
        {
            match (output_buffer, output_raster) {
                (OutputBuffer::U8(buffer), Some(TypedRaster2D::U8(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (OutputBuffer::U16(buffer), Some(TypedRaster2D::U16(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (OutputBuffer::U32(buffer), Some(TypedRaster2D::U32(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (OutputBuffer::U64(buffer), Some(TypedRaster2D::U64(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (OutputBuffer::I8(buffer), Some(TypedRaster2D::I8(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (OutputBuffer::I16(buffer), Some(TypedRaster2D::I16(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (OutputBuffer::I32(buffer), Some(TypedRaster2D::I32(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (OutputBuffer::I64(buffer), Some(TypedRaster2D::I64(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (OutputBuffer::F32(buffer), Some(TypedRaster2D::F32(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                (OutputBuffer::F64(buffer), Some(TypedRaster2D::F64(raster))) => {
                    buffer.read(raster.data_container.as_mut_slice()).enq()?;
                }
                _ => unreachable!(),
            };
        }

        for ((features, coordinates_buffer), offsets_buffer) in params
            .output_features
            .iter()
            .zip(self.coordinates_output_buffers.iter())
            .zip(self.point_offsets_output_buffers.iter())
        {
            match features.as_ref().expect("must exist") {
                TypedFeatureCollection::Data(_) => {
                    // no geo
                }
                TypedFeatureCollection::MultiPoint(points) => {
                    // TODO: don't need to initialize
                    // TODO: better way of determining length of coordinates
                    let mut coordinates: Vec<Coordinate2D> =
                        vec![Default::default(); points.coordinates().len()];
                    coordinates_buffer.read(coordinates.as_mut_slice()).enq()?;

                    let mut offsets: Vec<i32> =
                        vec![Default::default(); points.multipoint_offsets().len()];
                    offsets_buffer.read(offsets.as_mut_slice()).enq()?;

                    // TODO: fill output features collection
                }
                TypedFeatureCollection::MultiLineString(_)
                | TypedFeatureCollection::MultiPolygon(_) => todo!(),
            }
        }
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
            match features.vector_type {
                VectorDataType::Data => {
                    // no geo
                }
                VectorDataType::MultiPoint => {
                    kernel.arg_named(format!("IN_POINT_COORDS{}", idx), None::<&Buffer<f64>>);
                    kernel.arg_named(format!("IN_POINT_OFFSETS{}", idx), None::<&Buffer<i32>>);
                }
                VectorDataType::MultiLineString | VectorDataType::MultiPolygon => todo!(),
            }

            // TODO: columns
        }

        for (idx, features) in self.output_feature_types.iter().enumerate() {
            match features.vector_type {
                VectorDataType::Data => {
                    // no geo
                }
                VectorDataType::MultiPoint => {
                    kernel.arg_named(format!("OUT_POINT_COORDS{}", idx), None::<&Buffer<f64>>);
                    kernel.arg_named(format!("OUT_POINT_OFFSETS{}", idx), None::<&Buffer<i32>>);
                }
                VectorDataType::MultiLineString | VectorDataType::MultiPolygon => todo!(),
            }

            // TODO: columns
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::{MultiPoint, TimeInterval};
    use geoengine_datatypes::raster::Raster2D;
    use std::collections::HashMap;

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

        let mut params = compiled.params();
        params.set_input_raster(0, &in0).unwrap();
        params.set_input_raster(1, &in1).unwrap();
        params.set_output_raster(0, &mut out).unwrap();
        compiled.run(params).unwrap();

        assert_eq!(
            out.get_i32_ref().unwrap().data_container,
            vec![8, 10, 12, 14, 16, 18]
        );

        let mut params = compiled.params();
        params.set_input_raster(0, &in0).unwrap();
        params.set_input_raster(1, &in0).unwrap();
        params.set_output_raster(0, &mut out).unwrap();
        compiled.run(params).unwrap();

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

        let mut params = compiled.params();
        params.set_input_raster(0, &in0).unwrap();
        params.set_input_raster(1, &in1).unwrap();
        params.set_output_raster(0, &mut out).unwrap();
        compiled.run(params).unwrap();

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

        let mut params = compiled.params();
        params.set_input_raster(0, &in0).unwrap();
        params.set_output_raster(0, &mut out).unwrap();
        compiled.run(params).unwrap();

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

        let mut params = compiled.params();
        params.set_input_raster(0, &in0).unwrap();
        params.set_output_raster(0, &mut out).unwrap();
        compiled.run(params).unwrap();

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

        let mut params = compiled.params();
        params.set_input_raster(0, &in0).unwrap();
        params.set_output_raster(0, &mut out).unwrap();
        compiled.run(params).unwrap();

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

        let mut params = compiled.params();
        params.set_input_raster(0, &in0).unwrap();
        params.set_output_raster(0, &mut out).unwrap();
        compiled.run(params).unwrap();

        assert_eq!(
            out.get_i32_ref().unwrap().data_container,
            vec![0, 1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn points() {
        let input = TypedFeatureCollection::MultiPoint(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)], vec![(2., 2.)]]).unwrap(),
                vec![
                    TimeInterval::new_unchecked(0, 1),
                    TimeInterval::new_unchecked(1, 2),
                    TimeInterval::new_unchecked(2, 3),
                ],
                HashMap::new(),
            )
            .unwrap(),
        );

        // TODO: avoid creating a output collection
        let mut out = TypedFeatureCollection::MultiPoint(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![vec![(0., 0.)], vec![(1., 1.)], vec![(2., 2.)]]).unwrap(),
                vec![
                    TimeInterval::new_unchecked(0, 1),
                    TimeInterval::new_unchecked(1, 2),
                    TimeInterval::new_unchecked(2, 3),
                ],
                HashMap::new(),
            )
            .unwrap(),
        );

        let kernel = r#"
__kernel void points( 
            __global const double2 *IN_POINT_COORDS0,
            __global const int *IN_POINT_OFFSETS0,
            __global const double2 *OUT_POINT_COORDS0,
            __global const int *OUT_POINT_OFFSETS0)            
{
    int idx = get_global_id(0);
    OUT_POINT_COORD0[idx] = IN_POINT_COORDS[idx] + 1;
}"#;

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_features(VectorArgument::new(
            input.vector_data_type(),
            vec![],
            vec![],
        ));
        cl_program.add_input_features(VectorArgument::new(out.vector_data_type(), vec![], vec![]));

        let mut compiled = cl_program.compile(kernel, "points").unwrap();

        let mut params = compiled.params();
        params.set_input_features(0, &input).unwrap();
        params.set_output_features(0, &mut out).unwrap();
        compiled.run(params).unwrap();

        assert_eq!(
            out.get_points().unwrap().coordinates(),
            &[[1., 2.].into(), [2., 3.].into(), [3., 4.].into()]
        );
    }
}
