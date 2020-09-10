use crate::error;
use crate::util::Result;
use geoengine_datatypes::call_generic_raster2d;
use geoengine_datatypes::raster::{
    DynamicRasterDataType, Pixel, Raster2D, RasterDataType, TypedRaster2D,
};
use num_traits::AsPrimitive;
use ocl::builders::{KernelBuilder, ProgramBuilder};
use ocl::prm::{cl_double, cl_uint, cl_ushort};
use ocl::{Buffer, Context, Device, Kernel, MemFlags, OclPrm, Platform, Queue};
use snafu::ensure;
use snafu::ResultExt;

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

/// Specifies in and output types of CL program and compiles the source into a reusable `CompiledCLProgram`
pub struct CLProgram {
    input_rasters: Vec<RasterArgument>,
    output_rasters: Vec<RasterArgument>,
    iteration_type: IterationType,
}

impl CLProgram {
    pub fn new(iteration_type: IterationType) -> Self {
        Self {
            input_rasters: vec![],
            output_rasters: vec![],
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

    fn create_type_definitions(&self) -> String {
        let mut s = String::new();

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
        // TODO: check feature inputs
        ensure!(
            (self.iteration_type == IterationType::Vector && false)
                || (self.iteration_type == IterationType::Raster
                    && !self.input_rasters.is_empty()
                    && !self.output_rasters.is_empty()),
            error::CLInvalidInputsForIterationType
        );

        let typedefs = self.create_type_definitions();

        // TODO: add raster meta data info (NODATA etc.)

        // TODO: add code for pixel to world

        let platform = Platform::default(); // TODO: make configurable

        // TODO: fails for concurrent access, see https://github.com/cogciprocate/ocl/issues/189
        let device = Device::first(platform).context(error::OCL)?; // TODO: make configurable

        let ctx = Context::builder()
            .platform(platform)
            .devices(device)
            .build()
            .context(error::OCL)?; // TODO: make configurable

        let program = ProgramBuilder::new()
            .src(typedefs)
            .src(source)
            .build(&ctx)
            .context(error::OCL)?;

        let queue = Queue::new(&ctx, device, None).context(error::OCL)?;

        let mut kernel = KernelBuilder::new();
        kernel.queue(queue).program(&program).name(kernel_name);

        for (idx, raster) in self.input_rasters.iter().enumerate() {
            Self::add_data_buffer_placeholder(&mut kernel, format!("IN{}", idx), raster.data_type);
            kernel.arg_named(format!("IN_INFO{}", idx), None::<&Buffer<RasterInfo>>);
        }

        for (idx, raster) in self.output_rasters.iter().enumerate() {
            Self::add_data_buffer_placeholder(&mut kernel, format!("OUT{}", idx), raster.data_type);
            kernel.arg_named(format!("OUT_INFO{}", idx), None::<&Buffer<RasterInfo>>);
        }

        kernel.build().context(error::OCL)?;

        // TODO: raster meta data

        // TODO: feature collections

        let kernel = kernel.build().context(error::OCL)?;

        Ok(CompiledCLProgram::new(
            kernel,
            self.iteration_type,
            self.input_rasters,
            self.output_rasters,
        ))
    }
}

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
}

impl<'a> CLProgramParameters<'a> {
    fn new(
        input_raster_types: Vec<RasterArgument>,
        output_raster_types: Vec<RasterArgument>,
    ) -> Self {
        let mut v = Vec::new();
        v.resize_with(output_raster_types.len(), || None);

        Self {
            input_rasters: vec![None; input_raster_types.len()],
            output_rasters: v,
            input_raster_types,
            output_raster_types,
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
        // TODO: extract information from raster
        Self {
            size: [0, 0, 0],
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
pub struct CompiledCLProgram {
    kernel: Kernel,
    iteration_type: IterationType,
    input_raster_types: Vec<RasterArgument>,
    output_raster_types: Vec<RasterArgument>,
    output_buffers: Vec<OutputBuffer>,
}

impl CompiledCLProgram {
    pub fn new(
        kernel: Kernel,
        iteration_type: IterationType,
        input_raster_types: Vec<RasterArgument>,
        output_raster_types: Vec<RasterArgument>,
    ) -> Self {
        Self {
            kernel,
            iteration_type,
            input_raster_types,
            output_raster_types,
            output_buffers: vec![],
        }
    }

    pub fn params<'a>(&self) -> CLProgramParameters<'a> {
        CLProgramParameters::new(
            self.input_raster_types.clone(),
            self.output_raster_types.clone(),
        )
    }

    fn add_output_buffer<T>(
        &mut self,
        idx: usize,
        raster: &Raster2D<T>,
        f: fn(Buffer<T>) -> OutputBuffer,
    ) -> Result<()>
    where
        T: Pixel + OclPrm,
    {
        let buffer = Buffer::<T>::builder()
            .queue(self.kernel.default_queue().expect("checked").clone())
            .len(raster.data_container.len())
            .build()
            .context(error::OCL)?;

        self.kernel
            .set_arg(format!("OUT{}", idx), &buffer)
            .context(error::OCL)?;

        self.output_buffers.push(f(buffer));

        let info_buffer = Buffer::builder()
            .queue(self.kernel.default_queue().expect("checked").clone())
            .flags(MemFlags::new().read_only())
            .len(1)
            .copy_host_slice(&[RasterInfo::from_raster(&raster)])
            .build()
            .context(error::OCL)?;
        self.kernel
            .set_arg(format!("OUT_INFO{}", idx), info_buffer)
            .context(error::OCL)?;

        Ok(())
    }

    fn set_arguments(&mut self, params: &CLProgramParameters) -> Result<()> {
        ensure!(
            params.input_rasters.iter().all(Option::is_some),
            error::CLProgramUnspecifiedRaster
        );
        self.output_buffers.clear();

        for (idx, raster) in params.input_rasters.iter().enumerate() {
            let raster = raster.expect("checked");
            call_generic_raster2d!(raster, raster => {
                let data_buffer = Buffer::builder()
                .queue(self.kernel.default_queue().expect("checked").clone())
                .flags(MemFlags::new().read_only())
                .len(raster.data_container.len())
                .copy_host_slice(&raster.data_container)
                .build().context(error::OCL)?;
                self.kernel.set_arg(format!("IN{}",idx), data_buffer).context(error::OCL)?;

                let info_buffer = Buffer::builder()
                .queue(self.kernel.default_queue().expect("checked").clone())
                .flags(MemFlags::new().read_only())
                .len(1)
                .copy_host_slice(&[RasterInfo::from_raster(&raster)])
                .build().context(error::OCL)?;
                self.kernel.set_arg(format!("IN_INFO{}",idx), info_buffer).context(error::OCL)?;
            });
        }

        for (idx, raster) in params.output_rasters.iter().enumerate() {
            let raster = raster.as_ref().expect("checked");
            match raster {
                TypedRaster2D::U8(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::U8)?
                }
                TypedRaster2D::U16(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::U16)?
                }
                TypedRaster2D::U32(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::U32)?
                }
                TypedRaster2D::U64(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::U64)?
                }
                TypedRaster2D::I8(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::I8)?
                }
                TypedRaster2D::I16(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::I16)?
                }
                TypedRaster2D::I32(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::I32)?
                }
                TypedRaster2D::I64(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::I64)?
                }
                TypedRaster2D::F32(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::F32)?
                }
                TypedRaster2D::F64(raster) => {
                    self.add_output_buffer(idx, raster, OutputBuffer::F64)?
                }
            }
        }

        Ok(())
    }

    fn work_size(&self, params: &CLProgramParameters) -> usize {
        match self.iteration_type {
            IterationType::Raster => {
                call_generic_raster2d!(params.input_rasters[0].expect("checked"), raster => raster.data_container.len())
            }
            IterationType::Vector => unimplemented!(),
        }
    }

    pub fn run(&mut self, mut params: CLProgramParameters) -> Result<()> {
        self.set_arguments(&params)?;

        unsafe {
            self.kernel
                .cmd()
                .global_work_size(self.work_size(&params))
                .enq()
                .context(error::OCL)?;
        }

        for (output_buffer, output_raster) in self
            .output_buffers
            .iter()
            .zip(params.output_rasters.iter_mut())
        {
            match (output_buffer, output_raster) {
                (OutputBuffer::U8(buffer), Some(TypedRaster2D::U8(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                (OutputBuffer::U16(buffer), Some(TypedRaster2D::U16(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                (OutputBuffer::U32(buffer), Some(TypedRaster2D::U32(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                (OutputBuffer::U64(buffer), Some(TypedRaster2D::U64(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                (OutputBuffer::I8(buffer), Some(TypedRaster2D::I8(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                (OutputBuffer::I16(buffer), Some(TypedRaster2D::I16(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                (OutputBuffer::I32(buffer), Some(TypedRaster2D::I32(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                (OutputBuffer::I64(buffer), Some(TypedRaster2D::I64(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                (OutputBuffer::F32(buffer), Some(TypedRaster2D::F32(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                (OutputBuffer::F64(buffer), Some(TypedRaster2D::F64(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                _ => unreachable!(),
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::raster::Raster2D;

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
    uint const idx = get_global_id(0);
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
    uint const idx = get_global_id(0);
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
    uint const idx = get_global_id(0);
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
    uint const idx = get_global_id(0);
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
    uint const idx = get_global_id(0);
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
}
