use crate::error;
use crate::util::Result;
use geoengine_datatypes::call_generic_raster2d;
use geoengine_datatypes::raster::{
    DynamicRasterDataType, Pixel, Raster2D, RasterDataType, TypedRaster2D,
};
use ocl::builders::{ContextBuilder, KernelBuilder, ProgramBuilder};
use ocl::{Buffer, Device, Kernel, MemFlags, Platform, Queue};
use snafu::ensure;
use snafu::ResultExt;

/// Whether the kernel iterates over pixels or features
#[derive(PartialEq, Clone, Copy, Debug)]
pub enum IterationType {
    Raster,
    Vector,
}

/// Specifies in and output types of CL program and compiles the source into a reusable `CompiledCLProgram`
pub struct CLProgram {
    input_rasters: Vec<RasterDataType>,
    output_rasters: Vec<RasterDataType>,
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

    pub fn add_input_raster(&mut self, raster: RasterDataType) {
        self.input_rasters.push(raster);
    }

    pub fn add_output_raster(&mut self, raster: RasterDataType) {
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

    fn add_buffer_placeholder(
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

        for (idx, raster) in self.input_rasters.iter().enumerate() {
            s += &format!(
                "typedef {} IN_TYPE{};\n",
                Self::raster_data_type_to_cl(*raster),
                idx
            );
        }

        for (idx, raster) in self.input_rasters.iter().enumerate() {
            s += &format!(
                "typedef {} OUT_TYPE{};\n",
                Self::raster_data_type_to_cl(*raster),
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

        let ctx = ContextBuilder::new().build().context(error::OCL)?; // TODO: make configurable

        let program = ProgramBuilder::new()
            .src(typedefs)
            .src(source)
            .build(&ctx)
            .context(error::OCL)?;

        let platform = Platform::default(); // TODO: make configurable
        let device = Device::first(platform).context(error::OCL)?;

        let queue = Queue::new(&ctx, device, None).context(error::OCL)?;

        let mut kernel = KernelBuilder::new();
        kernel.queue(queue).program(&program).name(kernel_name);

        for (idx, raster) in self.input_rasters.iter().enumerate() {
            Self::add_buffer_placeholder(&mut kernel, format!("IN{}", idx), *raster);
        }

        for (idx, raster) in self.output_rasters.iter().enumerate() {
            Self::add_buffer_placeholder(&mut kernel, format!("OUT{}", idx), *raster);
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
    I32(Buffer<i32>),
}

pub struct CLProgramParameters<'a> {
    input_raster_types: Vec<RasterDataType>,
    output_raster_types: Vec<RasterDataType>,
    input_rasters: Vec<Option<&'a TypedRaster2D>>,
    output_rasters: Vec<Option<&'a mut TypedRaster2D>>,
}

impl<'a> CLProgramParameters<'a> {
    fn new(
        input_raster_types: Vec<RasterDataType>,
        output_raster_types: Vec<RasterDataType>,
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
            raster.raster_data_type() == self.input_raster_types[idx],
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
            raster.raster_data_type() == self.output_raster_types[idx],
            error::CLProgramInvalidRasterDataType
        );
        self.output_rasters[idx] = Some(raster);
        Ok(())
    }
}

/// Allows running kernels on different inputs and outputs
pub struct CompiledCLProgram {
    kernel: Kernel,
    iteration_type: IterationType,
    input_raster_types: Vec<RasterDataType>,
    output_raster_types: Vec<RasterDataType>,
    output_buffers: Vec<OutputBuffer>,
}

impl CompiledCLProgram {
    pub fn new(
        kernel: Kernel,
        iteration_type: IterationType,
        input_raster_types: Vec<RasterDataType>,
        output_raster_types: Vec<RasterDataType>,
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

    fn create_output_buffer<T>(&self, raster: &Raster2D<T>) -> Result<Buffer<T>>
    where
        T: Pixel + ocl::OclPrm,
    {
        Buffer::<T>::builder()
            .queue(self.kernel.default_queue().expect("checked").clone())
            .len(raster.data_container.len())
            .build()
            .context(error::OCL)
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
            let buffer = Buffer::builder()
                .queue(self.kernel.default_queue().expect("checked").clone())
                .flags(MemFlags::new().read_only())
                .len(raster.data_container.len())
                .copy_host_slice(&raster.data_container)
                .build().context(error::OCL)?;

                self.kernel.set_arg(format!("IN{}",idx), buffer).context(error::OCL)?;
            });
        }

        for (idx, raster) in params.output_rasters.iter().enumerate() {
            let raster = raster.as_ref().expect("checked");
            // TODO: missing types
            match raster {
                TypedRaster2D::U8(_raster) => {}
                TypedRaster2D::U16(_raster) => {}
                TypedRaster2D::U32(_raster) => {}
                TypedRaster2D::U64(_raster) => {}
                TypedRaster2D::I8(_raster) => {}
                TypedRaster2D::I16(_raster) => {}
                TypedRaster2D::I32(raster) => {
                    let buffer = self.create_output_buffer(raster)?;
                    self.kernel
                        .set_arg(format!("OUT{}", idx), &buffer)
                        .context(error::OCL)?;
                    self.output_buffers.push(OutputBuffer::I32(buffer));
                }
                TypedRaster2D::I64(_raster) => {}
                TypedRaster2D::F32(_raster) => {}
                TypedRaster2D::F64(_raster) => {}
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
                (OutputBuffer::I32(buffer), Some(TypedRaster2D::I32(raster))) => {
                    buffer
                        .read(raster.data_container.as_mut_slice())
                        .enq()
                        .context(error::OCL)?;
                }
                _ => unimplemented!(),
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
        let raster_a = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![1, 2, 3, 4, 5, 6],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let raster_b = TypedRaster2D::I32(
            Raster2D::new(
                [3, 2].into(),
                vec![7, 8, 9, 10, 11, 12],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );

        let mut raster_res = TypedRaster2D::I32(
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
            __global IN_TYPE0* a,
            __global IN_TYPE1* b,
            __global OUT_TYPE0* res)
            
{
    uint const idx = get_global_id(0);
    res[idx] = a[idx] + b[idx];
}"#;

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_raster(raster_a.raster_data_type());
        cl_program.add_input_raster(raster_b.raster_data_type());
        cl_program.add_output_raster(raster_res.raster_data_type());

        let mut compiled = cl_program.compile(kernel, "add").unwrap();

        let mut params = compiled.params();
        params.set_input_raster(0, &raster_a).unwrap();
        params.set_input_raster(1, &raster_b).unwrap();
        params.set_output_raster(0, &mut raster_res).unwrap();
        compiled.run(params).unwrap();

        assert_eq!(
            raster_res.get_i32_ref().unwrap().data_container,
            vec![8, 10, 12, 14, 16, 18]
        );

        let mut params = compiled.params();
        params.set_input_raster(0, &raster_a).unwrap();
        params.set_input_raster(1, &raster_a).unwrap();
        params.set_output_raster(0, &mut raster_res).unwrap();
        compiled.run(params).unwrap();

        assert_eq!(
            raster_res.get_i32().unwrap().data_container,
            vec![2, 4, 6, 8, 10, 12]
        );
    }
}
