use geoengine_datatypes::raster::RasterProperties;
use geoengine_datatypes::{
    dataset::{DataId, DatasetId},
    primitives::{Measurement, TimeInterval},
    raster::{Grid, RasterDataType, RasterTile2D},
    spatial_reference::SpatialReference,
    util::{test::TestDefault, Identifier},
};
use geoengine_operators::{
    engine::{RasterOperator, RasterResultDescriptor, SingleRasterSource},
    mock::{MockRasterSource, MockRasterSourceParams},
    processing::{
        AggregateFunctionParams, NeighborhoodAggregate, NeighborhoodAggregateParams,
        NeighborhoodParams,
    },
    source::{GdalSource, GdalSourceParameters},
};

const N: usize = 100;

/// This benchmark compares different serialization formats for operator graph names.
/// It is used to determine the best format for the cache.
fn main() {
    let o1 = GdalSource {
        params: GdalSourceParameters {
            data: DataId::Internal {
                dataset_id: DatasetId::new(),
            },
        },
    }
    .boxed();

    let o2 = NeighborhoodAggregate {
        params: NeighborhoodAggregateParams {
            neighborhood: NeighborhoodParams::WeightsMatrix {
                weights: vec![vec![1., 2., 3.], vec![4., 5., 6.], vec![7., 8., 9.]],
            },
            aggregate_function: AggregateFunctionParams::Sum,
        },
        sources: SingleRasterSource {
            raster: GdalSource {
                params: GdalSourceParameters {
                    data: DataId::Internal {
                        dataset_id: DatasetId::new(),
                    },
                },
            }
            .boxed(),
        },
    }
    .boxed();

    let o3 = MockRasterSource {
        params: MockRasterSourceParams::<u8> {
            data: vec![
                RasterTile2D {
                    time: TimeInterval::new_unchecked(1, 1),
                    tile_position: [-1, 0].into(),
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                        .unwrap()
                        .into(),
                    properties: RasterProperties::default(),
                },
                RasterTile2D {
                    time: TimeInterval::new_unchecked(1, 1),
                    tile_position: [-1, 1].into(),
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12])
                        .unwrap()
                        .into(),
                    properties: RasterProperties::default(),
                },
                RasterTile2D {
                    time: TimeInterval::new_unchecked(2, 2),
                    tile_position: [-1, 0].into(),
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([3, 2].into(), vec![13, 14, 15, 16, 17, 18])
                        .unwrap()
                        .into(),
                    properties: RasterProperties::default(),
                },
                RasterTile2D {
                    time: TimeInterval::new_unchecked(2, 2),
                    tile_position: [-1, 1].into(),
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([3, 2].into(), vec![19, 20, 21, 22, 23, 24])
                        .unwrap()
                        .into(),
                    properties: RasterProperties::default(),
                },
            ],
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                time: None,
                bbox: None,
                resolution: None,
            },
        },
    }
    .boxed();

    let os = [&o1, &o2, &o3];
    let on = ["gdal", "agg", "mock"];

    let ss: Vec<fn(&Box<dyn RasterOperator>)> = vec![json, cbor, rmp, bincode];
    let sn = vec!["json", "cbor", "rmp", "bincode"];

    println!("format, operator, time");
    for (op, op_name) in os.iter().zip(on) {
        for (ser, ser_name) in ss.iter().zip(sn.iter()) {
            let start = std::time::Instant::now();
            for _ in 0..N {
                ser(op);
            }
            println!("{}, {}: {:?}", ser_name, op_name, start.elapsed());
        }
    }
}

#[allow(clippy::borrowed_box)]
fn json(o: &Box<dyn RasterOperator>) {
    let _ = serde_json::to_string(o);
}

#[allow(clippy::borrowed_box)]
fn cbor(o: &Box<dyn RasterOperator>) {
    let vec = Vec::new();

    let _ = ciborium::into_writer(o, vec);
}

#[allow(clippy::borrowed_box)]
fn rmp(o: &Box<dyn RasterOperator>) {
    let _ = rmp_serde::to_vec(o).unwrap();
}

#[allow(clippy::borrowed_box)]
fn bincode(o: &Box<dyn RasterOperator>) {
    let _: Vec<u8> = bincode::serialize(o).unwrap();
}
