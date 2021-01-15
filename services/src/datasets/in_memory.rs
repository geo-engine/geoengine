use super::storage::DataSetDB;
use crate::datasets::listing::{
    DataSetListOptions, DataSetListing, DataSetProvider, TypedDataSetProvider,
};
use crate::datasets::storage::{
    AddDataSet, AddDataSetProvider, DataSet, DataSetProviderListOptions, DataSetProviderListing,
    ImportDataSet, RasterLoadingInfo, UserDataSetProviderPermission, VectorLoadingInfo,
};
use crate::datasets::storage::{DataSetPermission, UserDataSetPermission};
use crate::error;
use crate::error::Error::UnknownDataSetProviderId;
use crate::error::{Error, Result};
use crate::projects::project::{LayerInfo, VectorInfo};
use crate::users::user::UserId;
use crate::util::config::project_root;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::collections::{
    FeatureCollection, GeometryCollection, TypedFeatureCollection, VectorDataType,
};
use geoengine_datatypes::dataset::{
    DataSetId, DataSetProviderId, InternalDataSetId, StagingDataSetId,
};
use geoengine_datatypes::identifiers::Identifier;
use geoengine_datatypes::primitives::{Coordinate2D, Geometry};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    LoadingInfo, LoadingInfoProvider, RasterResultDescriptor, StaticLoadingInfo,
    VectorResultDescriptor,
};
use geoengine_operators::mock::MockDataSetDataSourceLoadingInfo;
use geoengine_operators::source::{OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceErrorSpec};
use snafu::ensure;
use std::collections::HashMap;

// TODO: merge Raster and Vector DataSet as generic DataSet
struct RasterDataSet {
    data_set: DataSet,
    #[allow(dead_code)] // TODO: use this
    loading_info: RasterLoadingInfo,
    #[allow(dead_code)] // TODO: use this
    result: RasterResultDescriptor,
}

struct VectorDataSet {
    data_set: DataSet,
    loading_info: VectorLoadingInfo,
    result: VectorResultDescriptor,
}

struct StagedRaster {
    #[allow(dead_code)] // TODO: use this
    loading_info: RasterLoadingInfo,
    #[allow(dead_code)] // TODO: use this
    result: RasterResultDescriptor,
}

struct StagedVector {
    loading_info: VectorLoadingInfo,
    result: VectorResultDescriptor,
}

pub struct HashmapDataSetDB {
    data_set_permissions: Vec<UserDataSetPermission>,
    external_providers: HashMap<DataSetProviderId, Box<dyn DataSetProvider>>,
    external_provider_permissions: Vec<UserDataSetProviderPermission>,
    staged_rasters: HashMap<StagingDataSetId, StagedRaster>,
    staged_vectors: HashMap<StagingDataSetId, StagedVector>,
    raster_data_sets: HashMap<InternalDataSetId, RasterDataSet>,
    vector_data_sets: HashMap<InternalDataSetId, VectorDataSet>,
}

impl Default for HashmapDataSetDB {
    fn default() -> Self {
        let mut vector_data_sets = HashMap::new();
        vector_data_sets.insert(
            InternalDataSetId(
                uuid::Uuid::parse_str("e3fc70fc-5fbc-41e9-9e7a-3d6ac27e12a9").unwrap(), // TODO: define a constant
            ),
            VectorDataSet {
                data_set: DataSet {
                    id: DataSetId::Internal(InternalDataSetId(
                        uuid::Uuid::parse_str("e3fc70fc-5fbc-41e9-9e7a-3d6ac27e12a9").unwrap(),
                    )),
                    name: "Ports".to_string(),
                    description: "".to_string(),
                    data_type: LayerInfo::Vector(VectorInfo {}),
                    source_operator: "OgrSource".to_string(),
                },
                loading_info: VectorLoadingInfo::Ogr(OgrSourceDataset {
                    file_name: project_root()
                        .join("operators/test-data/vector/data/ne_10m_ports/ne_10m_ports.shp"),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    columns: None,
                    default_geometry: None,
                    force_ogr_time_filter: false,
                    on_error: OgrSourceErrorSpec::Skip,
                    provenance: None,
                }),
                result: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::wgs84().into(),
                },
            },
        );

        Self {
            data_set_permissions: vec![],
            external_providers: Default::default(),
            external_provider_permissions: vec![],
            staged_rasters: Default::default(),
            staged_vectors: Default::default(),
            vector_data_sets,
            raster_data_sets: HashMap::default(),
        }
    }
}

#[async_trait]
impl DataSetDB for HashmapDataSetDB {
    async fn add_raster_data(
        &mut self,
        user: UserId,
        data_set_info: Validated<AddDataSet>,
        loading_info: RasterLoadingInfo,
        result: RasterResultDescriptor,
    ) -> Result<InternalDataSetId> {
        let data_set: DataSet = data_set_info.user_input.into();
        let id = data_set.id.internal().expect("added");

        self.raster_data_sets.insert(
            id,
            RasterDataSet {
                data_set,
                loading_info,
                result,
            },
        );

        self.data_set_permissions.push(UserDataSetPermission {
            user,
            data_set: id,
            permission: DataSetPermission::Owner,
        });

        Ok(id)
    }

    async fn add_vector_data(
        &mut self,
        user: UserId,
        data_set_info: Validated<AddDataSet>,
        loading_info: VectorLoadingInfo,
        result: VectorResultDescriptor,
    ) -> Result<InternalDataSetId> {
        let data_set: DataSet = data_set_info.user_input.into();
        let id = data_set
            .id
            .internal()
            .expect("Data sets from user inputs get internal ids");

        self.vector_data_sets.insert(
            id,
            VectorDataSet {
                data_set,
                loading_info,
                result,
            },
        );

        self.data_set_permissions.push(UserDataSetPermission {
            user,
            data_set: id,
            permission: DataSetPermission::Owner,
        });

        Ok(id)
    }

    async fn stage_raster_data(
        &mut self,
        _user: UserId,
        loading_info: RasterLoadingInfo,
        result: RasterResultDescriptor,
    ) -> Result<StagingDataSetId> {
        // TODO: store user data set relationship
        let id = StagingDataSetId::new();
        self.staged_rasters.insert(
            id,
            StagedRaster {
                loading_info,
                result,
            },
        );
        Ok(id)
    }

    async fn stage_vector_data(
        &mut self,
        _user: UserId,
        loading_info: VectorLoadingInfo,
        result: VectorResultDescriptor,
    ) -> Result<StagingDataSetId> {
        // TODO: store user data set relationship
        let id = StagingDataSetId::new();
        self.staged_vectors.insert(
            id,
            StagedVector {
                loading_info,
                result,
            },
        );
        Ok(id)
    }

    async fn unstage_data(&mut self, _user: UserId, data_set: StagingDataSetId) -> Result<()> {
        // TODO: check user permission?
        if self.staged_rasters.remove(&data_set).is_some() {
            return Ok(());
        }
        if self.staged_vectors.remove(&data_set).is_some() {
            return Ok(());
        }
        Err(Error::UnknownStagedDataSetId)
    }

    async fn import_raster_data<T: Pixel>(
        &mut self,
        _user: UserId,
        _data_set: Validated<ImportDataSet>,
        _stream: BoxStream<'_, geoengine_operators::util::Result<RasterTile2D<T>>>,
        _meta: RasterResultDescriptor,
    ) -> Result<InternalDataSetId> {
        // TODO: check user permission?
        todo!()
    }

    async fn import_vector_data<G: Geometry>(
        &mut self,
        user: UserId,
        data_set: Validated<ImportDataSet>,
        stream: BoxStream<'_, geoengine_operators::util::Result<FeatureCollection<G>>>,
        result: VectorResultDescriptor,
    ) -> Result<InternalDataSetId>
    where
        FeatureCollection<G>: Into<TypedFeatureCollection>,
    {
        // TODO: check user permission?
        let data_set: DataSet = data_set.user_input.into();

        // TODO: handle other vector data types
        let points: Vec<Vec<Coordinate2D>> = stream
            .map(|c| {
                // TODO: handle errors
                let c: TypedFeatureCollection = c.unwrap().into();
                let points = c.try_into_points().unwrap();
                // TODO: handle multi point features
                points.coordinates().to_vec()
            })
            .collect()
            .await;

        let points = points.into_iter().flatten().collect::<Vec<_>>();

        let id = data_set.id.internal().expect("imported");
        self.vector_data_sets.insert(
            id,
            VectorDataSet {
                data_set,
                loading_info: VectorLoadingInfo::Mock(MockDataSetDataSourceLoadingInfo { points }),
                result,
            },
        );

        self.data_set_permissions.push(UserDataSetPermission {
            user,
            data_set: id,
            permission: DataSetPermission::Owner,
        });

        Ok(id)
    }

    async fn add_data_set_permission(
        &mut self,
        _data_set: InternalDataSetId,
        _user: UserId,
        _permission: DataSetPermission,
    ) -> Result<()> {
        todo!()
    }

    async fn remove_data_set_permission(
        &mut self,
        _data_set: InternalDataSetId,
        _user: UserId,
        _permission: DataSetPermission,
    ) -> Result<()> {
        todo!()
    }

    async fn add_data_set_provider(
        &mut self,
        _user: UserId,
        provider: Validated<AddDataSetProvider>,
    ) -> Result<DataSetProviderId> {
        // TODO: check permission
        let id = DataSetProviderId::new();
        self.external_providers.insert(
            id,
            TypedDataSetProvider::new(provider.user_input).into_box(),
        );
        Ok(id)
    }

    async fn list_data_set_providers(
        &self,
        user: UserId,
        _options: Validated<DataSetProviderListOptions>,
    ) -> Result<Vec<DataSetProviderListing>> {
        // TODO: options
        Ok(self
            .external_provider_permissions
            .iter()
            .filter_map(|p| {
                if p.user == user {
                    // let provider = self.external_providers.get(&p.external_provider).unwrap();
                    Some(DataSetProviderListing {
                        // TODO
                        id: p.external_provider,
                        name: "".to_string(),
                        description: "".to_string(),
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    async fn data_set_provider(
        &self,
        user: UserId,
        provider: DataSetProviderId,
    ) -> Result<&dyn DataSetProvider> {
        ensure!(
            self.external_provider_permissions
                .iter()
                .any(|p| p.user == user && p.external_provider == provider),
            error::DataSetListingProviderUnauthorized
        );

        self.external_providers
            .get(&provider)
            .map(AsRef::as_ref)
            .ok_or(UnknownDataSetProviderId)
    }
}

#[async_trait]
impl DataSetProvider for HashmapDataSetDB {
    async fn list(
        &self,
        _user: UserId,
        _options: Validated<DataSetListOptions>,
    ) -> Result<Vec<DataSetListing>> {
        // TODO: use options
        // TODO: use data_set_permissions

        // TODO: DataSet into DataSetListing once unified
        Ok(self
            .vector_data_sets
            .values()
            .map(|d| DataSetListing {
                id: d.data_set.id.clone(),
                name: d.data_set.name.clone(),
                description: d.data_set.description.clone(),
                tags: vec![], // TODO
                source_operator: d.data_set.source_operator.clone(),
            })
            .chain(self.raster_data_sets.values().map(|d| DataSetListing {
                id: d.data_set.id.clone(),
                name: d.data_set.name.clone(),
                description: d.data_set.description.clone(),
                tags: vec![], // TODO
                source_operator: d.data_set.source_operator.clone(),
            }))
            .collect())
    }
}

impl LoadingInfoProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>
    for HashmapDataSetDB
{
    fn loading_info(
        &self,
        data_set: &DataSetId,
    ) -> Result<
        Box<dyn LoadingInfo<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        let loading_info =
            match data_set {
                DataSetId::Internal(id) => {
                    let data = self
                        .vector_data_sets
                        .get(&id)
                        .ok_or(geoengine_operators::error::Error::UnknownDataSetId)?;

                    match &data.loading_info {
                        VectorLoadingInfo::Mock(loading_info) => loading_info.clone(),
                        _ => return Err(
                            geoengine_operators::error::Error::DataSetLoadingInfoProviderMismatch,
                        ),
                    }
                }
                DataSetId::Staging(id) => {
                    let data = self
                        .staged_vectors
                        .get(&id)
                        .ok_or(geoengine_operators::error::Error::UnknownDataSetId)?;

                    match &data.loading_info {
                        VectorLoadingInfo::Mock(loading_info) => loading_info.clone(),
                        _ => return Err(
                            geoengine_operators::error::Error::DataSetLoadingInfoProviderMismatch,
                        ),
                    }
                }
                DataSetId::External(_) => {
                    return Err(geoengine_operators::error::Error::InvalidDataSetId)
                }
            };

        Ok(Box::new(loading_info)
            as Box<
                dyn LoadingInfo<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>,
            >)
    }
}

impl LoadingInfoProvider<OgrSourceDataset, VectorResultDescriptor> for HashmapDataSetDB {
    fn loading_info(
        &self,
        data_set: &DataSetId,
    ) -> Result<
        Box<dyn LoadingInfo<OgrSourceDataset, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        let (loading_info, result) =
            match data_set {
                DataSetId::Internal(id) => {
                    let data = self
                        .vector_data_sets
                        .get(&id)
                        .ok_or(geoengine_operators::error::Error::UnknownDataSetId)?;

                    match &data.loading_info {
                        VectorLoadingInfo::Ogr(loading_info) => (loading_info.clone(), data.result),
                        _ => return Err(
                            geoengine_operators::error::Error::DataSetLoadingInfoProviderMismatch,
                        ),
                    }
                }
                DataSetId::Staging(id) => {
                    let data = self
                        .staged_vectors
                        .get(&id)
                        .ok_or(geoengine_operators::error::Error::UnknownDataSetId)?;

                    match &data.loading_info {
                        VectorLoadingInfo::Ogr(loading_info) => (loading_info.clone(), data.result),
                        _ => return Err(
                            geoengine_operators::error::Error::DataSetLoadingInfoProviderMismatch,
                        ),
                    }
                }
                DataSetId::External(_) => {
                    return Err(geoengine_operators::error::Error::InvalidDataSetId)
                }
            };

        Ok(Box::new(StaticLoadingInfo {
            info: loading_info,
            meta: result,
        })
            as Box<
                dyn LoadingInfo<OgrSourceDataset, VectorResultDescriptor>,
            >)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Context, InMemoryContext};
    use crate::datasets::listing::OrderBy;
    use crate::projects::project::{LayerInfo, VectorInfo};
    use crate::users::user::UserRegistration;
    use crate::users::userdb::UserDB;
    use crate::util::user_input::UserInput;
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_operators::engine::VectorOperator;
    use geoengine_operators::engine::{
        MockQueryContext, QueryRectangle, TypedVectorQueryProcessor,
    };
    use geoengine_operators::mock::{MockDataSetDataSource, MockDataSetDataSourceParams};
    use geoengine_operators::source::{
        OgrSource, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceErrorSpec,
        OgrSourceParameters,
    };
    use std::str::FromStr;

    #[tokio::test]
    async fn mock() {
        let ctx = InMemoryContext::default();

        let user_id = ctx
            .user_db_ref_mut()
            .await
            .register(
                UserRegistration {
                    email: "test@example.com".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        // stage the data
        let coordinates: Vec<Coordinate2D> = vec![(1.0, 1.0).into(), (2.0, 2.0).into()];
        let staged_id = ctx
            .data_set_db_ref_mut()
            .await
            .stage_vector_data(
                user_id,
                VectorLoadingInfo::Mock(MockDataSetDataSourceLoadingInfo {
                    points: coordinates.clone(),
                }),
                VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReferenceOption::Unreferenced,
                },
            )
            .await
            .unwrap();

        // build operator that loads data
        let execution_context = ctx.execution_context();
        let mps = MockDataSetDataSource {
            params: MockDataSetDataSourceParams {
                data_set: DataSetId::Staging(staged_id),
            },
        }
        .boxed();
        let initialized = mps.initialize(&execution_context).unwrap();

        let typed_processor = initialized.query_processor();
        let point_processor = match typed_processor {
            Ok(TypedVectorQueryProcessor::MultiPoint(processor)) => processor,
            _ => panic!(),
        };

        // build stream from operator
        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let query_ctx = MockQueryContext::new(2 * std::mem::size_of::<Coordinate2D>());

        let stream = point_processor.vector_query(query_rectangle, &query_ctx);

        // import into the db
        let data_set_info = ImportDataSet {
            name: "Mock data".to_string(),
            description: "".to_string(),
            data_type: LayerInfo::Vector(VectorInfo {}),
            source_operator: "MockDataSetDataSource".to_string(),
        }
        .validated()
        .unwrap();

        let imported_id = ctx
            .data_set_db_ref_mut()
            .await
            .import_vector_data(
                user_id,
                data_set_info,
                stream,
                VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReferenceOption::Unreferenced,
                },
            )
            .await
            .unwrap();

        // query from db
        let mps = MockDataSetDataSource {
            params: MockDataSetDataSourceParams {
                data_set: imported_id.into(),
            },
        }
        .boxed();
        let initialized = mps.initialize(&execution_context).unwrap();

        let typed_processor = initialized.query_processor();
        let point_processor = match typed_processor {
            Ok(TypedVectorQueryProcessor::MultiPoint(processor)) => processor,
            _ => panic!(),
        };

        // build stream from operator
        let stream = point_processor.vector_query(query_rectangle, &query_ctx);

        let coords = stream
            .map(|c| {
                let c = c.unwrap();
                c.coordinates().to_vec()
            })
            .collect::<Vec<_>>()
            .await;

        let coords = coords.into_iter().flatten().collect::<Vec<_>>();

        assert_eq!(coords, coordinates);

        // list data sets
        let list = ctx
            .data_set_db_ref_mut()
            .await
            .list(
                user_id,
                DataSetListOptions {
                    filter: None,
                    order: OrderBy::NameAsc,
                    offset: 0, // Skip default data set
                    limit: 5,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            list,
            vec![
                DataSetListing {
                    id: DataSetId::Internal(InternalDataSetId(
                        uuid::Uuid::from_str("e3fc70fc-5fbc-41e9-9e7a-3d6ac27e12a9").unwrap()
                    )),
                    name: "Ports".to_string(),
                    description: "".to_string(),
                    tags: vec![],
                    source_operator: "OgrSource".to_string()
                },
                DataSetListing {
                    id: imported_id.into(),
                    name: "Mock data".to_string(),
                    description: "".to_string(),
                    tags: vec![],
                    source_operator: "MockDataSetDataSource".to_string()
                }
            ]
        );
    }

    #[tokio::test]
    async fn ogr() {
        let ctx = InMemoryContext::default();

        let user_id = ctx
            .user_db_ref_mut()
            .await
            .register(
                UserRegistration {
                    email: "test@example.com".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let add = AddDataSet {
            name: "Ports".to_string(),
            description: "".to_string(),
            data_type: LayerInfo::Vector(VectorInfo {}),
            source_operator: "OgrSource".to_string(),
        }
        .validated()
        .unwrap();

        // add the data
        let id = ctx
            .data_set_db_ref_mut()
            .await
            .add_vector_data(
                user_id,
                add,
                VectorLoadingInfo::Ogr(OgrSourceDataset {
                    file_name: "operators/test-data/vector/data/ne_10m_ports/ne_10m_ports.shp"
                        .into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    columns: None,
                    default_geometry: None,
                    force_ogr_time_filter: false,
                    on_error: OgrSourceErrorSpec::Skip,
                    provenance: None,
                }),
                VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReferenceOption::Unreferenced,
                },
            )
            .await
            .unwrap();

        // build operator that loads data
        let execution_context = ctx.execution_context();
        let mps = OgrSource {
            params: OgrSourceParameters {
                data_set: DataSetId::Internal(id),
                attribute_projection: None,
            },
        }
        .boxed();
        let initialized = mps.initialize(&execution_context).unwrap();

        let typed_processor = initialized.query_processor();
        let point_processor = match typed_processor {
            Ok(TypedVectorQueryProcessor::MultiPoint(processor)) => processor,
            _ => panic!(),
        };

        // build stream from operator
        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let query_ctx = MockQueryContext::new(2 * std::mem::size_of::<Coordinate2D>());

        let stream = point_processor.vector_query(query_rectangle, &query_ctx);

        assert_eq!(stream.collect::<Vec<_>>().await.len(), 1);

        // list data sets
        let list = ctx
            .data_set_db_ref_mut()
            .await
            .list(
                user_id,
                DataSetListOptions {
                    filter: None,
                    order: OrderBy::NameAsc,
                    offset: 0,
                    limit: 1,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            list[0],
            DataSetListing {
                id: id.into(),
                name: "Ports".to_string(),
                description: "".to_string(),
                tags: vec![],
                source_operator: "OgrSource".to_string()
            }
        );
    }
}
