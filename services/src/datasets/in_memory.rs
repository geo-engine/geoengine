use super::storage::DataSetDB;
use crate::datasets::listing::{
    DataSetListOptions, DataSetListing, DataSetProvider, TypedDataSetProvider,
};
use crate::datasets::storage::{
    AddDataSetProvider, DataSet, DataSetLoadingInfo, DataSetProviderListOptions,
    DataSetProviderListing, ImportDataSet, RasterLoadingInfo, UserDataSetProviderPermission,
    VectorLoadingInfo,
};
use crate::datasets::storage::{DataSetPermission, UserDataSetPermission};
use crate::error;
use crate::error::Error::UnknownDataSetProviderId;
use crate::error::{Error, Result};
use crate::users::user::UserId;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::collections::{
    FeatureCollection, GeometryCollection, TypedFeatureCollection,
};
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId, StagingDataSetId};
use geoengine_datatypes::identifiers::Identifier;
use geoengine_datatypes::primitives::{Coordinate2D, Geometry};
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_operators::engine::{LoadingInfo, LoadingInfoProvider, VectorResultDescriptor};
use geoengine_operators::mock::MockDataSetDataSourceLoadingInfo;
use snafu::ensure;
use std::collections::HashMap;

struct InMemoryDataSet {
    data_set: DataSet,
    loading_info: DataSetLoadingInfo,
}

#[derive(Default)]
pub struct HashmapDataSetDB {
    data_set_permissions: Vec<UserDataSetPermission>,
    external_providers: HashMap<DataSetProviderId, Box<dyn DataSetProvider>>,
    external_provider_permissions: Vec<UserDataSetProviderPermission>,
    staged_rasters: HashMap<StagingDataSetId, RasterLoadingInfo>,
    staged_vectors: HashMap<StagingDataSetId, VectorLoadingInfo>,
    data_sets: HashMap<DataSetId, InMemoryDataSet>,
}

#[async_trait]
impl DataSetDB for HashmapDataSetDB {
    async fn stage_raster_data(
        &mut self,
        _user: UserId,
        loading_info: RasterLoadingInfo,
    ) -> Result<StagingDataSetId> {
        // TODO: store user data set relationship
        let id = StagingDataSetId::new();
        self.staged_rasters.insert(id, loading_info);
        Ok(id)
    }

    async fn stage_vector_data(
        &mut self,
        _user: UserId,
        loading_info: VectorLoadingInfo,
    ) -> Result<StagingDataSetId> {
        // TODO: store user data set relationship
        let id = StagingDataSetId::new();
        self.staged_vectors.insert(id, loading_info);
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
    ) -> Result<DataSetId> {
        // TODO: check user permission?
        todo!()
    }

    async fn import_vector_data<G: Geometry>(
        &mut self,
        user: UserId,
        data_set: Validated<ImportDataSet>,
        stream: BoxStream<'_, geoengine_operators::util::Result<FeatureCollection<G>>>,
    ) -> Result<DataSetId>
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

        let id = data_set.id.clone();
        self.data_sets.insert(
            id.clone(),
            InMemoryDataSet {
                data_set,
                loading_info: DataSetLoadingInfo::Vector(VectorLoadingInfo::Mock(
                    MockDataSetDataSourceLoadingInfo { points },
                )),
            },
        );

        self.data_set_permissions.push(UserDataSetPermission {
            user,
            data_set: id.clone(),
            permission: DataSetPermission::Owner,
        });

        Ok(id)
    }

    async fn add_data_set_permission(
        &mut self,
        _data_set: DataSetId,
        _user: UserId,
        _permission: DataSetPermission,
    ) -> Result<()> {
        todo!()
    }

    async fn remove_data_set_permission(
        &mut self,
        _data_set: DataSetId,
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
        user: UserId,
        _options: Validated<DataSetListOptions>,
    ) -> Result<Vec<DataSetListing>> {
        // TODO: use options
        Ok(self
            .data_set_permissions
            .iter()
            .filter_map(|p| {
                if p.user == user {
                    let d = self.data_sets.get(&p.data_set).unwrap();
                    Some(DataSetListing {
                        id: d.data_set.id.clone(),
                        name: d.data_set.name.clone(),
                        description: "".to_string(), // TODO
                        tags: vec![],                // TODO
                    })
                } else {
                    None
                }
            })
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
                DataSetId::Internal(_) => {
                    let data = self
                        .data_sets
                        .get(&data_set)
                        .ok_or(geoengine_operators::error::Error::UnknownDataSetId)?;

                    match &data.loading_info {
                        DataSetLoadingInfo::Vector(VectorLoadingInfo::Mock(loading_info)) => {
                            loading_info.clone()
                        }
                        _ => return Err(
                            geoengine_operators::error::Error::DataSetLoadingInfoProviderMismatch,
                        ),
                    }
                }
                DataSetId::Staging(id) => {
                    let loading_info = self
                        .staged_vectors
                        .get(&id)
                        .ok_or(geoengine_operators::error::Error::UnknownDataSetId)?;

                    if let VectorLoadingInfo::Mock(loading_info) = loading_info {
                        loading_info.clone()
                    } else {
                        return Err(
                            geoengine_operators::error::Error::DataSetLoadingInfoProviderMismatch,
                        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Context, InMemoryContext};
    use crate::datasets::listing::{DataSetFilter, OrderBy};
    use crate::projects::project::{LayerInfo, VectorInfo};
    use crate::users::user::UserRegistration;
    use crate::users::userdb::UserDB;
    use crate::util::user_input::UserInput;
    use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};
    use geoengine_operators::engine::VectorOperator;
    use geoengine_operators::engine::{
        MockQueryContext, QueryRectangle, TypedVectorQueryProcessor,
    };
    use geoengine_operators::mock::{MockDataSetDataSource, MockDataSetDataSourceParams};

    #[tokio::test]
    async fn test() {
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
            data_type: LayerInfo::Vector(VectorInfo {}),
            source_operator: "MockDataSetDataSource".to_string(),
        }
        .validated()
        .unwrap();

        let imported_id = ctx
            .data_set_db_ref_mut()
            .await
            .import_vector_data(user_id, data_set_info, stream)
            .await
            .unwrap();

        // query from db
        let mps = MockDataSetDataSource {
            params: MockDataSetDataSourceParams {
                data_set: imported_id.clone(),
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
                    filter: DataSetFilter {
                        name: "".to_string(),
                    },
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
                id: imported_id,
                name: "Mock data".to_string(),
                description: "".to_string(),
                tags: vec![]
            }
        );
    }
}
