use crate::datasets::listing::{DataSetListOptions, DataSetListing, DataSetProvider, OrderBy};
use crate::datasets::storage::{
    AddDataSet, AddDataSetProvider, DataSet, DataSetDb, DataSetProviderDb,
    DataSetProviderListOptions, DataSetProviderListing, DataSetStore, DataSetStorer,
};
use crate::error;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use geoengine_datatypes::{
    dataset::{DataSetId, DataSetProviderId, InternalDataSetId},
    util::Identifier,
};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, StaticMetaData, TypedResultDescriptor,
    VectorResultDescriptor,
};
use geoengine_operators::source::{GdalLoadingInfo, GdalMetaDataRegular, OgrSourceDataset};
use geoengine_operators::{mock::MockDataSetDataSourceLoadingInfo, source::GdalMetaDataStatic};
use std::collections::HashMap;

use super::storage::MetaDataDefinition;

#[derive(Default)]
pub struct HashMapDataSetDb {
    data_sets: Vec<DataSet>,
    ogr_data_sets:
        HashMap<InternalDataSetId, StaticMetaData<OgrSourceDataset, VectorResultDescriptor>>,
    mock_data_sets: HashMap<
        InternalDataSetId,
        StaticMetaData<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>,
    >,
    gdal_data_sets:
        HashMap<InternalDataSetId, Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>>,
}

impl DataSetDb for HashMapDataSetDb {}

#[async_trait]
impl DataSetProviderDb for HashMapDataSetDb {
    async fn add_data_set_provider(
        &mut self,
        _user: UserId,
        _provider: Validated<AddDataSetProvider>,
    ) -> Result<DataSetProviderId> {
        todo!()
    }

    async fn list_data_set_providers(
        &self,
        _user: UserId,
        _options: Validated<DataSetProviderListOptions>,
    ) -> Result<Vec<DataSetProviderListing>> {
        todo!()
    }

    async fn data_set_provider(
        &self,
        _user: UserId,
        _provider: DataSetProviderId,
    ) -> Result<&dyn DataSetProvider> {
        todo!()
    }
}

pub trait HashMapStorable: Send + Sync {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDb) -> TypedResultDescriptor;
}

impl DataSetStorer for HashMapDataSetDb {
    type StorageType = Box<dyn HashMapStorable>;
}

impl HashMapStorable for MetaDataDefinition {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDb) -> TypedResultDescriptor {
        match self {
            MetaDataDefinition::MockMetaData(d) => d.store(id, db),
            MetaDataDefinition::OgrMetaData(d) => d.store(id, db),
            MetaDataDefinition::GdalMetaDataRegular(d) => d.store(id, db),
            MetaDataDefinition::GdalStatic(d) => d.store(id, db),
        }
    }
}

impl HashMapStorable for StaticMetaData<OgrSourceDataset, VectorResultDescriptor> {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDb) -> TypedResultDescriptor {
        db.ogr_data_sets.insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

impl HashMapStorable for StaticMetaData<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor> {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDb) -> TypedResultDescriptor {
        db.mock_data_sets.insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

impl HashMapStorable for GdalMetaDataRegular {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDb) -> TypedResultDescriptor {
        db.gdal_data_sets.insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

impl HashMapStorable for GdalMetaDataStatic {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDb) -> TypedResultDescriptor {
        db.gdal_data_sets.insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl DataSetStore for HashMapDataSetDb {
    async fn add_data_set(
        &mut self,
        _user: UserId,
        data_set: Validated<AddDataSet>,
        meta_data: Box<dyn HashMapStorable>,
    ) -> Result<DataSetId> {
        let data_set = data_set.user_input;
        let id = data_set
            .id
            .unwrap_or_else(|| InternalDataSetId::new().into());
        let result_descriptor = meta_data.store(id.internal().expect("from AddDataSet"), self);

        let d: DataSet = DataSet {
            id: id.clone(),
            name: data_set.name,
            description: data_set.description,
            result_descriptor,
            source_operator: data_set.source_operator,
        };
        self.data_sets.push(d);

        Ok(id)
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
    }
}

#[async_trait]
impl DataSetProvider for HashMapDataSetDb {
    async fn list(
        &self,
        _user: UserId,
        options: Validated<DataSetListOptions>,
    ) -> Result<Vec<DataSetListing>> {
        // TODO: permissions

        // TODO: include data sets from external data set providers
        let options = options.user_input;

        let mut list: Vec<_> = if let Some(filter) = &options.filter {
            self.data_sets
                .iter()
                .filter(|d| d.name.contains(filter) || d.description.contains(filter))
                .collect()
        } else {
            self.data_sets.iter().collect()
        };

        match options.order {
            OrderBy::NameAsc => list.sort_by(|a, b| a.name.cmp(&b.name)),
            OrderBy::NameDesc => list.sort_by(|a, b| b.name.cmp(&a.name)),
        };

        let list = list
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(DataSet::listing)
            .collect();

        Ok(list)
    }
}

impl MetaDataProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>
    for HashMapDataSetDb
{
    fn meta_data(
        &self,
        data_set: &DataSetId,
    ) -> Result<
        Box<dyn MetaData<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        Ok(Box::new(
            self.mock_data_sets
                .get(&data_set.internal().ok_or(
                    geoengine_operators::error::Error::DataSetMetaData {
                        source: Box::new(error::Error::DataSetIdTypeMissMatch),
                    },
                )?)
                .ok_or(geoengine_operators::error::Error::DataSetMetaData {
                    source: Box::new(error::Error::UnknownDataSetId),
                })?
                .clone(),
        ))
    }
}

impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor> for HashMapDataSetDb {
    fn meta_data(
        &self,
        data_set: &DataSetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        Ok(Box::new(
            self.ogr_data_sets
                .get(&data_set.internal().ok_or(
                    geoengine_operators::error::Error::DataSetMetaData {
                        source: Box::new(error::Error::DataSetIdTypeMissMatch),
                    },
                )?)
                .ok_or(geoengine_operators::error::Error::DataSetMetaData {
                    source: Box::new(error::Error::UnknownDataSetId),
                })?
                .clone(),
        ))
    }
}

impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor> for HashMapDataSetDb {
    fn meta_data(
        &self,
        data_set: &DataSetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        let id = data_set
            .internal()
            .ok_or(geoengine_operators::error::Error::DataSetMetaData {
                source: Box::new(error::Error::DataSetIdTypeMissMatch),
            })?;

        Ok(self
            .gdal_data_sets
            .get(&id)
            .ok_or(geoengine_operators::error::Error::DataSetMetaData {
                source: Box::new(error::Error::UnknownDataSetId),
            })?
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Context, InMemoryContext};
    use crate::datasets::listing::OrderBy;
    use crate::users::session::Session;
    use crate::util::user_input::UserInput;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_operators::source::OgrSourceErrorSpec;

    #[tokio::test]
    async fn add_ogr_and_list() -> Result<()> {
        let ctx = InMemoryContext::default();

        let session = Session::mock();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
        };

        let ds = AddDataSet {
            id: None,
            name: "OgrDataSet".to_string(),
            description: "My Ogr data set".to_string(),
            source_operator: "OgrSource".to_string(),
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: "".to_string(),
                data_type: None,
                time: Default::default(),
                columns: None,
                default_geometry: None,
                force_ogr_time_filter: false,
                on_error: OgrSourceErrorSpec::Skip,
                provenance: None,
            },
            result_descriptor: descriptor.clone(),
        };

        let id = ctx
            .data_set_db_ref_mut()
            .await
            .add_data_set(session.user.id, ds.validated()?, Box::new(meta))
            .await?;

        let exe_ctx = ctx.execution_context(&session)?;

        let meta: Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>> =
            exe_ctx.meta_data(&id)?;

        assert_eq!(
            meta.result_descriptor()?,
            VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default()
            }
        );

        let ds = ctx
            .data_set_db_ref()
            .await
            .list(
                session.user.id,
                DataSetListOptions {
                    filter: None,
                    order: OrderBy::NameAsc,
                    offset: 0,
                    limit: 1,
                }
                .validated()?,
            )
            .await?;

        assert_eq!(ds.len(), 1);

        assert_eq!(
            ds[0],
            DataSetListing {
                id,
                name: "OgrDataSet".to_string(),
                description: "My Ogr data set".to_string(),
                tags: vec![],
                source_operator: "OgrSource".to_string(),
                result_descriptor: descriptor.into()
            }
        );

        Ok(())
    }
}
