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
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId, InternalDataSetId};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, StaticMetaData, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDataSetDataSourceLoadingInfo;
use geoengine_operators::source::OgrSourceDataset;
use std::collections::HashMap;

#[derive(Default)]
pub struct HashMapDataSetDb {
    data_sets: Vec<DataSet>,
    ogr_data_sets:
        HashMap<InternalDataSetId, StaticMetaData<OgrSourceDataset, VectorResultDescriptor>>,
    mock_data_sets: HashMap<
        InternalDataSetId,
        StaticMetaData<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>,
    >,
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
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDb);
}

impl DataSetStorer for HashMapDataSetDb {
    type StorageType = Box<dyn HashMapStorable>;
}

impl HashMapStorable for StaticMetaData<OgrSourceDataset, VectorResultDescriptor> {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDb) {
        db.ogr_data_sets.insert(id, self.clone());
    }
}

impl HashMapStorable for StaticMetaData<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor> {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDb) {
        db.mock_data_sets.insert(id, self.clone());
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
        let d: DataSet = data_set.user_input.into();
        let id = d.id.clone();
        meta_data.store(id.internal().expect("from AddDataSet"), self);
        self.data_sets.push(d);
        Ok(id)
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
            name: "OgrDataSet".to_string(),
            description: "My Ogr data set".to_string(),
            source_operator: "OgrSource".to_string(),
            result_descriptor: descriptor.clone().into(),
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
