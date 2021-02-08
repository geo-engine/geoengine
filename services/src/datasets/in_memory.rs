use crate::datasets::listing::{DataSetListOptions, DataSetListing, DataSetProvider};
use crate::datasets::storage::{
    AddDataSet, AddDataSetProvider, DataSet, DataSetDB, DataSetProviderDB,
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
pub struct HashMapDataSetDB {
    data_sets: Vec<DataSet>,
    ogr_data_sets:
        HashMap<InternalDataSetId, StaticMetaData<OgrSourceDataset, VectorResultDescriptor>>,
    mock_data_sets: HashMap<
        InternalDataSetId,
        StaticMetaData<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>,
    >,
}

impl DataSetDB for HashMapDataSetDB {}

#[async_trait]
impl DataSetProviderDB for HashMapDataSetDB {
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
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDB);
}

impl DataSetStorer for HashMapDataSetDB {
    type StorageType = Box<dyn HashMapStorable>;
}

impl HashMapStorable for StaticMetaData<OgrSourceDataset, VectorResultDescriptor> {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDB) {
        db.ogr_data_sets.insert(id, self.clone());
    }
}

impl HashMapStorable for StaticMetaData<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor> {
    fn store(&self, id: InternalDataSetId, db: &mut HashMapDataSetDB) {
        db.mock_data_sets.insert(id, self.clone());
    }
}

#[async_trait]
impl DataSetStore for HashMapDataSetDB {
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
impl DataSetProvider for HashMapDataSetDB {
    async fn list(
        &self,
        _user: UserId,
        _options: Validated<DataSetListOptions>,
    ) -> Result<Vec<DataSetListing>> {
        // TODO: permissions
        // TODO: options
        Ok(self.data_sets.iter().map(DataSet::listing).collect())
    }
}

impl MetaDataProvider<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>
    for HashMapDataSetDB
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

impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor> for HashMapDataSetDB {
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
    use crate::projects::project::{LayerInfo, VectorInfo};
    use crate::util::user_input::UserInput;
    use crate::util::Identifier;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_operators::source::OgrSourceErrorSpec;

    #[tokio::test]
    async fn add_ogr_and_list() -> Result<()> {
        let ctx = InMemoryContext::default();

        let ds = AddDataSet {
            name: "OgrDataSet".to_string(),
            description: "My Ogr data set".to_string(),
            data_type: LayerInfo::Vector(VectorInfo {}),
            source_operator: "OgrSource".to_string(),
            spatial_reference: SpatialReferenceOption::Unreferenced,
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
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default(),
            },
        };

        let id = ctx
            .data_set_db_ref_mut()
            .await
            .add_data_set(UserId::new(), ds.validated()?, Box::new(meta))
            .await?;

        let exe_ctx = ctx.execution_context()?;

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
                UserId::new(),
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
                spatial_reference: SpatialReferenceOption::Unreferenced
            }
        );

        Ok(())
    }
}
