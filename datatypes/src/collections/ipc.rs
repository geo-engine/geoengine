use std::{collections::HashMap, sync::Arc};

use super::{FeatureCollection, IntoGeometryOptionsIterator};
use crate::{spatial_reference::SpatialReferenceOption, util::Result};
use arrow::{
    array::{Array, ArrayRef},
    datatypes::{Field, Schema},
    ipc::writer::{FileWriter, IpcWriteOptions},
    record_batch::RecordBatch,
};
use wkt::ToWkt;

pub const SPATIAL_REF_KEY: &str = "spatialReference";

pub trait FeatureCollectionIpc<'g> {
    fn to_arrow_record_batch(&'g self) -> Result<arrow::record_batch::RecordBatch>;

    fn to_arrow_ipc_file_bytes(&'g self, spatial_ref: SpatialReferenceOption) -> Result<Vec<u8>>;
}

impl<'g, G> FeatureCollectionIpc<'g> for FeatureCollection<G>
where
    Self: IntoGeometryOptionsIterator<'g>,
{
    fn to_arrow_record_batch(&'g self) -> Result<arrow::record_batch::RecordBatch> {
        // Currently, PyArrow does not support our storage of geometries
        // Thus, we will transform them to WKTs.
        // TODO: use `let record_batch: RecordBatch = struct_ref.into();` instead

        let mut wkts = Vec::with_capacity(self.table.len());
        for geometry_option in self.geometry_options() {
            if let Some(geometry) = geometry_option {
                wkts.push(geometry.wkt_string());
            } else {
                wkts.push(String::new());
            }
        }
        let wkt_array = Arc::new(arrow::array::StringArray::from(wkts));

        let arrow::datatypes::DataType::Struct(fields) = self.table.data_type() else {
            unreachable!("unable to get datatype as struct")
        };

        let mut record_batch_fields = Vec::with_capacity(fields.len());
        let mut record_batch_columns: Vec<ArrayRef> =
            Vec::with_capacity(self.table.columns().len());

        for (field, column) in fields.iter().zip(self.table.columns()) {
            if field.name() == Self::GEOMETRY_COLUMN_NAME {
                record_batch_fields.push(Field::new(
                    Self::GEOMETRY_COLUMN_NAME,
                    arrow::datatypes::DataType::Utf8,
                    false,
                ));

                record_batch_columns.push(wkt_array.clone());

                continue;
            }

            record_batch_fields.push(field.clone());
            record_batch_columns.push(column.clone());
        }

        let schema = Schema::new(record_batch_fields);
        RecordBatch::try_new(Arc::new(schema), record_batch_columns).map_err(Into::into)
    }

    fn to_arrow_ipc_file_bytes(&'g self, spatial_ref: SpatialReferenceOption) -> Result<Vec<u8>> {
        let record_batch = self.to_arrow_record_batch()?;

        let metadata: HashMap<String, String> =
            [(SPATIAL_REF_KEY.to_string(), spatial_ref.to_string())].into();

        let schema: Schema = record_batch
            .schema()
            .as_ref()
            .clone()
            .with_metadata(metadata);

        let mut file_writer = FileWriter::try_new_with_options(
            Vec::new(),
            &schema,
            // TODO: find out why this does not work in Python for strings or differntly sized arrays
            // IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?,
            IpcWriteOptions::default(),
        )?;
        file_writer.write(&record_batch)?;
        file_writer.finish()?;

        Ok(file_writer.into_inner()?)
    }
}

#[cfg(test)]
mod tests {
    use arrow::ipc::reader::FileReader;

    use super::*;
    use crate::{
        collections::MultiPolygonCollection,
        primitives::{FeatureData, MultiPolygon, TimeInterval},
        spatial_reference::SpatialReference,
        util::well_known_data::{COLOGNE_EPSG_4326, HAMBURG_EPSG_4326, MARBURG_EPSG_4326},
    };

    #[test]
    fn test_multi_polygon_to_ipc() {
        let collection = MultiPolygonCollection::from_slices(
            &[
                MultiPolygon::new(vec![
                    vec![vec![
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                    ]],
                    vec![vec![
                        COLOGNE_EPSG_4326,
                        HAMBURG_EPSG_4326,
                        MARBURG_EPSG_4326,
                        COLOGNE_EPSG_4326,
                    ]],
                ])
                .unwrap(),
                MultiPolygon::new(vec![vec![vec![
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                ]]])
                .unwrap(),
            ],
            &[TimeInterval::default(), TimeInterval::default()],
            &[
                ("A", FeatureData::Int(vec![1, 2])),
                (
                    "B",
                    FeatureData::Text(vec!["One".to_string(), "Two".to_string()]),
                ),
            ],
        )
        .unwrap();

        let _record_batch = collection.to_arrow_record_batch().unwrap();

        let bytes = collection
            .to_arrow_ipc_file_bytes(SpatialReference::epsg_4326().into())
            .unwrap();

        // read bytes

        let mut reader = FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        let schema = reader.schema();

        assert_eq!(schema.metadata()[SPATIAL_REF_KEY], "EPSG:4326");

        let data = reader.next().unwrap().unwrap();

        assert_eq!(
            data.num_columns(),
            4 /* geo + time + A column + B column */
        );
        assert_eq!(data.num_rows(), 2);

        let values = data
            .column_by_name("A")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .values()
            .to_vec();

        assert_eq!(values, vec![1, 2]);

        let string_values = data
            .column_by_name("B")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(
            [string_values.value(0), string_values.value(1)],
            ["One", "Two"]
        );

        assert!(reader.next().is_none()); // only one batch
    }
}
