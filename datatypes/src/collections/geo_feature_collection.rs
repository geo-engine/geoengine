use crate::primitives::{BoundingBox2D, Coordinate2D, Geometry, GeometryRef, TimeInterval};
use crate::util::arrow::ArrowTyped;
use crate::util::Result;
use arrow::{
    array::{Array, ArrayData},
    buffer::Buffer,
};
use std::sync::Arc;

use super::feature_collection::struct_array_from_data;
use super::FeatureCollection;

/// This trait allows iterating over the geometries of a feature collection
pub trait IntoGeometryIterator<'a> {
    type GeometryIterator: Iterator<Item = Self::GeometryType> + Send;
    type GeometryType: GeometryRef + Send;

    /// Return an iterator over geometries
    fn geometries(&'a self) -> Self::GeometryIterator;
}

/// This trait allows accessing the geometries of the collection by random access
pub trait GeometryRandomAccess<'a> {
    type GeometryType: GeometryRef + Send;

    /// Returns the geometry at index `feature_index`
    fn geometry_at(&'a self, feature_index: usize) -> Option<Self::GeometryType>;
}

/// This trait allows iterating over the geometries of a feature collection if the collection has geometries
pub trait IntoGeometryOptionsIterator<'i> {
    type GeometryOptionIterator: Iterator<Item = Option<Self::GeometryType>> + Send;
    type GeometryType: GeometryRef + Send;

    /// Return an iterator over geometries
    fn geometry_options(&'i self) -> Self::GeometryOptionIterator;
}

/// Common geo functionality for `FeatureCollection`s
pub trait GeometryCollection {
    fn coordinates(&self) -> &[Coordinate2D];

    fn feature_offsets(&self) -> &[i32];

    fn bbox(&self) -> Option<BoundingBox2D> {
        BoundingBox2D::from_coord_ref_iter(self.coordinates())
    }
}

pub trait ReplaceRawArrayCoords {
    fn replace_raw_coords(
        array_ref: &Arc<dyn Array>,
        new_coord_buffer: Buffer,
    ) -> Result<ArrayData>;
}

/// A trait for common feature collection modifications that are specific to the geometry type
pub trait GeoFeatureCollectionModifications<G: Geometry> {
    type Output;

    /// Replaces the current geometries and returns an updated collection.
    fn replace_geometries(&self, time_intervals: Vec<G>) -> Result<Self::Output>;
}

impl<G> GeoFeatureCollectionModifications<G> for FeatureCollection<G>
where
    FeatureCollection<G>: GeometryCollection,
    G: Geometry + ArrowTyped,
{
    type Output = FeatureCollection<G>;

    fn replace_geometries(&self, geometries: Vec<G>) -> Result<Self::Output> {
        let geometries = G::from_vec(geometries)?;

        let mut columns = Vec::<arrow::datatypes::Field>::with_capacity(self.table.num_columns());
        let mut column_values =
            Vec::<arrow::array::ArrayRef>::with_capacity(self.table.num_columns());

        // copy geometry data
        columns.push(arrow::datatypes::Field::new(
            Self::GEOMETRY_COLUMN_NAME,
            G::arrow_data_type(),
            false,
        ));
        column_values.push(Arc::new(geometries));

        // copy time data
        columns.push(arrow::datatypes::Field::new(
            Self::TIME_COLUMN_NAME,
            TimeInterval::arrow_data_type(),
            false,
        ));
        column_values.push(
            self.table
                .column_by_name(Self::TIME_COLUMN_NAME)
                .expect("The time column should have been added during creation of the collection")
                .clone(),
        );

        // copy remaining attribute data
        for (column_name, column_type) in &self.types {
            columns.push(arrow::datatypes::Field::new(
                column_name,
                column_type.arrow_data_type(),
                column_type.nullable(),
            ));
            column_values.push(
                self.table
                    .column_by_name(column_name)
                    .expect("The attribute column should exist")
                    .clone(),
            );
        }

        Ok(Self::new_from_internals(
            struct_array_from_data(columns, column_values, self.table.len())?,
            self.types.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        collections::{FeatureCollectionInfos, MultiLineStringCollection},
        primitives::{FeatureData, MultiLineString},
        util::well_known_data::{COLOGNE_EPSG_4326, HAMBURG_EPSG_4326, MARBURG_EPSG_4326},
    };

    #[test]
    fn test_replace_lines() {
        let collection = MultiLineStringCollection::from_slices(
            &[
                MultiLineString::new(vec![vec![MARBURG_EPSG_4326, HAMBURG_EPSG_4326]]).unwrap(),
                MultiLineString::new(vec![
                    vec![COLOGNE_EPSG_4326, MARBURG_EPSG_4326, HAMBURG_EPSG_4326],
                    vec![HAMBURG_EPSG_4326, COLOGNE_EPSG_4326],
                ])
                .unwrap(),
            ],
            &[TimeInterval::default(), TimeInterval::default()],
            &[("A", FeatureData::Int(vec![1, 2]))],
        )
        .unwrap();

        let collection2 = collection
            .replace_geometries(vec![
                MultiLineString::new(vec![vec![HAMBURG_EPSG_4326, COLOGNE_EPSG_4326]]).unwrap(),
                MultiLineString::new(vec![
                    vec![COLOGNE_EPSG_4326, MARBURG_EPSG_4326, HAMBURG_EPSG_4326],
                    vec![HAMBURG_EPSG_4326, COLOGNE_EPSG_4326],
                ])
                .unwrap(),
            ])
            .unwrap();

        assert_eq!(collection2.len(), 2);

        assert_ne!(collection, collection2);
    }
}
