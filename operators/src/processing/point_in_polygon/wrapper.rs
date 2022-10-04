use geoengine_datatypes::collections::MultiPolygonCollection;
use ouroboros::self_referencing;

use super::PointInPolygonTester;

/// A wrapper to allow storing a [`MultiPolygonCollection`] together with a [`PointInPolygonTester`].
#[self_referencing]
struct PipWrapperInternal {
    collection: MultiPolygonCollection,
    #[borrows(collection)]
    #[covariant]
    tester: PointInPolygonTester<'this>,
}

/// A wrapper to allow storing a [`MultiPolygonCollection`] together with a [`PointInPolygonTester`].
pub struct PointInPolygonTesterWithCollection(PipWrapperInternal);

impl PointInPolygonTesterWithCollection {
    pub fn new(collection: MultiPolygonCollection) -> Self {
        Self(
            PipWrapperInternalBuilder {
                collection,
                tester_builder: |collection| PointInPolygonTester::new(collection),
            }
            .build(),
        )
    }

    pub fn collection(&self) -> &MultiPolygonCollection {
        self.0.borrow_collection()
    }

    pub fn tester(&self) -> &PointInPolygonTester<'_> {
        self.0.borrow_tester()
    }
}

impl From<PointInPolygonTesterWithCollection> for MultiPolygonCollection {
    fn from(value: PointInPolygonTesterWithCollection) -> Self {
        value.0.into_heads().collection
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use geoengine_datatypes::{
        collections::FeatureCollectionInfos,
        primitives::{MultiPolygon, TimeInterval},
    };

    use super::*;

    #[test]
    fn test() {
        let collection = MultiPolygonCollection::from_data(
            vec![
                MultiPolygon::new(vec![vec![
                    vec![
                        (0.0, 0.1).into(),
                        (10.0, 10.1).into(),
                        (0.0, 10.1).into(),
                        (0.0, 0.1).into(),
                    ],
                    vec![
                        (2.0, 2.1).into(),
                        (3.0, 3.1).into(),
                        (2.0, 3.1).into(),
                        (2.0, 2.1).into(),
                    ],
                ]])
                .unwrap(),
                MultiPolygon::new(vec![vec![vec![
                    (5.0, 5.1).into(),
                    (6.0, 6.1).into(),
                    (5.0, 6.1).into(),
                    (5.0, 5.1).into(),
                ]]])
                .unwrap(),
            ],
            vec![Default::default(); 2],
            HashMap::new(),
        )
        .unwrap();

        let wrapper = PointInPolygonTesterWithCollection::new(collection);

        assert!(wrapper
            .tester()
            .any_polygon_contains_coordinate(&(5.0, 5.1).into(), &TimeInterval::default()));

        assert_eq!(wrapper.collection().len(), 2);
    }
}
