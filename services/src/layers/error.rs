use super::listing::LayerCollectionId;
use crate::api::model::datatypes::LayerId;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(
    visibility(pub(crate)),
    context(suffix(false)) /* disables default `Snafu` suffix */,
)]
pub enum LayerDbError {
    #[snafu(display("There is no layer with the given id {id}"))]
    NoLayerForGivenId { id: LayerId },

    #[snafu(display("There is no layer collection with the given id {id}"))]
    NoLayerCollectionForGivenId { id: LayerCollectionId },

    #[snafu(display("There is no layer {layer} in collection {collection}"))]
    NoLayerForGivenIdInCollection {
        collection: LayerCollectionId,
        layer: LayerId,
    },

    #[snafu(display("There is no collection {collection} in collection {parent}"))]
    NoCollectionForGivenIdInCollection {
        collection: LayerCollectionId,
        parent: LayerCollectionId,
    },

    #[snafu(display("You must not remove the root collection"))]
    CannotRemoveRootCollection,
}
