import {LayerCollectionListing, ProviderLayerCollectionId, SearchType} from '@geoengine/api-client';

export interface LayerCollectionItemOrSearch {
    type: 'collection' | 'search';
    id: ProviderLayerCollectionId;
}

// TODO: use model from OpenAPI client
export interface LayerCollectionItem extends LayerCollectionItemOrSearch, LayerCollectionListing {
    type: 'collection';
}

export interface LayerCollectionSearch extends LayerCollectionItemOrSearch {
    type: 'search';
    searchType: SearchType;
    searchString: string;
}
