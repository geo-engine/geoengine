import {
    Component,
    ViewChild,
    ElementRef,
    ChangeDetectionStrategy,
    OnInit,
    ChangeDetectorRef,
    OnChanges,
    SimpleChanges,
    OnDestroy,
    HostListener,
    inject,
    input,
    output,
    viewChild,
} from '@angular/core';
import {MatInput, MatFormField, MatPrefix, MatLabel} from '@angular/material/input';
import {
    LayerCollectionListing,
    LayerListing,
    ProviderLayerCollectionId,
    SearchCapabilities,
    SearchType,
    SearchTypes,
} from '@geoengine/api-client';
import {BehaviorSubject, Observable, debounceTime, distinctUntilChanged, switchMap} from 'rxjs';
import {LayerCollectionItem, LayerCollectionItemOrSearch, LayerCollectionSearch} from '../layer-collection.model';
import {CommonConfig} from '../../config.service';
import {LayersService} from '../layers.service';
import {UUID} from '../../datasets/dataset.model';
import {CollectionNavigation, LayerCollectionListComponent} from '../layer-collection-list/layer-collection-list.component';
import {MatIcon} from '@angular/material/icon';
import {FormsModule} from '@angular/forms';
import {MatAutocompleteTrigger, MatAutocomplete, MatOption} from '@angular/material/autocomplete';
import {MatButton} from '@angular/material/button';
import {MatTooltip} from '@angular/material/tooltip';
import {MatMenuTrigger, MatMenu} from '@angular/material/menu';
import {MatToolbar} from '@angular/material/toolbar';
import {MatSelect} from '@angular/material/select';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-layer-collection-navigation',
    templateUrl: './layer-collection-navigation.component.html',
    styleUrls: ['./layer-collection-navigation.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatFormField,
        MatIcon,
        MatPrefix,
        MatInput,
        FormsModule,
        MatAutocompleteTrigger,
        MatAutocomplete,
        MatOption,
        MatButton,
        MatTooltip,
        MatMenuTrigger,
        MatMenu,
        MatToolbar,
        MatLabel,
        MatSelect,
        LayerCollectionListComponent,
        AsyncPipe,
    ],
})
export class LayerCollectionNavigationComponent implements OnInit, OnChanges, OnDestroy {
    protected readonly config = inject(CommonConfig);
    protected readonly layerCollectionService = inject(LayersService);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);

    readonly showLayerToggle = input(true);
    readonly collectionNavigation = input(CollectionNavigation.Element);
    readonly highlightSelection = input(false);

    readonly collectionId = input.required<ProviderLayerCollectionId>();

    public readonly scrollElement = viewChild.required('scrollElement', {read: ElementRef});

    readonly layerCollectionListComponent = viewChild.required(LayerCollectionListComponent);

    breadcrumbs: BreadcrumbNavigation = this.createBreadcrumbNavigation();
    search: Search = this.createSearch();

    selectedCollection?: LayerCollectionItemOrSearch;

    readonly selectLayer = output<LayerListing>();
    readonly selectCollection = output<LayerCollectionListing>();
    readonly navigateCollection = output<LayerCollectionListing>();

    ngOnInit(): void {
        this.updateListView(undefined);
    }

    refreshCollection(): void {
        this.layerCollectionListComponent().refreshCollection();
    }

    refresh(): void {
        this.layerCollectionListComponent().refresh();
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (changes.rootCollectionItem) {
            this.breadcrumbs = this.createBreadcrumbNavigation();
            this.search.onDestroy();
            this.search = this.createSearch();
            this.updateListView(undefined);
        }
    }

    ngOnDestroy(): void {
        this.search.onDestroy();
    }

    /** Cast method for the template */
    layerCollectionItem(item: LayerCollectionItemOrSearch): LayerCollectionItem {
        if (item.type === 'collection') {
            return item as LayerCollectionItem;
        }

        throw Error('not a collection item');
    }

    /** Cast method for the template */
    layerCollectionSearch(item: LayerCollectionItemOrSearch): LayerCollectionSearch {
        if (item.type === 'search') {
            return item as LayerCollectionSearch;
        }

        throw Error('not a search item');
    }

    // Focus the search field when it is shown
    @ViewChild('searchInput', {read: MatInput})
    set searchInput(searchInput: MatInput | undefined) {
        searchInput?.focus();
    }

    @HostListener('window:keydown', ['$event'])
    onKeyDown(eventData: KeyboardEvent): void {
        if (!this.search.hasSearchCapabilities) {
            return;
        }

        if (!this.search.isSearching && eventData.key === 'f' && eventData.ctrlKey) {
            this.search.toggleSearch();
            eventData.preventDefault();
        } else if (this.search.isSearching && eventData.key === 'Escape') {
            // Note: for some reason, this event is rarely catched.
            this.search.exitSearch();
            eventData.preventDefault();
        } else if (this.search.isSearching && eventData.key === 'Enter') {
            this.search.toggleSearch();
            eventData.preventDefault();
        }
    }

    navigateToCollection(item: LayerCollectionListing): void {
        this.breadcrumbs.selectCollection({type: 'collection', id: item.id, name: item.name} as LayerCollectionItemOrSearch);
        this.navigateCollection.emit(item);
    }

    get providerLayerCollectionIdOrSearch(): ProviderLayerCollectionId | LayerCollectionSearch | undefined {
        if (this.selectedCollection?.type === 'collection') {
            return this.selectedCollection.id;
        } else if (this.selectedCollection?.type === 'search') {
            return this.selectedCollection as LayerCollectionSearch;
        } else {
            return undefined;
        }
    }

    protected createBreadcrumbNavigation(): BreadcrumbNavigation {
        return new BreadcrumbNavigation(
            (id) => this.updateListView(id),
            (id) => this.selectCollectionInBreadcrumbs(id),
            () => this.scrollToRight(),
        );
    }

    protected createSearch(): Search {
        return new Search({
            layersService: this.layerCollectionService,
            selectedCollection: () => this.selectedCollection?.id ?? this.collectionId(),
            searchResult: (searchResult): void => {
                this.breadcrumbs.selectCollection(searchResult);
            },
            debounceTimeMs: this.config.DELAYS.DEBOUNCE,
            maxAutocompleteResults: 10,
        });
    }

    protected scrollToRight(): void {
        setTimeout(() => {
            // wait until breadcrumbs are re-rendered before scrolling
            this.scrollElement().nativeElement.scrollLeft += this.scrollElement().nativeElement.scrollWidth;
        }, 0);
    }

    protected updateListView(id?: LayerCollectionItemOrSearch): void {
        this.selectedCollection = id ?? {type: 'collection', id: this.collectionId()};

        this.search.updateSearchCapabilities(this.selectedCollection.id).then(() => {
            this.changeDetectorRef.markForCheck();
        });
    }

    protected selectCollectionInBreadcrumbs(id?: LayerCollectionItemOrSearch): void {
        this.selectedCollection = id ?? {type: 'collection', id: this.collectionId()};
        this.navigateCollection.emit(this.selectedCollection as LayerCollectionItem);
    }
}

interface SearchSettings {
    searchType: SearchType;
    filter?: string;
}

const NO_SEARCH_CAPABILITIES: SearchCapabilities = {
    autocomplete: false,
    searchTypes: {
        fulltext: false,
        prefix: false,
    },
    filters: [],
};

class Search {
    searchCapabilities: SearchCapabilities = NO_SEARCH_CAPABILITIES;
    searchSettings: SearchSettings = {
        searchType: SearchType.Fulltext,
        filter: undefined,
    };
    isSearching = false;

    searchString = new BehaviorSubject<string>('');
    autocompleteResults: Observable<Array<string>>;

    protected searchCapabilitiesProviderId: UUID = '';
    protected autocompleteAbortController?: AbortController;

    protected layersService: LayersService;
    protected selectedCollection: () => ProviderLayerCollectionId;
    protected searchResult: (_: LayerCollectionSearch) => void;
    protected maxAutocompleteResults: number;

    constructor({
        layersService,
        selectedCollection,
        searchResult,
        debounceTimeMs,
        maxAutocompleteResults,
    }: {
        readonly layersService: LayersService;
        readonly selectedCollection: () => ProviderLayerCollectionId;
        readonly searchResult: (_: LayerCollectionSearch) => void;
        readonly debounceTimeMs: number;
        readonly maxAutocompleteResults: number;
    }) {
        this.layersService = layersService;
        this.selectedCollection = selectedCollection;
        this.searchResult = searchResult;
        this.maxAutocompleteResults = maxAutocompleteResults;

        this.autocompleteResults = this.searchString.pipe(
            debounceTime(debounceTimeMs),
            distinctUntilChanged(),
            switchMap((searchString) => this.computeAutocompleteResults(searchString)),
        );
    }

    onDestroy(): void {
        this.searchString.complete();
    }

    get hasSearchCapabilities(): boolean {
        const searchTypes = this.searchCapabilities.searchTypes;
        for (const searchType in searchTypes) {
            if (this.searchCapabilities.searchTypes[searchType as keyof SearchTypes]) {
                return true;
            }
        }

        return false;
    }

    toggleSearch(): void {
        if (this.isSearching && this.searchString.value) {
            this.search(this.searchString.value);
        }

        this.isSearching = !this.isSearching;
    }

    exitSearch(): void {
        this.searchString.next('');
        this.isSearching = false;
    }

    async computeAutocompleteResults(searchString: string): Promise<Array<string>> {
        if (searchString.length === 0 || this.searchCapabilities.autocomplete === false) {
            return [];
        }

        this.searchString.next(searchString);

        this.autocompleteAbortController?.abort();
        this.autocompleteAbortController = new AbortController();

        const collection = this.selectedCollection();

        return await this.layersService.autocompleteSearch(
            {
                provider: collection.providerId,
                collection: collection.collectionId,
                searchType: this.searchSettings.searchType,
                searchString,
                limit: this.maxAutocompleteResults,
                offset: 0,
            },
            {abortController: this.autocompleteAbortController},
        );
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    async search(searchString: string): Promise<void> {
        const collection = this.selectedCollection();

        this.searchResult({
            type: 'search',
            id: collection,
            searchType: this.searchSettings.searchType,
            searchString,
            offset: 0,
        } as LayerCollectionSearch);
    }

    async updateSearchCapabilities(layerCollection: ProviderLayerCollectionId): Promise<void> {
        const providerId = layerCollection.providerId;
        if (this.searchCapabilitiesProviderId === providerId) {
            return; // same provider, no need to re-query
        }

        this.searchCapabilities = (await this.layersService.capabilities(providerId)).search;
        this.searchCapabilitiesProviderId = providerId;

        // set some default settings
        this.searchSettings = {
            searchType: SearchType.Fulltext,
            filter: undefined,
        };
        // fulltext is the default, but if it is not supported, use the first supported search type
        if (!this.searchCapabilities.searchTypes.fulltext) {
            const searchTypes = this.searchCapabilities.searchTypes;
            for (const searchType in searchTypes) {
                if (this.searchCapabilities.searchTypes[searchType as keyof SearchTypes]) {
                    this.searchSettings.searchType = searchType as SearchType;
                    break;
                }
            }
        }
    }
}

/**
 * Encapsulates the logic for navigating through the breadcrumb trail.
 */
class BreadcrumbNavigation {
    activeTrail: Array<LayerCollectionItemOrSearch> = [];

    protected collections: Array<LayerCollectionItemOrSearch> = [];
    protected allTrails: Array<Array<LayerCollectionItemOrSearch>> = [];

    protected selectedCollection = -1;

    constructor(
        protected readonly updateListView: (_: LayerCollectionItemOrSearch | undefined) => void,
        protected readonly collectionSelection: (_: LayerCollectionItemOrSearch | undefined) => void,
        protected readonly scrollToRight: () => void,
    ) {}

    selectCollection(id: LayerCollectionItemOrSearch): void {
        if (this.activeTrail[this.activeTrail.length - 1]?.type === 'search' && id.type === 'search') {
            // SPECIAL CASE:
            // if the current item is a search and we get a search, swap instead of append

            this.collections[this.collections.length - 1] = id;
            this.activeTrail[this.activeTrail.length - 1] = id;
        } else {
            // DEFAULT CASE: append

            this.collections = this.collections.splice(0, this.activeTrail.length);
            this.collections.push(id);
            this.selectedCollection += 1;

            // Create a new trail, append it to the collection and display it
            const clone = this.collections.map((x) => Object.assign({}, x));
            this.allTrails = this.allTrails.slice(0, this.selectedCollection);
            this.allTrails.push(clone);
            this.activeTrail = this.allTrails[this.selectedCollection];
        }

        this.scrollToRight();

        this.updateListView(id);
    }

    get isBackDisabled(): boolean {
        return this.selectedCollection < 0;
    }

    get isForwardDisabled(): boolean {
        return this.selectedCollection >= this.allTrails.length - 1;
    }

    back(): void {
        if (this.selectedCollection > 0) {
            this.selectedCollection -= 1;
            this.updateTrailAndCollection();
        } else if (this.selectedCollection === 0) {
            this.activeTrail = [];
            this.updateListView(undefined);
            this.selectedCollection = -1;
        }
        this.emitCollectionSelection();
    }

    forward(): void {
        if (this.selectedCollection < this.allTrails.length - 1) {
            this.selectedCollection += 1;
            this.updateTrailAndCollection();
            this.scrollToRight();
        }
        this.emitCollectionSelection();
    }

    onBreadCrumbClick(index: number): void {
        // Creates and appends a new crumbtrail, then moves forward to it
        if (index === this.activeTrail.length - 1) {
            return;
        }
        const newTrail = this.activeTrail.map((x) => Object.assign({}, x)).slice(0, index + 1);
        this.allTrails.push(newTrail);
        this.selectedCollection = this.allTrails.length - 2;
        this.forward();
    }

    navigateToRoot(): void {
        if (this.selectedCollection === -1) {
            return;
        }
        const newTrail: Array<LayerCollectionItemOrSearch> = [];
        this.allTrails.push(newTrail);
        this.selectedCollection = this.allTrails.length - 2;
        this.forward();
    }

    protected updateTrailAndCollection(): void {
        const currentTrail = this.allTrails[this.selectedCollection];
        this.activeTrail = currentTrail;
        const lastId = currentTrail[currentTrail.length - 1];
        this.updateListView(lastId);
    }

    private emitCollectionSelection(): void {
        if (this.activeTrail.length == 0) {
            this.collectionSelection(undefined);
        } else {
            const collectionId = this.activeTrail[this.activeTrail.length - 1];
            this.collectionSelection(collectionId);
        }
    }
}
