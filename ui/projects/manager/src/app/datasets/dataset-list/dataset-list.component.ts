import {DataSource} from '@angular/cdk/collections';
import {CdkVirtualScrollViewport, CdkFixedSizeVirtualScroll, CdkVirtualForOf} from '@angular/cdk/scrolling';
import {AfterContentInit, Component, inject, output, viewChild} from '@angular/core';
import {MatDialog} from '@angular/material/dialog';
import {DatasetsService} from '@geoengine/common';
import {DatasetListing} from '@geoengine/api-client';
import {
    BehaviorSubject,
    Observable,
    Subject,
    concatMap,
    debounceTime,
    distinctUntilChanged,
    firstValueFrom,
    range,
    scan,
    skip,
    startWith,
} from 'rxjs';
import {AddDatasetComponent} from '../add-dataset/add-dataset.component';
import {MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatNavList, MatListItem, MatListItemTitle, MatListItemLine} from '@angular/material/list';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';
import {FormsModule} from '@angular/forms';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-manager-dataset-list',
    templateUrl: './dataset-list.component.html',
    styleUrl: './dataset-list.component.scss',
    imports: [
        MatButton,
        MatIcon,
        MatNavList,
        MatListItem,
        MatListItemTitle,
        MatListItemLine,
        MatFormField,
        MatLabel,
        MatInput,
        FormsModule,
        CdkVirtualScrollViewport,
        CdkFixedSizeVirtualScroll,
        CdkVirtualForOf,
        MatProgressSpinner,
        AsyncPipe,
    ],
})
export class DatasetListComponent implements AfterContentInit {
    private readonly datasetsService = inject(DatasetsService);
    private readonly dialog = inject(MatDialog);

    readonly viewport = viewChild.required(CdkVirtualScrollViewport);

    readonly selectDataset = output<DatasetListing | undefined>();

    searchName = '';

    addedDataset?: DatasetListing;

    readonly itemSizePx = 72;

    readonly loadingSpinnerDiameterPx: number = 3 * parseFloat(getComputedStyle(document.documentElement).fontSize);

    source?: DatasetDataSource;

    selectedDataset$ = new BehaviorSubject<DatasetListing | undefined>(undefined);

    private searchSubject$ = new BehaviorSubject<string | undefined>(undefined);

    constructor() {
        this.searchSubject$.pipe(skip(1), debounceTime(500), distinctUntilChanged()).subscribe((_searchText) => {
            this.setUpSource();
        });
    }

    ngAfterContentInit(): void {
        this.setUpSource();
    }

    /**
     * Fetch new data when scrolled to the end of the list.
     */
    onScrolledIndexChange(_scrolledIndex: number): void {
        const end = this.viewport().getRenderedRange().end;
        const total = this.viewport().getDataLength();

        // only fetch when scrolled to the end
        if (end >= total) {
            this.source?.fetchMoreData(1);
        }
    }

    trackById(_index: number, item: DatasetListing): string {
        return item.id;
    }

    select(item?: DatasetListing): void {
        this.selectedDataset$.next(item);
        this.selectDataset.emit(item);
    }

    onSearchChange(event: Event): void {
        if (event.target instanceof HTMLInputElement) {
            const searchValue = event.target.value;
            if (searchValue === '') {
                this.searchSubject$.next(undefined);
            } else {
                this.searchSubject$.next(searchValue);
            }
        }
    }

    setUpSource(): void {
        this.source = new DatasetDataSource(this.datasetsService, this.searchSubject$.value);
        // calculate initial number of elements to display in `setTimeout` because the viewport is not yet initialized
        setTimeout(() => {
            this.source?.init(this.calculateInitialNumberOfElements());
        });
    }

    backToAllDatasets(): void {
        this.addedDataset = undefined;
        this.select(undefined);
        this.setUpSource();
    }

    async addDataset(): Promise<void> {
        const dialogRef = this.dialog.open(AddDatasetComponent, {
            width: '60%',
            height: 'calc(90%)',
            autoFocus: false,
            disableClose: true,
        });

        const datasetName = await firstValueFrom(dialogRef.afterClosed());

        if (!datasetName) {
            return;
        }

        const dataset = await this.datasetsService.getDataset(datasetName);
        const listing = {
            description: dataset.description,
            displayName: dataset.displayName,
            id: dataset.id,
            name: dataset.name,
            provenance: dataset.provenance,
            resultDescriptor: dataset.resultDescriptor,
            sourceOperator: dataset.sourceOperator,
            symbology: dataset.symbology,
            tags: dataset.tags,
        } as DatasetListing;

        this.addedDataset = listing;
        this.select(listing);
    }

    protected calculateInitialNumberOfElements(): number {
        const element = this.viewport().elementRef.nativeElement;
        const numberOfElements = Math.ceil(element.clientHeight / this.itemSizePx);
        // add one such that scrolling happens
        return numberOfElements + 1;
    }
}

/**
 * A custom data source that allows fetching datasets for a virtual scroll source.
 */
class DatasetDataSource extends DataSource<DatasetListing> {
    // cannot increase this, since it is limited by the server
    readonly scrollFetchSize = 20;

    readonly loading$ = new BehaviorSubject(false);

    protected nextBatch$ = new Subject<number>();
    protected noMoreData = false;
    protected offset = 0;

    constructor(
        private datasetsService: DatasetsService,
        private filterValue?: string,
    ) {
        super();
    }

    init(numberOfElements: number): void {
        this.fetchMoreData(Math.ceil(numberOfElements / this.scrollFetchSize)); // initially populate source
    }

    connect(): Observable<Array<DatasetListing>> {
        return this.nextBatch$.pipe(
            concatMap((numberOfTimes) => range(0, numberOfTimes)),
            concatMap(() => this.getMoreDataFromServer()),
            scan((acc, newValues) => [...acc, ...newValues]),
            startWith([]), // emit empty array initially to trigger loading animation properly
        );
    }

    /**
     * Clean up resources
     */
    disconnect(): void {
        // do nothing
    }

    fetchMoreData(numberOfTimes: number): void {
        if (this.noMoreData) {
            return;
        }
        this.nextBatch$.next(numberOfTimes);
    }

    protected async getMoreDataFromServer(): Promise<Array<DatasetListing>> {
        if (this.noMoreData) {
            return [];
        }

        this.loading$.next(true);

        const offset = this.offset;
        const limit = this.scrollFetchSize;

        return this.datasetsService.getDatasets(offset, limit, this.filterValue).then((items) => {
            this.offset += items.length;

            if (items.length < limit) {
                this.noMoreData = true;
            }

            this.loading$.next(false);

            return items;
        });
    }
}
