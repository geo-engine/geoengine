import {DataSource} from '@angular/cdk/collections';
import {CdkFixedSizeVirtualScroll, CdkVirtualForOf, CdkVirtualScrollViewport} from '@angular/cdk/scrolling';
import {AfterContentInit, Component, inject, viewChild, output, input, effect, signal} from '@angular/core';
import {LayerProviderListing} from '@geoengine/api-client';
import {BehaviorSubject, concatMap, firstValueFrom, Observable, range, scan, startWith, Subject} from 'rxjs';
import {LayersService} from '@geoengine/common';
import {MatDialog} from '@angular/material/dialog';
import {AddProviderComponent} from '../add-provider/add-provider.component';
import {AsyncPipe} from '@angular/common';
import {MatListItem, MatNavList} from '@angular/material/list';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {MatIcon} from '@angular/material/icon';
import {MatButton} from '@angular/material/button';

@Component({
    selector: 'geoengine-manager-provider-list',
    templateUrl: './provider-list.component.html',
    styleUrl: './provider-list.component.scss',
    imports: [
        CdkVirtualScrollViewport,
        AsyncPipe,
        MatNavList,
        MatListItem,
        MatProgressSpinner,
        CdkVirtualForOf,
        MatIcon,
        CdkFixedSizeVirtualScroll,
        MatButton,
    ],
})
export class ProviderListComponent implements AfterContentInit {
    readonly viewport = viewChild.required(CdkVirtualScrollViewport);

    readonly selectProvider = output<LayerProviderListing | undefined>();

    addedProvider?: LayerProviderListing;

    readonly itemSizePx = 72;

    readonly loadingSpinnerDiameterPx: number = 3 * parseFloat(getComputedStyle(document.documentElement).fontSize);

    source?: ProviderDataSource;

    readonly selectedProvider$ = input<LayerProviderListing>();
    readonly _selectedProvider$ = signal<LayerProviderListing | undefined>(undefined);

    private readonly layersService = inject(LayersService);
    private readonly dialog = inject(MatDialog);

    constructor() {
        effect(() => {
            this._selectedProvider$.set(this.selectedProvider$());
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

    trackById(_index: number, item: LayerProviderListing): string {
        return item.id;
    }

    select(item?: LayerProviderListing): void {
        this._selectedProvider$.set(item);
        this.selectProvider.emit(item);
    }

    reload(): void {
        this.setUpSource();
    }

    setUpSource(): void {
        this.source = new ProviderDataSource(this.layersService);
        // calculate initial number of elements to display in `setTimeout` because the viewport is not yet initialized
        setTimeout(() => {
            this.source?.init(this.calculateInitialNumberOfElements());
        });
    }

    protected calculateInitialNumberOfElements(): number {
        const element = this.viewport().elementRef.nativeElement;
        const numberOfElements = Math.ceil(element.clientHeight / this.itemSizePx);
        // add one such that scrolling happens
        return numberOfElements + 1;
    }

    async addProvider(): Promise<void> {
        const dialogRef = this.dialog.open(AddProviderComponent, {
            width: '60%',
            height: 'calc(90%)',
            autoFocus: false,
        });

        const id = await firstValueFrom(dialogRef.afterClosed());

        this.reload();

        if (id) {
            this.source?.connect().subscribe((items) => {
                const selectedItem = items.find((item) => item.id === id);
                this.select(selectedItem);
            });
        }
    }
}

/**
 * A custom data source that allows fetching providers for a virtual scroll source.
 */
class ProviderDataSource extends DataSource<LayerProviderListing> {
    // cannot increase this, since it is limited by the server
    readonly scrollFetchSize = 20;

    readonly loading$ = new BehaviorSubject(false);

    protected nextBatch$ = new Subject<number>();
    protected noMoreData = false;
    protected offset = 0;

    constructor(private layersService: LayersService) {
        super();
    }

    init(numberOfElements: number): void {
        this.fetchMoreData(Math.ceil(numberOfElements / this.scrollFetchSize)); // initially populate source
    }

    connect(): Observable<Array<LayerProviderListing>> {
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
        /* do nothing */
    }

    fetchMoreData(numberOfTimes: number): void {
        if (this.noMoreData) {
            return;
        }
        this.nextBatch$.next(numberOfTimes);
    }

    protected async getMoreDataFromServer(): Promise<Array<LayerProviderListing>> {
        if (this.noMoreData) {
            return [];
        }

        this.loading$.next(true);

        const offset = this.offset;
        const limit = this.scrollFetchSize;

        return this.layersService.getProviders(offset, limit).then((items) => {
            this.offset += items.length;

            if (items.length < limit) {
                this.noMoreData = true;
            }

            this.loading$.next(false);

            return items;
        });
    }
}
