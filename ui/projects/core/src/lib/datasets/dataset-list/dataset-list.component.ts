import {Component, OnInit, ChangeDetectionStrategy, AfterViewInit, inject, input, viewChild} from '@angular/core';
import {DatasetService} from '../dataset.service';
import {DataSource} from '@angular/cdk/collections';
import {BehaviorSubject, EMPTY, Observable, range, Subject} from 'rxjs';
import {CdkVirtualScrollViewport, CdkFixedSizeVirtualScroll, CdkVirtualForOf} from '@angular/cdk/scrolling';
import {concatMap, filter, first, scan, tap} from 'rxjs/operators';
import {LayoutService} from '../../layout.service';
import {UUID} from '../../backend/backend.model';
import {Dataset} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatNavList} from '@angular/material/list';
import {DatasetComponent} from '../dataset/dataset.component';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-dataset-list',
    templateUrl: './dataset-list.component.html',
    styleUrls: ['./dataset-list.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        CdkVirtualScrollViewport,
        CdkFixedSizeVirtualScroll,
        MatNavList,
        CdkVirtualForOf,
        DatasetComponent,
        MatProgressSpinner,
        AsyncPipe,
    ],
})
export class DatasetListComponent implements OnInit, AfterViewInit {
    datasetService = inject(DatasetService);

    readonly viewport = viewChild.required(CdkVirtualScrollViewport);

    readonly repositoryName = input('Data Repository');

    // TODO: dataset search

    // TODO: ordering of datasets

    readonly itemSizePx = 72;

    readonly loadingSpinnerDiameterPx: number = 3 * LayoutService.remInPx;

    datasetSource?: DatasetDataSource;

    ngOnInit(): void {
        this.datasetSource = new DatasetDataSource(this.datasetService);
    }

    ngAfterViewInit(): void {
        this.datasetSource?.init(this.calculateInitialNumberOfElements());
    }

    /**
     * Fetch new data when scrolled to the end of the list.
     */
    onScrolledIndexChange(_scrolledIndex: number): void {
        const end = this.viewport().getRenderedRange().end;
        const total = this.viewport().getDataLength();

        // only fetch when scrolled to the end
        if (end >= total) {
            this.datasetSource?.fetchMoreData(1);
        }
    }

    trackById(_index: number, dataset: Dataset): UUID {
        return dataset.id;
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
class DatasetDataSource extends DataSource<Dataset> {
    // cannot increase this, since it is limited by the server
    readonly scrollFetchSize = 20;

    readonly loading$ = new BehaviorSubject(false);

    protected nextBatch$ = new Subject<number>();
    protected noMoreData = false;
    protected offset = 0;

    protected getDatasets: (offset: number, limit: number) => Observable<Array<Dataset>>;

    constructor(protected datasetService: DatasetService) {
        super();

        this.getDatasets = (offset, limit): Observable<Array<Dataset>> =>
            datasetService.getDatasets(offset, limit).pipe(
                first(), // first because we only want to fetch once
            );
    }

    init(numberOfElements: number): void {
        this.fetchMoreData(Math.ceil(numberOfElements / this.scrollFetchSize)); // initially populate source
    }

    connect(): Observable<Array<Dataset>> {
        return this.nextBatch$.pipe(
            filter(() => !this.loading$.value),
            concatMap((numberOfTimes) => range(0, numberOfTimes)),
            concatMap(() => this.getMoreDataFromServer()),
            scan((acc, newValues) => [...acc, ...newValues]),
        );
    }

    /**
     * Clean up resources
     */
    disconnect(): void {
        // do nothing
    }

    fetchMoreData(numberOfTimes: number): void {
        this.nextBatch$.next(numberOfTimes);
    }

    protected getMoreDataFromServer(): Observable<Array<Dataset>> {
        if (this.noMoreData) {
            return EMPTY;
        }

        this.loading$.next(true);

        const offset = this.offset;
        const limit = this.scrollFetchSize;

        return this.getDatasets(offset, limit).pipe(
            tap((datasets) => {
                this.offset += datasets.length;

                if (datasets.length < limit) {
                    this.noMoreData = true;
                }

                this.loading$.next(false);
            }),
        );
    }
}
