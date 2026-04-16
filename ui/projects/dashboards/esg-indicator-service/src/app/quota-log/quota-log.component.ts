import {DataSource} from '@angular/cdk/collections';
import {
    AfterViewInit,
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    Component,
    signal,
    inject,
    viewChild,
    OnInit,
    OnDestroy,
} from '@angular/core';
import {Observable, Subject, Subscription, tap} from 'rxjs';
import {MatPaginator, MatPaginatorModule} from '@angular/material/paginator';
import {UserService} from '@geoengine/common';
import {MatTableModule} from '@angular/material/table';
import {ComputationQuota, OperatorQuota} from '@geoengine/api-client';
import {MatButtonModule} from '@angular/material/button';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {MatIconModule} from '@angular/material/icon';

@Component({
    selector: 'geoengine-quota-log',
    templateUrl: './quota-log.component.html',
    styleUrl: './quota-log.component.scss',
    imports: [MatTableModule, MatPaginatorModule, MatProgressBarModule, MatButtonModule, MatIconModule],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class QuotaLogComponent implements AfterViewInit, OnInit, OnDestroy {
    private readonly userService = inject(UserService);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);

    readonly paginator = viewChild.required(MatPaginator);

    readonly loadingSpinnerDiameterPx: number = 3 * parseFloat(getComputedStyle(document.documentElement).fontSize);

    source!: QuotaLogDataSource;

    displayedColumns: string[] = ['timestamp', 'computationId', 'workflowId', 'count', 'details'];
    displayedDetailsColumns: string[] = ['name', 'path', 'count'];

    readonly detailsVisible = signal(false);
    readonly details = signal<OperatorQuota[] | undefined>(undefined);

    private refreshInterval: ReturnType<typeof setInterval> | undefined;

    private pageLoadSubscription?: Subscription;

    ngOnInit(): void {
        void this.setUpSource();
    }

    ngAfterViewInit(): void {
        this.pageLoadSubscription = this.paginator()
            .page.pipe(tap(() => void this.loadQuotaLogsPage()))
            .subscribe();
    }

    ngOnDestroy(): void {
        this.pageLoadSubscription?.unsubscribe();
    }

    async showDetails(element: ComputationQuota): Promise<void> {
        this.detailsVisible.set(true);
        const quota = await this.userService.computationQuota(element.computationId);
        this.details.set(quota);
    }

    hideDetails(): void {
        this.details.set(undefined);
        this.detailsVisible.set(false);
    }

    async refresh(): Promise<void> {
        await this.setUpSource();
        this.changeDetectorRef.markForCheck();
    }

    protected async loadQuotaLogsPage(): Promise<void> {
        await this.source.loadQuotaLogs(this.paginator().pageIndex, this.paginator().pageSize);
    }

    protected async setUpSource(): Promise<void> {
        this.source = new QuotaLogDataSource(this.userService, this.paginator());
        await this.source.loadQuotaLogs(0, 5);
    }
}

/**
 * A custom data source that allows fetching datasets for a virtual scroll source.
 */
class QuotaLogDataSource extends DataSource<ComputationQuota> {
    readonly loading = signal(false);

    protected quotas$ = new Subject<Array<ComputationQuota>>();

    constructor(
        private userService: UserService,
        private paginator: MatPaginator,
    ) {
        super();
    }

    connect(): Observable<Array<ComputationQuota>> {
        return this.quotas$.asObservable();
    }

    /**
     * Clean up resources
     */
    disconnect(): void {
        this.quotas$.complete();
    }

    async loadQuotaLogs(pageIndex: number, pageSize: number): Promise<void> {
        this.loading.set(true);

        const logs = await this.userService.computationsQuota(pageIndex * pageSize, pageSize);

        if (this.paginator && logs.length === pageSize) {
            // we do not know the number of items in total, so instead for each full page set the length to show the "next" button
            this.paginator.length = (pageIndex + 1) * pageSize + 1;
        }

        this.loading.set(false);
        this.quotas$.next(logs);
    }
}
