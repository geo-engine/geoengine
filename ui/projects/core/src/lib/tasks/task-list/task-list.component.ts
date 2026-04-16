import {
    Component,
    ChangeDetectionStrategy,
    AfterViewInit,
    OnDestroy,
    ChangeDetectorRef,
    Injectable,
    inject,
    viewChild,
} from '@angular/core';
import {MatPaginator, MatPaginatorIntl, PageEvent} from '@angular/material/paginator';
import {combineLatest, forkJoin, map, mergeMap, of, startWith, Subject, Subscription, switchMap} from 'rxjs';
import {TaskStatusDict, TaskStatusType, UUID} from '../../backend/backend.model';
import {BackendService} from '../../backend/backend.service';
import {NotificationService, UserService} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatFormField, MatLabel} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatMiniFabButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {
    MatTable,
    MatColumnDef,
    MatHeaderCellDef,
    MatHeaderCell,
    MatCellDef,
    MatCell,
    MatHeaderRowDef,
    MatHeaderRow,
    MatRowDef,
    MatRow,
} from '@angular/material/table';
import {MatTooltip} from '@angular/material/tooltip';
import {JsonPipe, TitleCasePipe} from '@angular/common';

@Injectable()
export class MyCustomPaginatorIntl implements MatPaginatorIntl {
    changes = new Subject<void>();

    // For internationalization, the `$localize` function from
    // the `@angular/localize` package can be used.
    firstPageLabel = `First page`;
    itemsPerPageLabel = `Items per page:`;
    lastPageLabel = `Last page`;

    // You can set labels to an arbitrary string too, or dynamically compute
    // it through other third-party internationalization libraries.
    nextPageLabel = 'Next page';
    previousPageLabel = 'Previous page';

    getRangeLabel(page: number, pageSize: number, length: number): string {
        if (length === 0) {
            return `Page 1 of 1`;
        }
        const amountPages = Math.ceil(length / pageSize);
        const plusPages = Number.isInteger(length) ? '' : '+';
        return `Page ${page + 1} of ${amountPages}${plusPages}`;
    }
}

@Component({
    selector: 'geoengine-task-list',
    templateUrl: './task-list.component.html',
    styleUrls: ['./task-list.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [{provide: MatPaginatorIntl, useClass: MyCustomPaginatorIntl}],
    imports: [
        SidenavHeaderComponent,
        MatFormField,
        MatLabel,
        MatSelect,
        MatOption,
        MatMiniFabButton,
        MatIcon,
        MatProgressSpinner,
        MatTable,
        MatColumnDef,
        MatHeaderCellDef,
        MatHeaderCell,
        MatCellDef,
        MatCell,
        MatTooltip,
        MatButton,
        MatHeaderRowDef,
        MatHeaderRow,
        MatRowDef,
        MatRow,
        MatPaginator,
        JsonPipe,
        TitleCasePipe,
    ],
})
export class TaskListComponent implements AfterViewInit, OnDestroy {
    protected readonly userService = inject(UserService);
    protected readonly backend = inject(BackendService);
    protected readonly changeDetectorRef = inject(ChangeDetectorRef);
    protected readonly notificationService = inject(NotificationService);

    taskStatusOptions: Array<TaskStatusType> = ['running', 'completed', 'aborted', 'failed'];
    pageSize = 15; // current backend does not allow > 20 and we need one more for the next page check

    displayedColumns: string[] = ['status', 'timeStarted', 'time', 'info', 'cleanUp', 'actions'];
    tasks: Array<TaskStatusDict> = [];
    filter?: TaskStatusType = undefined;

    isLoading = true;

    readonly paginator = viewChild.required(MatPaginator);

    protected taskSubscription?: Subscription;

    ngOnDestroy(): void {
        this.taskSubscription?.unsubscribe();
    }

    ngAfterViewInit(): void {
        this.taskSubscription = combineLatest({
            sessionToken: this.userService.getSessionTokenStream(),
            pageEvent: this.paginator().page.pipe(
                startWith({
                    pageIndex: this.paginator().pageIndex,
                    pageSize: this.paginator().pageSize,
                    length: this.paginator().length,
                } as PageEvent),
            ),
        })
            .pipe(
                switchMap(({sessionToken, pageEvent}) => {
                    this.isLoading = true;

                    return forkJoin({
                        pageEvent: of(pageEvent),
                        tasks: this.backend.getTasksList(
                            sessionToken,
                            this.filter,
                            pageEvent.pageIndex * pageEvent.pageSize,
                            // we query one more to check if there is a next page
                            pageEvent.pageSize + 1,
                        ),
                    });
                }),
                map(({pageEvent, tasks}) => {
                    // Flip flag to show that loading has finished.
                    this.isLoading = false;

                    let newTotalResults = pageEvent.pageSize * pageEvent.pageIndex + tasks.length;
                    if (tasks.length > pageEvent.pageSize) {
                        // indicate that there is a next page
                        newTotalResults -= 0.5;
                    }
                    this.paginator().length = Math.max(this.paginator().length, newTotalResults);

                    // shrink to page since, since we queried one more
                    return tasks.slice(0, pageEvent.pageSize);
                }),
            )
            .subscribe({
                next: (tasks) => {
                    this.tasks = tasks;
                    this.changeDetectorRef.markForCheck();
                },
            });
    }

    setFilter(taskStatus?: TaskStatusType): void {
        this.filter = taskStatus;

        // reset paginator and trigger reload
        const paginator = this.paginator();

        paginator.pageIndex = 0;
        paginator.length = 0;

        paginator.page.emit({
            pageIndex: paginator.pageIndex,
            pageSize: paginator.pageSize,
            length: paginator.length,
        } as PageEvent);
    }

    refreshPage(): void {
        this.paginator().page.emit({
            pageIndex: this.paginator().pageIndex,
            pageSize: this.paginator().pageSize,
            length: this.paginator().length,
        } as PageEvent);
    }

    abortTask(taskId: UUID): void {
        this.userService
            .getSessionTokenForRequest()
            .pipe(mergeMap((sessionToken) => this.backend.abortTask(sessionToken, taskId)))
            .subscribe({
                next: () => {
                    this.refreshPage();
                },
                error: (error) => {
                    if (error.message) {
                        this.notificationService.error(error.message);
                    }
                    this.refreshPage();
                },
            });
    }

    localTime(time: string): string {
        return new Date(time).toLocaleString();
    }
}
