import {BehaviorSubject, Subscription} from 'rxjs';
import {filter, first} from 'rxjs/operators';

import {AfterViewInit, ChangeDetectionStrategy, Component, ElementRef, OnDestroy, inject, input} from '@angular/core';

import {OperatorListComponent} from '../../operators/dialogs/operator-list/operator-list.component';
import {ProjectService} from '../../project/project.service';
import {LayoutService} from '../../layout.service';
import {LoadingState} from '../../project/loading-state.model';
import {Plot, AsyncNumberSanitizer, AsyncValueDefault} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatButton} from '@angular/material/button';
import {PlotListEntryComponent} from '../plot-list-entry/plot-list-entry.component';
import {AsyncPipe} from '@angular/common';

/**
 * This component lists all current plots.
 */
@Component({
    selector: 'geoengine-plot-list',
    templateUrl: './plot-list.component.html',
    styleUrls: ['./plot-list.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [SidenavHeaderComponent, MatButton, PlotListEntryComponent, AsyncPipe, AsyncNumberSanitizer, AsyncValueDefault],
})
export class PlotListComponent implements AfterViewInit, OnDestroy {
    readonly projectService = inject(ProjectService);
    private readonly layoutService = inject(LayoutService);
    private readonly elementRef = inject(ElementRef);

    /**
     * If the list is empty, show the following button.
     */
    readonly operatorsListConfig = input({component: OperatorListComponent});

    readonly cardWidth$ = new BehaviorSubject<number | undefined>(undefined);

    readonly defaultLoadingState = LoadingState.LOADING;

    private subscriptions: Array<Subscription> = [];

    ngAfterViewInit(): void {
        this.subscriptions.push(
            this.projectService
                .getPlotStream()
                .pipe(
                    filter((plots) => plots.length > 0),
                    first(),
                )
                .subscribe(() => {
                    setTimeout(() => {
                        const cardContent = this.elementRef.nativeElement.querySelector('mat-card-content');
                        const width = parseInt(getComputedStyle(cardContent).width, 10);
                        this.cardWidth$.next(width);
                    });
                }),
        );
    }

    ngOnDestroy(): void {
        this.subscriptions.forEach((subscription) => subscription.unsubscribe());
    }

    /**
     * Loads the component in `operatorsListConfig` into the sidenav
     */
    goToOperatorsTab(): void {
        this.layoutService.setSidenavContentComponent(this.operatorsListConfig());
    }

    idOfPlot(index: number, plot: Plot): number {
        return plot.id;
    }
}
