import {BreakpointObserver, Breakpoints} from '@angular/cdk/layout';
import {AsyncPipe} from '@angular/common';
import {Component, ChangeDetectionStrategy, HostBinding, inject} from '@angular/core';
import {MatGridList, MatGridTile} from '@angular/material/grid-list';
import {Observable, map} from 'rxjs';
import {AsyncStringSanitizer} from '@geoengine/common';

@Component({
    selector: 'geoengine-ebv-attributions',
    templateUrl: './attributions.component.html',
    styleUrls: ['./attributions.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatGridList, MatGridTile, AsyncPipe, AsyncStringSanitizer],
})
export class AttributionsComponent {
    protected breakpointObserver = inject(BreakpointObserver);

    @HostBinding('className') componentClass = 'mat-typography';

    colSpan: Observable<number>;
    rowHeight: Observable<string>;

    constructor() {
        this.colSpan = this.breakpointObserver.observe(Breakpoints.XLarge).pipe(
            map(({matches}) => {
                return matches ? 1 : 2;
            }),
        );
        this.rowHeight = this.colSpan.pipe(
            map((span) => {
                return span === 1 ? '1:1' : '1:2';
            }),
        );
    }
}
