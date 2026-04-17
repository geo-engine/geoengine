import {Component, ChangeDetectionStrategy, ElementRef, Directive, inject, output, contentChildren, effect} from '@angular/core';
import {SidenavRef} from '../sidenav-ref.service';

@Directive({selector: '[geoengineSidenavSearchRight]'})
export class SidenavSearchRightDirective {}

@Component({
    selector: 'geoengine-sidenav-search',
    templateUrl: './sidenav-search.component.html',
    styleUrls: ['./sidenav-search.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SidenavSearchComponent {
    private sidenavRef = inject(SidenavRef);

    readonly children = contentChildren(SidenavSearchRightDirective, {read: ElementRef});

    readonly searchString = output<string>();

    constructor() {
        effect(() => {
            this.sidenavRef.setSearch(this.children(), this.searchString);
        });
    }
}
