import {Component, ChangeDetectionStrategy, ElementRef, AfterViewInit, AfterViewChecked, inject} from '@angular/core';
import {SidenavRef} from '../sidenav-ref.service';

@Component({
    selector: 'geoengine-sidenav-header',
    templateUrl: './sidenav-header.component.html',
    styleUrls: ['./sidenav-header.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [],
})
export class SidenavHeaderComponent implements AfterViewInit, AfterViewChecked {
    private elementRef = inject<ElementRef<HTMLElement>>(ElementRef);
    private sidenavRef = inject(SidenavRef);

    ngAfterViewInit(): void {
        this.sidenavRef.setTitle(this.elementRef.nativeElement.textContent ?? undefined);
    }

    ngAfterViewChecked(): void {
        this.sidenavRef.setTitle(this.elementRef.nativeElement.textContent ?? undefined);
    }
}
