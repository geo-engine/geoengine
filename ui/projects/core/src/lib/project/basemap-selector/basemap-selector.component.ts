import {Component, ChangeDetectionStrategy, inject} from '@angular/core';
import {MatSelectModule} from '@angular/material/select';
import {BasemapService} from '../../layers/basemap.service';
import {KeyValuePipe} from '@angular/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';

@Component({
    selector: 'geoengine-basemap-selector',
    templateUrl: './basemap-selector.component.html',
    styleUrls: ['./basemap-selector.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatSelectModule, KeyValuePipe, SidenavHeaderComponent],
})
export class BasemapSelectorComponent {
    protected basmapService = inject(BasemapService);

    basemaps = this.basmapService.basemaps;
    selectedBasemap = this.basmapService.selectedBasemapName;
    projections = this.basmapService.basemapProjections;

    selectBasemap(name: string): void {
        this.basmapService.selectBasemap(name);
    }
}
