import {Observable, BehaviorSubject, first} from 'rxjs';
import {AfterViewInit, ChangeDetectionStrategy, Component, HostListener, OnInit, ViewContainerRef, inject, viewChild} from '@angular/core';
import {MatIconRegistry} from '@angular/material/icon';
import {
    LayoutService,
    ProjectService,
    MapService,
    MapContainerComponent,
    SpatialReferenceService,
    WEB_MERCATOR,
    CoreModule,
} from '@geoengine/core';
import {DomSanitizer} from '@angular/platform-browser';
import {AppConfig} from '../app-config.service';
import {ComponentPortal, CdkPortalOutlet} from '@angular/cdk/portal';
import {DataSelectionService} from '../data-selection.service';
import {SpeciesSelectorComponent} from '../species-selector/species-selector.component';
import {Layer, RandomColorService, UserService, FxFlexDirective} from '@geoengine/common';
import {MatToolbar, MatToolbarRow} from '@angular/material/toolbar';
import {LegendComponent} from '../legend/legend.component';
import {MatSidenav} from '@angular/material/sidenav';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-main',
    templateUrl: './main.component.html',
    styleUrls: ['./main.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatToolbar,
        MatToolbarRow,
        FxFlexDirective,
        CdkPortalOutlet,
        CoreModule,
        LegendComponent,
        MapContainerComponent,
        MatSidenav,
        AsyncPipe,
    ],
})
export class MainComponent implements OnInit, AfterViewInit {
    readonly config = inject<AppConfig>(AppConfig);
    readonly layoutService = inject(LayoutService);
    readonly projectService = inject(ProjectService);
    readonly dataSelectionService = inject(DataSelectionService);
    readonly _vcRef = inject(ViewContainerRef);
    readonly userService = inject(UserService);
    private iconRegistry = inject(MatIconRegistry);
    private _randomColorService = inject(RandomColorService);
    private mapService = inject(MapService);
    private _spatialReferenceService = inject(SpatialReferenceService);
    private sanitizer = inject(DomSanitizer);

    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly layersReverse$: Observable<Array<Layer>>;
    readonly windowHeight$ = new BehaviorSubject<number>(window.innerHeight);

    datasetPortal = new ComponentPortal(SpeciesSelectorComponent);

    constructor() {
        this.layersReverse$ = this.dataSelectionService.layers;
    }

    ngOnInit(): void {
        this.mapService.registerMapComponent(this.mapComponent());
        this.reset();
    }

    ngAfterViewInit(): void {
        // this.reset();
        this.mapComponent().resize();

        // change projection to web mercator if for whatever reasons it is a different one
        this.projectService
            .getSpatialReferenceStream()
            .pipe(first())
            .subscribe({
                next: (spatialReference) => {
                    if (spatialReference.equals(WEB_MERCATOR.spatialReference)) {
                        return;
                    }

                    this.projectService.setSpatialReference(WEB_MERCATOR.spatialReference);
                },
            });
    }

    idFromLayer(index: number, layer: Layer): number {
        return layer.id;
    }

    private reset(): void {
        this.projectService.clearLayers();
        this.projectService.clearPlots();
        // this.projectService.setTime(new Time(moment.utc()));
    }

    @HostListener('window:resize')
    windowHeight(): void {
        this.windowHeight$.next(window.innerHeight);
    }
}
