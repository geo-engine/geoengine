import {Observable, BehaviorSubject, mergeMap} from 'rxjs';
import {AfterViewInit, ChangeDetectionStrategy, Component, HostListener, OnInit, ViewContainerRef, inject, viewChild} from '@angular/core';
import {MatIconRegistry, MatIcon} from '@angular/material/icon';
import {
    LayoutService,
    ProjectService,
    MapService,
    MapContainerComponent,
    SpatialReferenceService,
    SidenavContainerComponent,
    TimeStepSelectorComponent,
    ZoomHandlesComponent,
    OlVectorLayerComponent,
    OlRasterLayerComponent,
} from '@geoengine/core';
import {DomSanitizer} from '@angular/platform-browser';
import {AppConfig} from './app-config.service';
import {ComponentPortal, CdkPortalOutlet} from '@angular/cdk/portal';
import moment from 'moment';
import {DataSelectionService} from './data-selection.service';
import {EbvSelectorComponent} from './ebv-selector/ebv-selector.component';
import {MatDrawerToggleResult, MatSidenav, MatSidenavContainer} from '@angular/material/sidenav';
import {Layer, RandomColorService, Time, UserService, AsyncStringSanitizer, AsyncValueDefault} from '@geoengine/common';
import {MatToolbar} from '@angular/material/toolbar';
import {MatButton} from '@angular/material/button';
import {LegendComponent} from './legend/legend.component';
import {MatProgressBar} from '@angular/material/progress-bar';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatSidenavContainer,
        MatToolbar,
        CdkPortalOutlet,
        MatButton,
        MatIcon,
        TimeStepSelectorComponent,
        ZoomHandlesComponent,
        LegendComponent,
        MapContainerComponent,
        OlVectorLayerComponent,
        OlRasterLayerComponent,
        MatSidenav,
        SidenavContainerComponent,
        MatProgressBar,
        AsyncPipe,
        AsyncStringSanitizer,
        AsyncValueDefault,
    ],
})
export class AppComponent implements OnInit, AfterViewInit {
    readonly config = inject<AppConfig>(AppConfig);
    readonly layoutService = inject(LayoutService);
    readonly projectService = inject(ProjectService);
    readonly dataSelectionService = inject(DataSelectionService);
    readonly vcRef = inject(ViewContainerRef);
    readonly userService = inject(UserService);
    private iconRegistry = inject(MatIconRegistry);
    private _randomColorService = inject(RandomColorService);
    private mapService = inject(MapService);
    private _spatialReferenceService = inject(SpatialReferenceService);
    private sanitizer = inject(DomSanitizer);

    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly rightSidenav = viewChild.required(MatSidenav);
    readonly rightSidenavContainer = viewChild.required(SidenavContainerComponent);

    readonly layersReverse$: Observable<Array<Layer>>;
    readonly analysisVisible$ = new BehaviorSubject(false);
    readonly windowHeight$ = new BehaviorSubject<number>(window.innerHeight);

    datasetPortal = new ComponentPortal(EbvSelectorComponent);

    constructor() {
        this.registerIcons();

        this.layersReverse$ = this.dataSelectionService.layers;
    }

    ngOnInit(): void {
        this.mapService.registerMapComponent(this.mapComponent());

        this.layoutService.getSidenavContentComponentStream().subscribe((sidenavConfig) => {
            this.rightSidenavContainer().load(sidenavConfig);

            let openClosePromise: Promise<MatDrawerToggleResult>;
            if (sidenavConfig) {
                openClosePromise = this.rightSidenav().open();
            } else {
                openClosePromise = this.rightSidenav().close();
            }

            openClosePromise.then(() => this.mapComponent().resize());
        });
    }

    ngAfterViewInit(): void {
        this.reset();
        this.mapComponent().resize();
    }

    idFromLayer(index: number, layer: Layer): number {
        return layer.id;
    }

    showAnalysis(): void {
        this.analysisVisible$.next(true);
    }

    private reset(): void {
        this.projectService
            .clearLayers()
            .pipe(
                mergeMap(() => this.projectService.clearPlots()),
                mergeMap(() => this.projectService.setTime(new Time(moment.utc()))),
            )
            .subscribe();
    }

    private registerIcons(): void {
        this.iconRegistry.addSvgIconInNamespace(
            'geoengine',
            'logo',
            this.sanitizer.bypassSecurityTrustResourceUrl('assets/geoengine-white.svg'),
        );

        // used for navigation
        this.iconRegistry.addSvgIcon('cogs', this.sanitizer.bypassSecurityTrustResourceUrl('assets/icons/cogs.svg'));
    }

    @HostListener('window:resize')
    windowHeight(): void {
        this.windowHeight$.next(window.innerHeight);
    }
}
