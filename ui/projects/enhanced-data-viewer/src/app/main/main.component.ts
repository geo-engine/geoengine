import {
    ChangeDetectionStrategy,
    Component,
    ElementRef,
    afterNextRender,
    computed,
    inject,
    booleanAttribute,
    input,
    signal,
    viewChild,
    effect,
} from '@angular/core';
import {MatSidenavModule} from '@angular/material/sidenav';
import {ProjectService, MapService, MapContainerComponent, CoreModule, SpatialReferenceService, WGS_84} from '@geoengine/core';
import {AppConfig} from '../app-config.service';
import {Layer, Time, UserService} from '@geoengine/common';
import {MatToolbar, MatToolbarModule} from '@angular/material/toolbar';
import {MatButtonModule} from '@angular/material/button';
import {MatIconModule} from '@angular/material/icon';
import {MatTooltipModule} from '@angular/material/tooltip';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatRadioModule} from '@angular/material/radio';
import {A11yModule} from '@angular/cdk/a11y';
import {MeasureDirective, MeasurementType} from './measure.directive';
import {isActive, Router, RouterModule} from '@angular/router';
import {addCitationToMapImage} from './map-image-export';
import {EdvLayersService} from '../layers/layers.service';

@Component({
    selector: 'geoengine-main',
    templateUrl: './main.component.html',
    styleUrls: ['./main.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        A11yModule,
        CoreModule,
        MapContainerComponent,
        MatButtonModule,
        MatButtonToggleModule,
        MatIconModule,
        MatRadioModule,
        MatSidenavModule,
        MatToolbarModule,
        MatTooltipModule,
        MeasureDirective,
        RouterModule,
    ],
    host: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        '(window:resize)': 'onResize()',
    },
})
export class MainComponent {
    readonly config = inject(AppConfig);
    readonly projectService = inject(ProjectService);
    readonly userService = inject(UserService);
    private readonly mapService = inject(MapService);
    private readonly router = inject(Router);
    private readonly edvLayersService = inject(EdvLayersService);

    private readonly spatialReferenceService = inject(SpatialReferenceService);

    // Bound from the debug query parameter and passed to the layers controls.
    readonly debug = input(false, {transform: booleanAttribute});

    readonly topToolbar = viewChild.required<MatToolbar, ElementRef<HTMLElement>>('topToolbar', {read: ElementRef});
    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly layersReverse = signal<Array<Layer>>([]);

    readonly totalHeight = signal(window.innerHeight);
    readonly topToolbarHeight = signal(64);
    readonly middleContainerHeight = computed(() => this.totalHeight() - this.topToolbarHeight());

    readonly sessionToken = toSignal(this.userService.getSessionTokenStream());
    readonly isGuestUser = toSignal(this.userService.isGuestUserStream(), {initialValue: true});

    readonly spatialReference = toSignal(this.projectService.getSpatialReferenceStream());
    readonly currentTime = toSignal(this.projectService.getTimeStream());

    readonly mapTileLayer = computed(() => this.edvLayersService.mapTileLayer());
    readonly tileLoading = signal(false);
    readonly isLoading = computed(() => this.edvLayersService.catalogueLoading() || this.tileLoading());

    readonly isLayersActive = isActive('/map/layers', this.router);
    readonly isComputeActive = isActive('/map/compute', this.router);
    readonly isDownloadActive = isActive('/map/download', this.router);
    readonly isAboutActive = isActive('/map/about', this.router);

    readonly MeasurementType = MeasurementType;

    readonly mapImageLoading = signal(false);

    readonly testIsVisible = signal(true);

    constructor() {
        afterNextRender({
            read: () => {
                this.mapService.registerMapComponent(this.mapComponent());

                this.zoomToGermany();

                this.onToolbarResize();
                const topToolbarObserver = new ResizeObserver(() => this.onToolbarResize());
                topToolbarObserver.observe(this.topToolbar().nativeElement);
            },
        });

        effect(() => {
            this.edvLayersService.debug.set(this.debug());
        });
    }

    /**
     * Zoom the map to the configured focus extent (Germany) once the project's
     * spatial reference is known. The extent is defined in WGS 84 and is
     * reprojected into the map's projection before fitting the view.
     */
    private zoomToGermany(): void {
        this.projectService.getSpatialReferenceOnce().subscribe((projection) => {
            const extent = this.spatialReferenceService.reprojectExtent(
                this.config.DEFAULTS.FOCUS_EXTENT,
                WGS_84.spatialReference,
                projection,
            );
            this.mapService.zoomTo(extent);
        });
    }

    onTileLoading(loading: boolean): void {
        this.tileLoading.set(loading);
    }

    onResize(): void {
        this.totalHeight.set(window.innerHeight);
    }

    onToolbarResize(): void {
        this.topToolbarHeight.set(this.topToolbar().nativeElement.offsetHeight);
    }

    idFromLayer(index: number, layer: Layer): number {
        return layer.id;
    }

    /**
     * Downloads the current map view as an image.
     */
    async downloadMapImage(): Promise<void> {
        if (this.mapImageLoading()) return;

        const [currentDate] = (this.currentTime()?.toString() ?? new Date().toISOString()).split('T');
        const currentLayer = this.layersReverse().at(-1)?.name ?? 'enhanced-data-viewer-map';
        const citation = replaceCitationPlaceholders(
            this.edvLayersService.selectedDataSource()?.citation ?? '',
            this.currentTime() ?? new Date(),
        );

        this.mapImageLoading.set(true);

        try {
            const mapImage = await this.mapComponent().mapAsImage();
            const mapImageWithCitation = await addCitationToMapImage(mapImage, citation);
            const link = document.createElement('a');
            link.href = mapImageWithCitation;
            link.download = `${currentDate} ${currentLayer}.png`;
            link.click();
            link.remove();
        } finally {
            this.mapImageLoading.set(false);
        }
    }
}
export interface LayerIdPair {
    dataConnectorId: string;
    layerId: string;
}

function replaceCitationPlaceholders(citation: string, currentTime: Time | Date): string {
    let year;
    if (currentTime instanceof Date) {
        year = currentTime.getFullYear().toString();
    } else {
        year = currentTime.toString().substring(0, 4);
    }
    return citation.replace('[Year]', year);
}
