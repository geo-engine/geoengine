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
} from '@angular/core';
import {MatSidenavModule} from '@angular/material/sidenav';
import {ProjectService, MapService, MapContainerComponent, CoreModule, SpatialReferenceService, WGS_84} from '@geoengine/core';
import {AppConfig} from '../app-config.service';
import {assertNever, Layer, UserService} from '@geoengine/common';
import {MatToolbar, MatToolbarModule} from '@angular/material/toolbar';
import {MatButtonModule} from '@angular/material/button';
import {MatIconModule} from '@angular/material/icon';
import {MatTooltipModule} from '@angular/material/tooltip';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatRadioModule} from '@angular/material/radio';
import {A11yModule} from '@angular/cdk/a11y';
import {MeasureDirective, MeasurementType} from './measure.directive';
import {ComponentPortal} from '@angular/cdk/portal';
import {LayersComponent} from '../layers/layers.component';
import {ComputeComponent} from '../compute/compute.component';

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
        LayersComponent,
        ComputeComponent,
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

    private readonly spatialReferenceService = inject(SpatialReferenceService);

    // Bound from the debug query parameter and passed to the layers controls.
    readonly debug = input(false, {transform: booleanAttribute});
    readonly layersComponent = viewChild(LayersComponent);

    readonly topToolbar = viewChild.required<MatToolbar, ElementRef<HTMLElement>>('topToolbar', {read: ElementRef});
    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly layersReverse = signal<Array<Layer>>([]);

    readonly totalHeight = signal(window.innerHeight);
    readonly topToolbarHeight = signal(64);
    readonly middleContainerHeight = computed(() => this.totalHeight() - this.topToolbarHeight());

    readonly sessionToken = toSignal(this.userService.getSessionTokenStream());

    readonly spatialReference = toSignal(this.projectService.getSpatialReferenceStream());
    readonly currentTime = toSignal(this.projectService.getTimeStream());

    readonly mapTileLayer = computed(() => this.layersComponent()?.mapTileLayer());
    readonly tileLoading = signal(false);
    readonly isLoading = computed(() => (this.layersComponent()?.mapTileLayerResource.isLoading() ?? false) || this.tileLoading());

    private readonly openTab = signal<Tab>(Tab.Layers);
    readonly tabComponent = computed<ComponentPortal<unknown>>(() => {
        const tab = this.openTab();

        switch (tab) {
            case Tab.Layers:
                return new ComponentPortal(EmptyComponent);
            case Tab.Compute:
                return new ComponentPortal(EmptyComponent);
            case Tab.Search:
                // TODO: create component
                return new ComponentPortal(EmptyComponent);
            case Tab.About:
                // TODO: create component
                return new ComponentPortal(EmptyComponent);
            default:
                assertNever(tab);
        }
    });
    readonly isLayersActive = computed(() => this.openTab() === Tab.Layers);
    readonly isComputeActive = computed(() => this.openTab() === Tab.Compute);
    readonly isSearchActive = computed(() => this.openTab() === Tab.Search);
    readonly isAboutActive = computed(() => this.openTab() === Tab.About);

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

        this.mapImageLoading.set(true);

        try {
            const mapImage = await this.mapComponent().mapAsImage();
            const link = document.createElement('a');
            link.href = mapImage;
            link.download = `${currentDate} ${currentLayer}.png`;
            link.click();
            link.remove();
        } finally {
            this.mapImageLoading.set(false);
        }
    }

    openLayersTab(): void {
        this.openTab.set(Tab.Layers);
    }

    openComputeTab(): void {
        this.openTab.set(Tab.Compute);
    }

    openSearchTab(): void {
        this.openTab.set(Tab.Search);
    }

    openAboutTab(): void {
        this.openTab.set(Tab.About);
    }
}

enum Tab {
    Layers,
    Compute,
    Search,
    About,
}

@Component({
    standalone: true,
    template: '', // Renders nothing
})
export class EmptyComponent {}

export interface LayerIdPair {
    dataConnectorId: string;
    layerId: string;
}
