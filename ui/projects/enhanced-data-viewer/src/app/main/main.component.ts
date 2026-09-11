import {
    ChangeDetectionStrategy,
    Component,
    ElementRef,
    afterNextRender,
    computed,
    inject,
    linkedSignal,
    resource,
    signal,
    viewChild,
} from '@angular/core';
import {MatSidenavModule} from '@angular/material/sidenav';
import {ProjectService, MapService, MapContainerComponent, CoreModule} from '@geoengine/core';
import {AppConfig} from '../app-config.service';
import {assertNever, Layer, LayersService, UserService} from '@geoengine/common';
import {MatToolbar, MatToolbarModule} from '@angular/material/toolbar';
import {MatButtonModule} from '@angular/material/button';
import {MatIconModule} from '@angular/material/icon';
import {MatTooltipModule} from '@angular/material/tooltip';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatRadioModule} from '@angular/material/radio';
import {ProviderLayerId} from '@geoengine/api-client/dist/models/ProviderLayerId';
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
    private readonly layerService = inject(LayersService);
    private readonly mapService = inject(MapService);

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

    readonly selectedLayer = linkedSignal(() => this.landCover.value());

    private readonly openTab = signal<Tab>(Tab.Layers);
    readonly tabComponent = computed<ComponentPortal<unknown>>(() => {
        const tab = this.openTab();

        switch (tab) {
            case Tab.Layers:
                return new ComponentPortal(LayersComponent);
            case Tab.Compute:
                return new ComponentPortal(ComputeComponent);
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

    readonly landCover = resource({
        params: () => ({}),
        loader: async ({params: _}) => {
            const connectorId = 'cbb21ee3-d15d-45c5-a175-66964adf4e85';

            const items = await this.layerService.getLayerCollectionItems(connectorId, 'tags:*');

            const landCover = items.items.find((item) => item.name === 'Land Cover');

            if (!landCover) return;

            const id = landCover.id as ProviderLayerId;

            return {
                dataConnectorId: id.providerId,
                layerId: id.layerId,
            };
        },
    });
    readonly modisNdvi = resource({
        params: () => ({}),
        loader: async ({params: _}) => {
            const connectorId = 'cbb21ee3-d15d-45c5-a175-66964adf4e85';

            const items = await this.layerService.getLayerCollectionItems(connectorId, 'tags:*');

            const modisNdvi = items.items.find((item) => item.name === 'NDVI');

            if (!modisNdvi) {
                console.error('Could not find MODIS NDVI layer in collection');
                return;
            }

            const id = modisNdvi.id as ProviderLayerId;

            return {
                dataConnectorId: id.providerId,
                layerId: id.layerId,
            };
        },
    });
    readonly testIsVisible = signal(true);

    constructor() {
        afterNextRender({
            read: () => {
                this.mapService.registerMapComponent(this.mapComponent());

                this.onToolbarResize();
                const topToolbarObserver = new ResizeObserver(() => this.onToolbarResize());
                topToolbarObserver.observe(this.topToolbar().nativeElement);
            },
        });

        // setTimeout(() => {
        //     this.testIsVisible.set(false);
        //     setTimeout(() => {
        //         this.testIsVisible.set(true);
        //     }, 5000);
        // }, 5000);
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
