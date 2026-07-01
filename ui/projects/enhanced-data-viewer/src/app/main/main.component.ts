import {
    ChangeDetectionStrategy,
    Component,
    ElementRef,
    afterNextRender,
    computed,
    inject,
    resource,
    signal,
    viewChild,
} from '@angular/core';
import {MatSidenavModule} from '@angular/material/sidenav';
import {ProjectService, MapService, MapContainerComponent, CoreModule} from '@geoengine/core';
import {AppConfig} from '../app-config.service';
import {Layer, LayersService, Time, UserService} from '@geoengine/common';
import {MatToolbar, MatToolbarModule} from '@angular/material/toolbar';
import {MatButtonModule} from '@angular/material/button';
import {MatIconModule} from '@angular/material/icon';
import {MatTooltipModule} from '@angular/material/tooltip';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatRadioModule} from '@angular/material/radio';
import {MatDatepickerInputEvent, MatDatepickerModule} from '@angular/material/datepicker';
import {ProviderLayerId} from '@geoengine/api-client/dist/models/ProviderLayerId';
import {A11yModule} from '@angular/cdk/a11y';

@Component({
    selector: 'geoengine-main',
    templateUrl: './main.component.html',
    styleUrls: ['./main.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        CoreModule,
        MapContainerComponent,
        MatButtonModule,
        MatButtonToggleModule,
        MatDatepickerModule,
        MatIconModule,
        MatRadioModule,
        MatSidenavModule,
        MatToolbarModule,
        MatTooltipModule,
        A11yModule,
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

    readonly sessionToken = toSignal(this.userService.getSessionTokenStream());

    readonly middleContainerHeight = computed(() => this.totalHeight() - this.topToolbarHeight());

    readonly currentTime = toSignal(this.projectService.getTimeStream());
    readonly formattedTime = computed<string>(() => {
        const projectTime = this.currentTime();
        if (!projectTime) return '';
        return projectTime.start.format('DD.MM.YYYY');
    });
    readonly timeStepDuration = toSignal(this.projectService.getTimeStepDurationStream());
    readonly currentDate = computed<Date | undefined>(() => {
        const time = this.currentTime();
        if (!time) return undefined;
        return time.start.toDate();
    });
    readonly spatialReference = toSignal(this.projectService.getSpatialReferenceStream());

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

    async timeForward(): Promise<void> {
        const time = this.currentTime();
        const timeStepDuration = this.timeStepDuration();

        if (!time || !timeStepDuration) return;

        const updatedTime = time.add(timeStepDuration.durationAmount, timeStepDuration.durationUnit);
        await this.projectService.setTime(updatedTime);
    }

    async timeBackwards(): Promise<void> {
        const time = this.currentTime();
        const timeStepDuration = this.timeStepDuration();

        if (!time || !timeStepDuration) return;

        const updatedTime = time.subtract(timeStepDuration.durationAmount, timeStepDuration.durationUnit);
        await this.projectService.setTime(updatedTime);
    }

    async setDate(event: MatDatepickerInputEvent<Date>): Promise<void> {
        if (!event?.value) return;

        const utcDate = new Date(Date.UTC(event.value.getFullYear(), event.value.getMonth(), event.value.getDate()));
        const time = new Time(utcDate);
        await this.projectService.setTime(time);
    }
}
