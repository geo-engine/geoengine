import {Observable, BehaviorSubject, of, concat} from 'rxjs';
import {map, mergeMap, tap} from 'rxjs/operators';
import {
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    Component,
    ElementRef,
    ViewContainerRef,
    afterNextRender,
    computed,
    effect,
    inject,
    signal,
    viewChild,
} from '@angular/core';
import {MatSidenav, MatSidenavContainer} from '@angular/material/sidenav';
import {MatTabGroup} from '@angular/material/tabs';
import {
    AddDataComponent,
    AddDataButton,
    SidenavContainerComponent,
    LayoutService,
    ProjectService,
    NavigationButton,
    NavigationComponent,
    MapService,
    MapContainerComponent,
    WorkspaceSettingsComponent,
    OperatorListComponent,
    OperatorListButtonGroups,
    TimeConfigComponent,
    PlotListComponent,
    SidenavConfig,
    TaskListComponent,
    CoreModule,
} from '@geoengine/core';
import {AppConfig} from '../app-config.service';
import {ReplaySubject} from 'rxjs';
import {Layer, LayersService, UserService, AsyncNumberSanitizer, AsyncValueDefault} from '@geoengine/common';
import {MatToolbar} from '@angular/material/toolbar';
import {MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatTooltip} from '@angular/material/tooltip';
import {AsyncPipe} from '@angular/common';
import {rxResource, toObservable} from '@angular/core/rxjs-interop';

@Component({
    selector: 'geoengine-main',
    templateUrl: './main.component.html',
    styleUrls: ['./main.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatToolbar,
        CoreModule,
        MatButton,
        MatIcon,
        MatTooltip,
        MatSidenavContainer,
        MatSidenav,
        MapContainerComponent,
        AsyncPipe,
        AsyncNumberSanitizer,
        AsyncValueDefault,
    ],
    host: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        '(window:resize)': 'onResize()',
    },
})
export class MainComponent {
    readonly config = inject(AppConfig);
    readonly layoutService = inject(LayoutService);
    readonly projectService = inject(ProjectService);
    readonly vcRef = inject(ViewContainerRef);
    readonly userService = inject(UserService);
    private readonly layerService = inject(LayersService);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);
    private readonly mapService = inject(MapService);

    readonly mapComponent = viewChild.required(MapContainerComponent);
    readonly bottomTabs = viewChild.required(MatTabGroup);

    readonly rightSidenav = viewChild.required(MatSidenav);
    readonly sidenavContainerElement = viewChild.required<MatSidenavContainer, ElementRef<HTMLElement>>(MatSidenavContainer, {
        read: ElementRef,
    });
    readonly rightSidenavContainer = viewChild.required(SidenavContainerComponent);
    private readonly sidenavConfig = rxResource({
        stream: () => this.layoutService.getSidenavContentComponentStream(),
    });

    readonly layersReverse$: Observable<Array<Layer>>;
    readonly layerListVisible$: Observable<boolean>;
    readonly layerDetailViewVisible$: Observable<boolean>;

    readonly addDataConfig = new BehaviorSubject<SidenavConfig | undefined>(undefined);
    readonly navigationButtons = new ReplaySubject<Array<NavigationButton>>(1);
    readonly AddDataComponent = AddDataComponent;

    middleContainerHeight$: Observable<number>;
    bottomContainerHeight$: Observable<number>;
    layerListHeight$: Observable<number>;
    mapIsGrid$: Observable<boolean>;

    private readonly windowHeight$ = signal<number>(window.innerHeight);
    private readonly totalHeight$ = computed(() => {
        this.windowHeight$();
        return this.sidenavContainerElement().nativeElement.offsetHeight;
    });

    constructor() {
        const config = this.config;
        const vcRef = this.vcRef;

        vcRef.length; // eslint-disable-line @typescript-eslint/no-unused-expressions

        this.layersReverse$ = this.projectService.getLayerStream().pipe(map((layers) => layers.slice(0).reverse()));

        this.layerListVisible$ = this.layoutService.getLayerListVisibilityStream();
        this.layerDetailViewVisible$ = this.layoutService.getLayerDetailViewVisibilityStream();

        this.mapIsGrid$ = this.mapService.isGrid$;

        this.middleContainerHeight$ = this.layoutService
            .getMapHeightStream(toObservable(this.totalHeight$))
            .pipe(tap(() => this.mapComponent().resize()));
        this.layerListHeight$ = config.COMPONENTS.MAP_RESOLUTION_EXTENT_OVERLAY.AVAILABLE
            ? this.middleContainerHeight$.pipe(map((height) => height - 62))
            : this.middleContainerHeight$;
        this.bottomContainerHeight$ = this.layoutService.getLayerDetailViewStream(toObservable(this.totalHeight$));

        this.createAddDataConfigStream().subscribe((addDataConfig) => this.addDataConfig.next(addDataConfig));
        this.createNavigationButtonStream().subscribe((navigationButtons) => {
            this.navigationButtons.next(navigationButtons);
            // loading spinners somewhat don't show up without this
            setTimeout(() => this.changeDetectorRef.detectChanges());
        });

        effect(() => {
            const sidenavConfig = this.sidenavConfig.value();

            this.rightSidenavContainer().load(sidenavConfig);
            const rightSidenav = this.rightSidenav();

            const shouldBeOpen = !!sidenavConfig;

            if (shouldBeOpen !== rightSidenav.opened) {
                void (shouldBeOpen ? rightSidenav.open() : rightSidenav.close()).then(() => {
                    this.mapComponent().resize();
                });
            }
        });

        afterNextRender(() => {
            this.mapService.registerMapComponent(this.mapComponent());

            this.layoutService.setLayerDetailViewVisibility(false);

            this.onResize();

            this.projectService
                .getNewPlotStream()
                .subscribe(() => this.layoutService.setSidenavContentComponent({component: PlotListComponent}));
        });
    }

    setTabIndex(index: number): void {
        this.layoutService.setLayerDetailViewTabIndex(index);
        this.layoutService.setLayerDetailViewVisibility(true);
    }

    idFromLayer(index: number, layer: Layer): number {
        return layer.id;
    }

    private createNavigationButtonStream(): Observable<Array<NavigationButton>> {
        return this.addDataConfig.pipe(
            map((addDataConfig) => [
                NavigationComponent.createLoginButton(this.userService, this.layoutService, this.config),
                addDataConfig
                    ? NavigationComponent.createAddDataButton(addDataConfig)
                    : NavigationComponent.createLoadingButton('add data'),
                {
                    sidenavConfig: {component: OperatorListComponent, config: {operators: MainComponent.createOperatorListButtons()}},
                    icon: {
                        type: 'svg',
                        name: 'cogs',
                    },
                    tooltip: 'Operators',
                },
                {
                    sidenavConfig: {component: PlotListComponent},
                    icon: {
                        type: 'icon',
                        name: 'equalizer',
                    },
                    tooltip: 'Plots',
                },
                {
                    sidenavConfig: {component: TaskListComponent},
                    icon: {
                        type: 'icon',
                        name: 'assignment',
                    },
                    tooltip: 'Tasks',
                },
                {
                    sidenavConfig: {component: TimeConfigComponent},
                    icon: {
                        type: 'icon',
                        name: 'access_time',
                    },
                    tooltip: 'Time',
                },
                {
                    sidenavConfig: {component: WorkspaceSettingsComponent},
                    icon: {
                        type: 'icon',
                        name: 'settings',
                    },
                    tooltip: 'Workspace',
                },
                // {
                //     sidenavConfig: {component: HelpComponent},
                //     icon: 'help',
                //     tooltip: 'Help',
                // },
            ]),
        );
    }

    private createAddDataConfigStream(): Observable<SidenavConfig | undefined> {
        return this.userService.getSessionStream().pipe(
            mergeMap(() =>
                concat(
                    of(undefined), // first emit undefined to show loading indicator
                    this.createAddDataListButtons(),
                ),
            ),
            map((buttons) => (buttons ? {component: AddDataComponent, config: {buttons}} : undefined)),
        );
    }

    private createAddDataListButtons(): Observable<Array<AddDataButton>> {
        return AddDataComponent.createLayerRootCollectionButtons(this.layerService).pipe(
            map((buttons) => [
                ...buttons,
                AddDataComponent.createUploadButton(),
                AddDataComponent.createDrawFeaturesButton(),
                AddDataComponent.createAddWorkflowByIdButton(),
            ]),
        );
    }

    private static createOperatorListButtons(): OperatorListButtonGroups {
        return [
            {name: 'Mixed', list: OperatorListComponent.DEFAULT_MIXED_OPERATOR_DIALOGS},
            {name: 'Plots', list: OperatorListComponent.DEFAULT_PLOT_OPERATOR_DIALOGS},
            {name: 'Raster', list: OperatorListComponent.DEFAULT_RASTER_OPERATOR_DIALOGS},
            {name: 'Vector', list: OperatorListComponent.DEFAULT_VECTOR_OPERATOR_DIALOGS},
        ];
    }

    onResize(): void {
        this.windowHeight$.set(window.innerHeight);
    }

    // private async debugCallDialog(): Promise<void> {
    //     const core = await import('@geoengine/core');

    //     this.layoutService.setSidenavContentComponent({
    //         component: core.ClassHistogramOperatorComponent,
    //     });
    // }
}
