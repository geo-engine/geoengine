import {BehaviorSubject, concat, Observable, of, ReplaySubject} from 'rxjs';
import {map, mergeMap, tap} from 'rxjs/operators';

import {
    AfterViewInit,
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    Component,
    ElementRef,
    HostListener,
    OnInit,
    ViewContainerRef,
    inject,
    viewChild,
} from '@angular/core';
import {MatDialog} from '@angular/material/dialog';
import {MatIconRegistry, MatIcon} from '@angular/material/icon';
import {MatDrawerToggleResult, MatSidenav, MatSidenavContainer} from '@angular/material/sidenav';
import {MatTabGroup} from '@angular/material/tabs';
import {
    AddDataButton,
    AddDataComponent,
    LayoutService,
    MapContainerComponent,
    MapService,
    NavigationButton,
    NavigationComponent,
    OidcComponent,
    OperatorListButtonGroups,
    OperatorListComponent,
    PlotListComponent,
    ProjectService,
    SidenavConfig,
    SidenavContainerComponent,
    SpatialReferenceService,
    TimeConfigComponent,
    WorkspaceSettingsComponent,
    LayerListMenuComponent,
    ZoomHandlesComponent,
    SmallTimeInteractionComponent,
    LayerListComponent,
    OlVectorLayerComponent,
    OlRasterLayerComponent,
    MapResolutionExtentOverlayComponent,
    TabsComponent,
} from '@geoengine/core';
import {DomSanitizer} from '@angular/platform-browser';
import {ActivatedRoute, ParamMap} from '@angular/router';
import {AppConfig} from './app-config.service';
import {HelpComponent} from './help/help.component';
import {SplashDialogComponent} from './splash-dialog/splash-dialog.component';
import {GfBioCollectionDialogComponent as GfBioCollectionDialogComponent} from './gfbio-collection/gfbio-collection-dialog.component';
import {
    Layer,
    LayersService,
    NotificationService,
    RandomColorService,
    UserService,
    AsyncNumberSanitizer,
    AsyncValueDefault,
} from '@geoengine/common';
import {MatToolbar} from '@angular/material/toolbar';
import {MatButton} from '@angular/material/button';
import {MatTooltip} from '@angular/material/tooltip';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatToolbar,
        MatIcon,
        LayerListMenuComponent,
        MatButton,
        MatTooltip,
        ZoomHandlesComponent,
        SmallTimeInteractionComponent,
        NavigationComponent,
        MatSidenavContainer,
        MatSidenav,
        SidenavContainerComponent,
        LayerListComponent,
        MapContainerComponent,
        OlVectorLayerComponent,
        OlRasterLayerComponent,
        MapResolutionExtentOverlayComponent,
        TabsComponent,
        AsyncPipe,
        AsyncNumberSanitizer,
        AsyncValueDefault,
    ],
})
export class AppComponent implements OnInit, AfterViewInit {
    readonly config = inject<AppConfig>(AppConfig);
    readonly layoutService = inject(LayoutService);
    readonly projectService = inject(ProjectService);
    readonly vcRef = inject(ViewContainerRef);
    readonly userService = inject(UserService);
    private readonly layersService = inject(LayersService);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);
    private readonly dialog = inject(MatDialog);
    private readonly iconRegistry = inject(MatIconRegistry);
    private readonly randomColorService = inject(RandomColorService);
    private readonly activatedRoute = inject(ActivatedRoute);
    private readonly notificationService = inject(NotificationService);
    private readonly mapService = inject(MapService);
    private readonly spatialReferenceService = inject(SpatialReferenceService);
    private readonly sanitizer = inject(DomSanitizer);

    readonly mapComponent = viewChild.required(MapContainerComponent);
    readonly bottomTabs = viewChild.required(MatTabGroup);

    readonly rightSidenav = viewChild.required(MatSidenav);
    readonly sidenavContainerElement = viewChild.required(MatSidenavContainer, {read: ElementRef});
    readonly rightSidenavContainer = viewChild.required(SidenavContainerComponent);

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

    private windowHeight$ = new BehaviorSubject<number>(window.innerHeight);

    private GFBIO_COLLECTIONS_DATA_PROVIDER_ID = 'f64e2d5b-3b80-476a-83f5-c330956b2909';

    constructor() {
        const config = this.config;
        const vcRef = this.vcRef;

        this.registerIcons();

        vcRef.length; // eslint-disable-line @typescript-eslint/no-unused-expressions

        this.layersReverse$ = this.projectService.getLayerStream().pipe(map((layers) => layers.slice(0).reverse()));

        this.layerListVisible$ = this.layoutService.getLayerListVisibilityStream();
        this.layerDetailViewVisible$ = this.layoutService.getLayerDetailViewVisibilityStream();

        this.mapIsGrid$ = this.mapService.isGrid$;

        const totalHeight$ = this.windowHeight$.pipe(map((_height) => this.sidenavContainerElement().nativeElement.offsetHeight));

        this.middleContainerHeight$ = this.layoutService.getMapHeightStream(totalHeight$).pipe(tap(() => this.mapComponent().resize()));
        this.layerListHeight$ = config.COMPONENTS.MAP_RESOLUTION_EXTENT_OVERLAY.AVAILABLE
            ? this.middleContainerHeight$.pipe(map((height) => height - 62))
            : this.middleContainerHeight$;
        this.bottomContainerHeight$ = this.layoutService.getLayerDetailViewStream(totalHeight$);

        this.createAddDataConfigStream().subscribe((addDataConfig) => this.addDataConfig.next(addDataConfig));
        this.createNavigationButtonStream().subscribe((navigationButtons) => {
            this.navigationButtons.next(navigationButtons);
            // loading spinners somewhat don't show up without this
            setTimeout(() => this.changeDetectorRef.detectChanges());
        });
    }

    ngOnInit(): void {
        this.mapService.registerMapComponent(this.mapComponent());

        // TODO: remove if table is back
        this.layoutService.setLayerDetailViewVisibility(false);
    }

    ngAfterViewInit(): void {
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
        this.projectService
            .getNewPlotStream()
            .subscribe(() => this.layoutService.setSidenavContentComponent({component: PlotListComponent}));

        setTimeout(() => {
            // emit window height once to resize components if necessary
            this.windowHeight();
        });
        void this.handleQueryParameters();
    }

    setTabIndex(index: number): void {
        this.layoutService.setLayerDetailViewTabIndex(index);
        this.layoutService.setLayerDetailViewVisibility(true);
    }

    idFromLayer(index: number, layer: Layer): number {
        return layer.id;
    }

    private registerIcons(): void {
        this.iconRegistry.addSvgIconInNamespace('vat', 'logo', this.sanitizer.bypassSecurityTrustResourceUrl('assets/vat_logo.svg'));

        // used for navigation
        this.iconRegistry.addSvgIcon('cogs', this.sanitizer.bypassSecurityTrustResourceUrl('assets/icons/cogs.svg'));
    }

    private createNavigationButtonStream(): Observable<Array<NavigationButton>> {
        return this.addDataConfig.pipe(
            map((addDataConfig) => [
                {
                    sidenavConfig: {component: OidcComponent},
                    icon: {
                        type: 'icon',
                        name: 'account_circle',
                    },
                    tooltip: 'Login',
                },
                addDataConfig
                    ? NavigationComponent.createAddDataButton(addDataConfig)
                    : NavigationComponent.createLoadingButton('add data'),
                {
                    sidenavConfig: {component: OperatorListComponent, config: {operators: AppComponent.createOperatorListButtons()}},
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
                {
                    sidenavConfig: {component: HelpComponent},
                    icon: {
                        type: 'icon',
                        name: 'help',
                    },
                    tooltip: 'Help',
                },
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
        return AddDataComponent.createLayerRootCollectionButtons(this.layersService).pipe(
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

    @HostListener('window:resize')
    windowHeight(): void {
        this.windowHeight$.next(window.innerHeight);
    }

    /**
     * @private
     */
    private async handleQueryParameters(): Promise<void> {
        const params = new URLSearchParams(window.location.search);
        const sessionState = params.get('session_state');
        const code = params.get('code');
        const state = params.get('state');

        // this tries to restore an OIDC sesson
        if (sessionState && code && state && sessionStorage.getItem(UserService.OIDC_RESTORE_ROUTE_KEY)) {
            const _login = await this.userService.oidcLogin({sessionState, code, state});
        }

        const handleParams = async (p: ParamMap): Promise<void> => {
            const collectionId = p.get('collectionId');
            if (collectionId) {
                const result = await this.layersService.getLayerCollectionItems(
                    this.GFBIO_COLLECTIONS_DATA_PROVIDER_ID,
                    `collections/${collectionId}`,
                );
                this.dialog.open(GfBioCollectionDialogComponent, {data: {result}});
            } else {
                const showSplash = this.userService.getSettingFromLocalStorage(SplashDialogComponent.SPLASH_DIALOG_NAME);
                if (showSplash === null || JSON.parse(showSplash)) {
                    this.dialog.open(SplashDialogComponent, {});
                }
            }
        };

        this.activatedRoute.queryParamMap.pipe(mergeMap((p) => handleParams(p))).subscribe();
    }
}
