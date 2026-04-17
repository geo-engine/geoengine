import {Observable, BehaviorSubject, first, filter, map, forkJoin} from 'rxjs';
import {AfterViewInit, ChangeDetectionStrategy, Component, HostListener, OnInit, ViewContainerRef, inject, viewChild} from '@angular/core';
import {MatIconRegistry, MatIcon} from '@angular/material/icon';
import {
    LayoutService,
    ProjectService,
    MapService,
    MapContainerComponent,
    DatasetService,
    SidenavContainerComponent,
    LayerCollectionListingDict,
    SymbologyEditorComponent,
    CoreModule,
} from '@geoengine/core';
import {DomSanitizer} from '@angular/platform-browser';
import {AppConfig} from '../app-config.service';
import moment from 'moment';
import {DataSelectionService} from '../data-selection.service';
import {AppDatasetService} from '../app-dataset.service';
import {MatDrawerToggleResult, MatSidenav, MatSidenavContainer} from '@angular/material/sidenav';
import {Layer, LayersService, Time, UserService, AsyncStringSanitizer, AsyncValueDefault} from '@geoengine/common';
import {MatToolbar} from '@angular/material/toolbar';
import {
    MatAccordion,
    MatExpansionPanel,
    MatExpansionPanelHeader,
    MatExpansionPanelTitle,
    MatExpansionPanelDescription,
    MatExpansionPanelContent,
} from '@angular/material/expansion';
import {MatTabGroup, MatTab} from '@angular/material/tabs';
import {AccordionEntryComponent} from '../accordion-entry/accordion-entry.component';
import {AccordionVectorEntryComponent} from '../accordion-vector-entry/accordion-vector-entry.component';
import {AboutComponent} from '../about/about.component';
import {MatButton} from '@angular/material/button';
import {MatCard} from '@angular/material/card';
import {AnalysisComponent} from '../analysis/analysis.component';
import {DataPointComponent} from '../data-point/data-point.component';
import {LegendComponent} from '../legend/legend.component';
import {MatProgressBar} from '@angular/material/progress-bar';
import {AsyncPipe} from '@angular/common';

interface LayerCollectionBiListing {
    name: string;
    raster?: LayerCollectionListingDict;
    otherRaster?: LayerCollectionListingDict;
    vector?: LayerCollectionListingDict;
}

@Component({
    selector: 'geoengine-main',
    templateUrl: './main.component.html',
    styleUrls: ['./main.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatSidenavContainer,
        MatToolbar,
        MatAccordion,
        MatExpansionPanel,
        MatExpansionPanelHeader,
        MatExpansionPanelTitle,
        MatExpansionPanelDescription,
        MatIcon,
        MatTabGroup,
        MatTab,
        AccordionEntryComponent,
        AccordionVectorEntryComponent,
        MatExpansionPanelContent,
        AboutComponent,
        MatButton,
        CoreModule,
        MatCard,
        AnalysisComponent,
        DataPointComponent,
        LegendComponent,
        MapContainerComponent,
        MatSidenav,
        MatProgressBar,
        AsyncPipe,
        AsyncStringSanitizer,
        AsyncValueDefault,
    ],
})
export class MainComponent implements OnInit, AfterViewInit {
    readonly config = inject<AppConfig>(AppConfig);
    readonly layoutService = inject(LayoutService);
    readonly projectService = inject(ProjectService);
    readonly userService = inject(UserService);
    readonly dataSelectionService = inject(DataSelectionService);
    readonly _vcRef = inject(ViewContainerRef);
    readonly datasetService = inject<DatasetService>(AppDatasetService);
    private iconRegistry = inject(MatIconRegistry);
    private mapService = inject(MapService);
    private sanitizer = inject(DomSanitizer);
    private readonly layersService = inject(LayersService);

    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly leftSidenav = viewChild.required(MatSidenav);
    readonly leftSidenavContainer = viewChild.required(SidenavContainerComponent);

    readonly topLevelCollections$ = new BehaviorSubject<Array<LayerCollectionBiListing>>([]);

    readonly layersReverse$: Observable<Array<Layer>>;
    readonly analysisVisible$ = new BehaviorSubject(false);
    readonly windowHeight$ = new BehaviorSubject<number>(window.innerHeight);

    readonly isSymbologyButtonVisible: Observable<boolean> = this.dataSelectionService.rasterLayer.pipe(
        map((rasterLayer) => !!rasterLayer),
    );

    constructor() {
        this.registerIcons();

        this.layersReverse$ = this.dataSelectionService.layers;

        forkJoin({
            raster4d: this.layersService.getLayerCollectionItems(this.config.DATA.RASTER4D.PROVIDER, this.config.DATA.RASTER4D.COLLECTION),
            rasterOther: this.layersService.getLayerCollectionItems(
                this.config.DATA.RASTER_OTHER.PROVIDER,
                this.config.DATA.RASTER_OTHER.COLLECTION,
            ),
            vector: this.layersService.getLayerCollectionItems(this.config.DATA.VECTOR.PROVIDER, this.config.DATA.VECTOR.COLLECTION),
        }).subscribe(({raster4d, rasterOther, vector}) => {
            const collections = new Map<string, LayerCollectionBiListing>();

            // create initial groups
            for (const item of raster4d.items) {
                if (item.type !== 'collection') {
                    continue;
                }

                collections.set(item.name, {
                    name: item.name,
                    raster: item as LayerCollectionListingDict,
                });
            }

            // add other raster layers to groups
            for (const item of rasterOther.items) {
                if (item.type !== 'collection') {
                    continue;
                }

                const collection = collections.get(item.name);

                if (collection?.raster) {
                    collection.otherRaster = item as LayerCollectionListingDict;
                } else {
                    collections.set(item.name, {
                        name: item.name,
                        otherRaster: item as LayerCollectionListingDict,
                    });
                }
            }

            // add vector layers to groups
            for (const item of vector.items) {
                if (item.type !== 'collection') {
                    continue;
                }

                const collection = collections.get(item.name);

                if (collection) {
                    collection.vector = item as LayerCollectionListingDict;
                } else {
                    collections.set(item.name, {
                        name: item.name,
                        vector: item as LayerCollectionListingDict,
                    });
                }
            }

            this.topLevelCollections$.next(Array.from(collections.values()).sort((a, b) => a.name.localeCompare(b.name)));
        });
    }

    ngOnInit(): void {
        this.mapService.registerMapComponent(this.mapComponent());

        this.layoutService.getSidenavContentComponentStream().subscribe((sidenavConfig) => {
            this.leftSidenavContainer().load(sidenavConfig);

            let openClosePromise: Promise<MatDrawerToggleResult>;
            if (sidenavConfig) {
                openClosePromise = this.leftSidenav().open();
            } else {
                openClosePromise = this.leftSidenav().close();
            }

            openClosePromise.then(() => this.mapComponent().resize());
        });
    }

    ngAfterViewInit(): void {
        this.reset();
        this.mapComponent().resize();
    }

    icon(name: string): string {
        if (name === 'Anthropogenic activity') {
            return 'terrain';
        } else if (name === 'Biodiversity') {
            return 'pets';
        } else if (name === 'Climate') {
            return 'public';
        } else {
            return 'class';
        }
    }

    idFromLayer(index: number, layer: Layer): number {
        return layer.id;
    }

    showAnalysis(): void {
        this.analysisVisible$.next(true);
    }

    editSymbology(): void {
        this.dataSelectionService.rasterLayer
            .pipe(
                first(),
                filter((rasterLayer) => !!rasterLayer),
            )
            .subscribe((rasterLayer) => {
                this.layoutService.setSidenavContentComponent({
                    component: SymbologyEditorComponent,
                    keepParent: false,
                    config: {
                        layer: rasterLayer,
                    },
                });
            });
    }

    private reset(): void {
        this.projectService.clearLayers();
        this.projectService.clearPlots();
        this.projectService.setTime(new Time(moment.utc()));
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
