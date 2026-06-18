import {ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit, inject} from '@angular/core';
import {
    BackendService,
    DatasetService,
    MapService,
    ProjectService,
    VectorResultDescriptorDict,
    WGS_84,
    SpatialReferenceService,
    NamedDataDict,
    CoreModule,
} from '@geoengine/core';
import {BehaviorSubject, combineLatest, combineLatestWith, first, mergeMap, Observable, of, Subscription, tap} from 'rxjs';
import {DataSelectionService} from '../data-selection.service';
import moment from 'moment';
import {
    ClusteredPointSymbology,
    Dataset,
    ExpressionDict,
    Layer,
    PointSymbology,
    RasterLayer,
    RasterSymbology,
    RasterVectorJoinDict,
    Time,
    TimeProjectionDict,
    UserService,
    VectorLayer,
    extentToBboxDict,
    CommonModule,
    FxLayoutDirective,
    FxFlexDirective,
    FxLayoutGapDirective,
    FxLayoutAlignDirective,
    AsyncValueDefault,
} from '@geoengine/common';
import {LegacyTypedOperatorOperator, Workflow as WorkflowDict} from '@geoengine/api-client';
import {MatFormField, MatLabel} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatSelectSearchComponent} from 'ngx-mat-select-search';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatProgressBar} from '@angular/material/progress-bar';
import {MatExpansionPanel, MatExpansionPanelHeader} from '@angular/material/expansion';
import {MatDivider} from '@angular/material/list';
import {MatSlideToggle} from '@angular/material/slide-toggle';
import {MatSlider, MatSliderThumb} from '@angular/material/slider';
import {MatButton} from '@angular/material/button';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {AttributionsComponent} from '../attributions/attributions.component';
import {AsyncPipe} from '@angular/common';

interface EnvironmentLayer {
    name: string;
    displayName: string;
    dataRange: [number, number];
    plotType?: 'pieChart';
}

const START_YEAR = 1991;
const END_YEAR = 2020;
const DRAGONFLY_SPECIES = [
    'Aeshna affinis',
    'Aeshna caerulea',
    'Aeshna cyanea',
    'Aeshna grandis',
    'Aeshna isoceles',
    'Aeshna juncea',
    'Aeshna mixta',
    'Aeshna subarctica',
    'Aeshna viridis',
    'Anax ephippiger',
    'Anax imperator',
    'Anax parthenope',
    'Boyeria irene',
    'Brachytron pratense',
    'Calopteryx splendens',
    'Calopteryx virgo',
    'Ceriagrion tenellum',
    'Chalcolestes viridis',
    'Coenagrion armatum',
    'Coenagrion hastulatum',
    'Coenagrion lunulatum',
    'Coenagrion mercuriale',
    'Coenagrion ornatum',
    'Coenagrion puella',
    'Coenagrion pulchellum',
    'Coenagrion scitulum',
    'Cordulegaster bidentata',
    'Cordulegaster boltonii',
    'Cordulia aenea',
    'Crocothemis erythraea',
    'Enallagma cyathigerum',
    'Epitheca bimaculata',
    'Erythromma lindenii',
    'Erythromma najas',
    'Erythromma viridulum',
    'Gomphus flavipes',
    'Gomphus pulchellus',
    'Gomphus simillimus',
    'Gomphus vulgatissimus',
    'Ischnura elegans',
    'Ischnura pumilio',
    'Lestes barbarus',
    'Lestes dryas',
    'Lestes sponsa',
    'Lestes virens',
    'Leucorrhinia albifrons',
    'Leucorrhinia caudalis',
    'Leucorrhinia dubia',
    'Leucorrhinia pectoralis',
    'Leucorrhinia rubicunda',
    'Libellula depressa',
    'Libellula fulva',
    'Libellula quadrimaculata',
    'Nehalennia speciosa',
    'Onychogomphus forcipatus',
    'Onychogomphus uncatus',
    'Ophiogomphus cecilia',
    'Orthetrum albistylum',
    'Orthetrum brunneum',
    'Orthetrum cancellatum',
    'Orthetrum coerulescens',
    'Oxygastra curtisii',
    'Platycnemis pennipes',
    'Pyrrhosoma nymphula',
    'Somatochlora alpestris',
    'Somatochlora arctica',
    'Somatochlora flavomaculata',
    'Somatochlora metallica',
    'Sympecma fusca',
    'Sympecma paedisca',
    'Sympetrum danae',
    'Sympetrum depressiusculum',
    'Sympetrum flaveolum',
    'Sympetrum fonscolombii',
    'Sympetrum meridionale',
    'Sympetrum pedemontanum',
    'Sympetrum sanguineum',
    'Sympetrum striolatum',
    'Sympetrum vulgatum',
];
const FISH_SPECIES = [
    'Abramis brama',
    'Acipenser baerii',
    'Acipenser gueldenstaedtii',
    'Acipenser nudiventris',
    'Acipenser ruthenus',
    'Acipenser spec.',
    'Acipenser stellatus',
    'Acipenser sturio',
    'Agonus cataphractus',
    'Alburnoides bipunctatus',
    'Alburnus alburnus',
    'Alburnus mento',
    'Alopias vulpinus',
    'Alosa fallax',
    'Amatitlania nigrofasciata',
    'Amblyraja radiata',
    'Ameiurus melas',
    'Ameiurus nebulosus',
    'Ammodytes marinus',
    'Ammodytes tobianus',
    'Anguilla anguilla',
    'Aphia minuta',
    'Babka gymnotrachelus',
    'Ballerus ballerus',
    'Ballerus sapa',
    'Barbatula barbatula',
    'Barbus barbus',
    'Belone belone',
    'Blicca bjoerkna',
    'Carassius auratus',
    'Carassius carassius',
    'Carassius gibelio',
    'Carassius langsdorfii',
    'Cetorhinus maximus',
    'Chelon labrosus',
    'Chondrostoma nasus',
    'Ciliata mustela',
    'Clupea harengus',
    'Cobitis elongatoides',
    'Cobitis taenia',
    'Coregonus albula',
    'Coregonus arenicolus',
    'Coregonus bavaricus',
    'Coregonus fontanae',
    'Coregonus hoferi',
    'Coregonus lavaretus',
    'Coregonus lucinensis',
    'Coregonus macrophthalmus',
    'Coregonus maraena',
    'Coregonus maraenoides',
    'Coregonus oxyrinchus',
    'Coregonus sp. "Kärnten"',
    'Coregonus wartmanni',
    'Coregonus widegreni',
    'Cottus gobio',
    'Cottus microstomus',
    'Cottus perifretum',
    'Cottus poecilopus',
    'Ctenolabrus rupestris',
    'Ctenopharyngodon idella',
    'Cyclopterus lumpus',
    'Cyprinus carpio',
    'Dicentrarchus labrax',
    'Enchelyopus cimbrius',
    'Esox lucius',
    'Eudontomyzon mariae',
    'Gadus morhua',
    'Gasterosteus aculeatus',
    'Gasterosteus gymnurus',
    'Gobio albipinnatus',
    'Gobio gobio',
    'Gobio obtusirostris',
    'Gobiosoma bosc',
    'Gobius niger',
    'Gobiusculus flavescens',
    'Gymnocephalus baloni',
    'Gymnocephalus cernua',
    'Gymnocephalus schraetser',
    'Hippocampus hippocampus',
    'Hucho hucho',
    'Huso huso',
    'Hyperoplus lanceolatus',
    'Hypophthalmichthys molitrix',
    'Lampetra fluviatilis',
    'Lampetra planeri',
    'Lepomis gibbosus',
    'Leucaspius delineatus',
    'Leuciscus aspius',
    'Leuciscus idus',
    'Leuciscus leuciscus',
    'Limanda limanda',
    'Liparis liparis',
    'Lipophrys pholis',
    'Lota lota',
    'Merlangius merlangus',
    'Misgurnus anguillicaudatus',
    'Misgurnus bipartitus',
    'Misgurnus fossilis',
    'Mola mola',
    'Molva molva',
    'Morone saxatilis',
    'Myoxocephalus scorpius',
    'Neogobius fluviatilis',
    'Neogobius melanostomus',
    'Nerophis ophidion',
    'Oncorhynchus mykiss',
    'Osmerus eperlanus',
    'Paramisgurnus dabryanus',
    'Pelecus cultratus',
    'Perca fluviatilis',
    'Perccottus glenii',
    'Petromyzon marinus',
    'Pholis gunnellus',
    'Phoxinus phoxinus',
    'Pimephales promelas',
    'Platichthys flesus',
    'Pleuronectes platessa',
    'Poecilia reticulata',
    'Pomatoschistus microps',
    'Pomatoschistus minutus',
    'Pomatoschistus pictus',
    'Ponticola kessleri',
    'Proterorhinus semilunaris',
    'Psetta maxima',
    'Pseudorasbora parva',
    'Pungitius pungitius',
    'Raja clavata',
    'Raniceps raninus',
    'Rhodeus amarus',
    'Romanogobio belingi',
    'Romanogobio kesslerii',
    'Romanogobio uranoscopus',
    'Romanogobio vladykovi',
    'Rutilus meidingeri',
    'Rutilus rutilus',
    'Rutilus virgo',
    'Sabanejewia balcanica',
    'Sabanejewia baltica',
    'Salmo salar',
    'Salmo trutta',
    'Salvelinus evasus',
    'Salvelinus fontinalis',
    'Salvelinus namaycush',
    'Salvelinus umbla',
    'Sander lucioperca',
    'Sander volgensis',
    'Scardinius erythrophthalmus',
    'Scyliorhinus canicula',
    'Silurus glanis',
    'Solea solea',
    'Spinachia spinachia',
    'Sprattus sprattus',
    'Squalius cephalus',
    'Syngnathus rostellatus',
    'Syngnathus typhle',
    'Tachysurus fulvidraco',
    'Taurulus bubalis',
    'Thymallus thymallus',
    'Tinca tinca',
    'Trachurus trachurus',
    'Umbra krameri',
    'Vimba vimba',
    'Xiphias gladius',
    'Zingel streber',
    'Zingel zingel',
    'Zoarces viviparus',
];
/* eslint-disable @typescript-eslint/naming-convention */
const SPECIES_INFO: Record<string, SpeciesInfo> = {
    'Anax imperator': {
        text: `Die Große Königslibelle erreicht Flügelspannweiten von 9,5 bis 11 Zentimetern. Der Brustabschnitt (Thorax) der Tiere ist grün gefärbt,
        der Hinterleib (Abdomen) der Männchen ist hellblau mit einem durchgehenden schwarzen Längsband am Rücken, das an jedem Segment eine zahnartige
        Ausbuchtung besitzt. Der Hinterleib der Weibchen ist blaugrün, das Längsband am Rücken ist braun und breit. Im Gegensatz dazu hat die etwas
        kleinere Kleine Königslibelle (Anax parthenope) eine braune Brust und der Hinterleib ist nur im vorderen Bereich blau. `,
        imageSrc: 'https://upload.wikimedia.org/wikipedia/commons/0/06/Anax_imperator_qtl2.jpg',
        imageRef: 'Quartl',
    },
    'Coenagrion puella': {
        text: `Die Hufeisen-Azurjungfer (Coenagrion puella) erreicht Körperlängen von 35 bis 40 Millimetern und ist in der Regel sehr schlank, fast nadelförmig
        gebaut. Den Namen hat die Hufeisen-Azurjungfer dem hufeisenförmigen schwarzen Mal, das auf dem zweiten Hinterleibssegment des
        Männchens zu finden ist (Bild 7), zu verdanken. Dies existiert jedoch auch bei ähnlichen Arten wie etwa der
        Fledermaus-Azurjungfer (C. pulchellum) in ähnlicher Ausprägung, bei der die schwarze Zeichnung der folgenden Hinterleibssegmente
        jedoch umfassender ist. Außerdem wird zur einwandfreien Identifizierung der Männchen die Form der Zange am letzten
        Hinterleibssegment (Bild 10) herangezogen. Die Männchen sind blau mit schwarzer Zeichnung. Bei den ausgefärbten Weibchen
        überwiegt die schwarze Zeichnung, in der Grundfarbe sind sie meist grün (heterochrome Weibchen, Bild 3), manchmal hellblau
        (homoeochrome Weibchen, Bild 4). Junge Exemplare sind bei Männchen und Weibchen milchig blass (Bild 5, 6). Die Weibchen tragen
        auf dem ersten Abdominalsegment kein „Hufeisen“, sondern eine Zeichnung, die an einen Pokal erinnert (Bild 8). Die Zeichnung
        variiert jedoch ebenfalls beträchtlich und es gibt Überschneidungen zu mehreren anderen Arten. Deswegen zieht man zur Bestimmung
        der Art die Linie heran, in der das Pronotum nach hinten abschließt. Bei der Hufeisen-Azurjungfer ist diese doppelt geschwungen
        und meist blau (Bild 9). Die Zangenform des Männchens und die Form des Halsschildes beim Weibchen sind konstante Merkmale, da
        sie für das Paarungsrad (Bild 14) genau „passen“ müssen.`,
        imageSrc: 'https://upload.wikimedia.org/wikipedia/commons/4/4e/Coenagrion_puella_3%28loz%29.jpg',
        imageRef: 'L. B. Tettenborn',
    },
    'Abramis brama': {
        text: `Im Alter von acht Jahren erreichen Brachsen eine Länge von ca. 30 bis 50 cm. Bei einer Länge von 60 cm wiegen sie im Durchschnitt 3 bis 3,5 kg.
        Unter idealen Bedingungen können Brachsen maximal 85 cm und über 8 kg schwer werden, so auch der deutsche Rekordfisch, gefangen im Jahr 2000.[1] Brachsen
        können ein Alter von etwa 16 Jahren erreichen. Brachsen sind seitlich sehr stark abgeflacht und hochrückig. Bei schlechter Ernährung kommt es bei 
        den Brachsen zum sogenannten Messerrücken, der Bildung einer sehr scharfen Rückenkante. Das stumpfe Maul ist leicht unterständig, die Augen verhältnismäßig 
        klein. Auffällig ist die grünlich glänzende, schwarze bis bleigraue oder bleiblaue Färbung auf dem Rücken, der die Fische den Namen Blei verdanken.
        Die Seiten glänzen metallisch, der Bauch ist weißlich mit Perlmuttglanz. Bei älteren Brachsen kommt ein lichter Bronze- oder goldgrüner Ton durch.
        Die Schuppen sind stark mit Schleim bedeckt. Die Rückenflosse ist 12-, die Afterflosse 26- bis 31-strahlig. Bis auf die Brustflossen sind die Flossen
        dunkelgrau, die mittelgrauen Brustflossen der Brachsen sind lang und reichen angelegt bis an den Ansatz der Bauchflossen heran. Dadurch unterscheiden sie
        sich vom Güster, mit dem sie manchmal verwechselt werden. Durch gleichzeitige Laichzeiten vermischen sich Eier und Samen von Blei und Güster
        (und anderen Weißfischen), dadurch entstehen sogenannte Bastardfische, die sich aber anhand der Anzahl und Verteilung der Schlundzähne unterscheiden lassen.`,
        imageSrc: 'https://upload.wikimedia.org/wikipedia/commons/6/69/Carp_bream.jpg',
        imageRef: 'Микова Наталия',
    },
};

interface SpeciesInfo {
    text: string;
    imageSrc: string;
    imageRef?: string;
}

@Component({
    selector: 'geoengine-species-selector',
    templateUrl: './species-selector.component.html',
    styleUrls: ['./species-selector.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatFormField,
        MatLabel,
        MatSelect,
        MatOption,
        MatSelectSearchComponent,
        CommonModule,
        FormsModule,
        ReactiveFormsModule,
        MatProgressBar,
        CoreModule,
        MatExpansionPanel,
        MatExpansionPanelHeader,
        MatDivider,
        FxLayoutDirective,
        FxFlexDirective,
        FxLayoutGapDirective,
        FxLayoutAlignDirective,
        MatSlideToggle,
        MatSlider,
        MatSliderThumb,
        MatButton,
        MatProgressSpinner,
        AttributionsComponent,
        AsyncPipe,
        AsyncValueDefault,
    ],
})
export class SpeciesSelectorComponent implements OnInit, OnDestroy {
    readonly dataSelectionService = inject(DataSelectionService);
    private readonly projectService = inject(ProjectService);
    private readonly datasetService = inject(DatasetService);
    private readonly userService = inject(UserService);
    private readonly backend = inject(BackendService);
    private readonly mapService = inject(MapService);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);
    private readonly spatialReferenceService = inject(SpatialReferenceService);

    readonly dragonflySpecies: string[] = DRAGONFLY_SPECIES;
    readonly fishSpecies: string[] = FISH_SPECIES;

    readonly environmentLayers: EnvironmentLayer[] = [
        // {
        //     id: '36574dc3-560a-4b09-9d22-d5945f2b8111',
        //     name: 'NDVI',
        //     dataRange: [-2000, 10000],
        // },
        // {
        //     id: '36574dc3-560a-4b09-9d22-d5945f2b8666',
        //     name: 'Water Bodies 333m',
        //     dataRange: [70, 71],
        // },
        {
            name: 'ECMWF_ERA_5_land_2m_temperature_celsius',
            displayName: 'Mittlere monatliche Temperatur in C° (2000 - 2020)',
            dataRange: [-5, 30],
        },
        {
            name: 'ECMWF_ERA_5_land_total_precipitation_mm',
            displayName: 'Mittlerer monatlicher Niederschlag in mm (2000 - 2020)',
            dataRange: [0, 20],
        },
        {
            name: 'landcover_classification',
            displayName: 'Landnutzungstypen (2019 & 2020)',
            dataRange: [0, 60],
        },
        {
            name: 'IOER-Monitor',
            displayName: 'Anteil Gebiete „Natur- und Artenschutz“ an Gebietsfläche',
            dataRange: [0, 100],
            plotType: 'pieChart',
        },
    ];

    plotSpecies = '';
    plotEnvironmentLayer = '';
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    readonly plotData = new BehaviorSubject<any>(undefined);
    readonly plotLoading = new BehaviorSubject(false);

    currentMonth = 1;

    selectedDragonflySpecies?: string = undefined;
    selectedFishSpecies?: string = undefined;
    selectedEnvironmentLayer?: EnvironmentLayer = undefined;
    selectedEnvironmentCitation = new BehaviorSubject<string>('');

    dragonflySpeciesLayer?: Layer = undefined;
    fishSpeciesLayer?: Layer = undefined;
    intensityLayer?: RasterLayer = undefined;
    environmentLayer?: Layer = undefined;

    plotLayerSelection: 'dragonfly' | 'fish' = 'dragonfly';

    readonly startYear = START_YEAR;
    readonly endYear = END_YEAR;

    private readonly dragonflyDataset: NamedDataDict = 'gdo_libellen';
    private readonly fishDataset: NamedDataDict = 'gfi_fischartenatlas';
    private readonly intensityDataset: NamedDataDict = 'beprobungsintensitaet_nrw';

    private selectedEnvironmentDataset?: Dataset = undefined;

    private readonly subscriptions: Array<Subscription> = [];

    ngOnInit(): void {
        const species1LayerSubscription = this.dataSelectionService.speciesLayer1.subscribe((speciesLayer) => {
            this.dragonflySpeciesLayer = speciesLayer;
            this.changeDetectorRef.markForCheck();
        });
        this.subscriptions.push(species1LayerSubscription);

        const species2LayerSubscription = this.dataSelectionService.speciesLayer2.subscribe((speciesLayer) => {
            this.fishSpeciesLayer = speciesLayer;
            this.changeDetectorRef.markForCheck();
        });
        this.subscriptions.push(species2LayerSubscription);

        const environmentLayerSubscription = this.dataSelectionService.rasterLayer.subscribe((environmentLayer) => {
            this.environmentLayer = environmentLayer;
            this.changeDetectorRef.markForCheck();
        });
        this.subscriptions.push(environmentLayerSubscription);

        // initialize start values
        // must ensure that project is loaded before
        this.projectService.clearLayers().subscribe(() => {
            this.dataSelectionService.setTimeSteps(
                [...generateYearlyTimeSteps(START_YEAR, END_YEAR, this.currentMonth)],
                undefined,
                new Time('2010-01-01T00:00:00.000Z', '2010-02-01T00:00:00.000Z'),
            );

            this.selectDragonflySpecies('Anax imperator');
            this.selectFishSpecies('Abramis brama');
        });
    }

    ngOnDestroy(): void {
        for (const sub of this.subscriptions) {
            sub.unsubscribe();
        }
    }

    speciesPredicate(filter: string, element: string): boolean {
        return element.toLowerCase().includes(filter);
    }

    selectDragonflySpecies(species: string): void {
        this.selectedDragonflySpecies = species;

        if (!this.selectedDragonflySpecies) {
            this.dataSelectionService.resetSpecies1Layer().subscribe();

            return;
        }

        const workflow: WorkflowDict = {
            type: 'Vector',
            operator: {
                type: 'TimeProjection',
                params: {
                    step: {
                        granularity: 'years',
                        step: 1,
                    },
                },
                sources: {
                    vector: {
                        type: 'OgrSource',
                        params: {
                            data: this.dragonflyDataset,
                            attributeProjection: [],
                            attributeFilters: [
                                {
                                    attribute: 'Species',
                                    ranges: [[species, species]],
                                    keepNulls: false,
                                },
                            ],
                        },
                    },
                },
            } as TimeProjectionDict,
        };

        this.projectService
            .registerWorkflow(workflow)
            .pipe(
                mergeMap((workflowId) =>
                    this.dataSelectionService.setSpecies1Layer(
                        new VectorLayer({
                            workflowId,
                            name: 'Beobachtungen',
                            symbology: ClusteredPointSymbology.fromPointSymbologyDict({
                                type: 'point',
                                radius: {
                                    type: 'static',
                                    value: PointSymbology.DEFAULT_POINT_RADIUS,
                                },
                                stroke: {
                                    width: {
                                        type: 'static',
                                        value: 1,
                                    },
                                    color: {
                                        type: 'static',
                                        color: [0, 0, 0, 255],
                                    },
                                },
                                fillColor: {
                                    type: 'static',
                                    color: [189, 42, 11, 255],
                                },
                            }),
                            isLegendVisible: false,
                            isVisible: true,
                        }),
                    ),
                ),
            )
            .subscribe();
    }

    selectFishSpecies(species: string): void {
        this.selectedFishSpecies = species;

        if (!this.selectedFishSpecies) {
            this.dataSelectionService.resetSpecies2Layer().subscribe();

            return;
        }

        const workflow: WorkflowDict = {
            type: 'Vector',
            operator: {
                type: 'TimeProjection',
                params: {
                    step: {
                        granularity: 'years',
                        step: 1,
                    },
                },
                sources: {
                    vector: {
                        type: 'OgrSource',
                        params: {
                            data: this.fishDataset,
                            attributeProjection: [],
                            attributeFilters: [
                                {
                                    attribute: 'Species',
                                    ranges: [[species, species]],
                                    keepNulls: false,
                                },
                            ],
                        },
                    },
                },
            } as TimeProjectionDict,
        };

        this.projectService
            .registerWorkflow(workflow)
            .pipe(
                mergeMap((workflowId) =>
                    this.dataSelectionService.setSpecies2Layer(
                        new VectorLayer({
                            workflowId,
                            name: 'Beobachtungen',
                            symbology: ClusteredPointSymbology.fromPointSymbologyDict({
                                type: 'point',
                                radius: {
                                    type: 'static',
                                    value: PointSymbology.DEFAULT_POINT_RADIUS,
                                },
                                stroke: {
                                    width: {
                                        type: 'static',
                                        value: 1,
                                    },
                                    color: {
                                        type: 'static',
                                        color: [0, 0, 0, 255],
                                    },
                                },
                                fillColor: {
                                    type: 'static',
                                    color: [16, 83, 120, 255],
                                },
                            }),
                            isLegendVisible: false,
                            isVisible: true,
                        }),
                    ),
                ),
            )
            .subscribe();
    }

    selectEnvironmentLayer(layer: EnvironmentLayer | undefined): void {
        if (!layer) {
            this.selectedEnvironmentLayer = undefined;
            this.selectedEnvironmentDataset = undefined;
            this.selectedEnvironmentCitation.next('');

            this.dataSelectionService.resetRasterLayer().subscribe();

            return;
        }

        this.selectedEnvironmentLayer = layer;

        const workflow: WorkflowDict = {
            type: 'Raster',
            operator: {
                type: 'GdalSource',
                params: {
                    data: layer.name,
                },
            },
        };

        this.selectedEnvironmentCitation.next('');

        this.projectService
            .registerWorkflow(workflow)
            .pipe(
                combineLatestWith(this.datasetService.getDataset(layer.name)),
                tap(([workflowId, _dataset]) => {
                    this.userService
                        .getSessionTokenForRequest()
                        .pipe(mergeMap((token) => this.backend.getWorkflowProvenance(workflowId, token)))
                        .subscribe((provenance) => {
                            this.selectedEnvironmentCitation.next(provenance.map((p) => p.provenance.citation).join(','));
                        });
                }),
                mergeMap(([workflowId, dataset]) => {
                    this.selectedEnvironmentDataset = dataset;
                    if (!!dataset.symbology && dataset.symbology instanceof RasterSymbology) {
                        return this.dataSelectionService.setRasterLayer(
                            new RasterLayer({
                                workflowId,
                                name: layer.displayName,
                                symbology: dataset.symbology,
                                isLegendVisible: false,
                                isVisible: true,
                            }),
                            {
                                min: layer.dataRange[0],
                                max: layer.dataRange[1],
                            },
                        );
                    }

                    return of(undefined);
                }),
            )
            .subscribe();
    }

    getSpeciesInfo(species?: string): SpeciesInfo | undefined {
        if (!species) {
            return undefined;
        }
        return SPECIES_INFO[species];
    }

    showIntensities(show: boolean): void {
        this.intensityLayer = undefined;

        if (!show) {
            this.dataSelectionService.setIntensityLayer(undefined).subscribe();
            return;
        }

        const workflow: WorkflowDict = {
            type: 'Raster',
            operator: {
                type: 'GdalSource',
                params: {
                    data: this.intensityDataset,
                },
            },
        };

        this.projectService
            .registerWorkflow(workflow)
            .pipe(
                combineLatestWith(this.datasetService.getDataset(this.intensityDataset)),
                tap(([workflowId, _dataset]) => {
                    this.userService
                        .getSessionTokenForRequest()
                        .pipe(mergeMap((token) => this.backend.getWorkflowProvenance(workflowId, token)))
                        .subscribe((_provenance) => {
                            // TODO: citation
                            // this.selectedEnvironmentCitation.next(provenance.map((p) => p.provenance.citation).join(','));
                        });
                }),
                mergeMap(([workflowId, dataset]) => {
                    this.selectedEnvironmentDataset = dataset;
                    if (!!dataset.symbology && dataset.symbology instanceof RasterSymbology) {
                        this.intensityLayer = new RasterLayer({
                            workflowId,
                            name: 'Beprobungshäufigkeit',
                            symbology: dataset.symbology,
                            isLegendVisible: false,
                            isVisible: true,
                        });
                        this.changeDetectorRef.markForCheck();
                        return this.dataSelectionService.setIntensityLayer(this.intensityLayer);
                    }

                    return of(undefined);
                }),
            )
            .subscribe();
    }

    computePieChart(): void {
        if (
            (!this.selectedFishSpecies && !this.selectDragonflySpecies) ||
            !this.selectedEnvironmentLayer ||
            !this.selectedEnvironmentDataset
        ) {
            return;
        }

        let speciesLayer$: Observable<VectorLayer | undefined>;
        let selectedSpecies: string | undefined;

        if (!this.selectedDragonflySpecies) {
            speciesLayer$ = this.dataSelectionService.speciesLayer2;
            selectedSpecies = this.selectedFishSpecies;
        } else if (!this.selectFishSpecies) {
            speciesLayer$ = this.dataSelectionService.speciesLayer1;
            selectedSpecies = this.selectedDragonflySpecies;
        } else if (this.plotLayerSelection === 'dragonfly') {
            speciesLayer$ = this.dataSelectionService.speciesLayer1;
            selectedSpecies = this.selectedDragonflySpecies;
        } /* if (this.plotLayerSelection === 'fish') */ else {
            speciesLayer$ = this.dataSelectionService.speciesLayer2;
            selectedSpecies = this.selectedFishSpecies;
        }

        const environmentColumnName = 'environment';

        const computationCrs = WGS_84;

        combineLatest([
            this.dataSelectionService.rasterLayer.pipe(
                mergeMap<RasterLayer | undefined, Observable<RasterLayer>>((layer) => (layer ? of(layer) : of())),
            ),
            speciesLayer$.pipe(mergeMap<VectorLayer | undefined, Observable<VectorLayer>>((layer) => (layer ? of(layer) : of()))),
        ])
            .pipe(
                first(),
                tap(() => {
                    this.plotLoading.next(true);
                    this.plotData.next(undefined);

                    this.plotSpecies = selectedSpecies ?? '';
                    this.plotEnvironmentLayer = this.selectedEnvironmentLayer ? this.selectedEnvironmentLayer.displayName : '';
                }),
                mergeMap(([rasterLayer, speciesLayer]) =>
                    combineLatest([
                        this.projectService.getWorkflow(rasterLayer.workflowId),
                        this.projectService.getWorkflow(speciesLayer.workflowId),
                        this.projectService.getLayerMetadata(rasterLayer),
                        this.projectService.getLayerMetadata(speciesLayer),
                    ]),
                ),
                mergeMap(([rasterWorkflow, speciesWorkflow, rasterMetadata, vectorMetadata]) => {
                    let rasterOperator = rasterWorkflow.operator;
                    if (!rasterMetadata.spatialReference.equals(computationCrs.spatialReference)) {
                        rasterOperator = {
                            type: 'Reprojection',
                            params: {
                                targetSpatialReference: computationCrs.spatialReference.srsString,
                            },
                            sources: {
                                source: rasterOperator,
                            },
                        };
                    }

                    let vectorOperator = speciesWorkflow.operator;
                    if (!vectorMetadata.spatialReference.equals(computationCrs.spatialReference)) {
                        vectorOperator = {
                            type: 'Reprojection',
                            params: {
                                targetSpatialReference: computationCrs.spatialReference.srsString,
                            },
                            sources: {
                                source: vectorOperator,
                            },
                        };
                    }

                    return this.projectService.registerWorkflow({
                        type: 'Vector',
                        operator: {
                            type: 'RasterVectorJoin',
                            params: {
                                names: {
                                    type: 'names',
                                    values: [environmentColumnName],
                                },
                                temporalAggregation: 'none',
                                featureAggregation: 'first',
                            },
                            sources: {
                                rasters: [
                                    {
                                        type: 'Expression',
                                        params: {
                                            expression: 'if A > 50 { 1 } else { 0 }',
                                            outputType: 'U8',
                                            outputBand: {
                                                name: 'expression',
                                                measurement: {
                                                    type: 'classification',
                                                    measurement: 'Naturschutzgebiete',
                                                    classes: {
                                                        '0': 'Nicht-Naturschutzgebiet',
                                                        '1': 'Naturschutzgebiet',
                                                    },
                                                },
                                            },
                                            mapNoData: false,
                                        },
                                        sources: {
                                            raster: rasterOperator,
                                        },
                                    } as ExpressionDict,
                                ],
                                vector: vectorOperator,
                            },
                        } as RasterVectorJoinDict,
                    });
                }),
                mergeMap((workflowId) =>
                    combineLatest([
                        this.projectService.getWorkflow(workflowId),
                        this.projectService.getWorkflowMetaData(workflowId) as Observable<VectorResultDescriptorDict>,
                        this.dataSelectionService.dataRange,
                    ]),
                ),
                mergeMap(([workflow, metadata, dataRange]) => {
                    let plotWorkflow: LegacyTypedOperatorOperator;

                    if (
                        environmentColumnName in metadata.columns &&
                        metadata.columns[environmentColumnName].measurement.type === 'classification'
                    ) {
                        plotWorkflow = {
                            type: 'PieChart',
                            params: {
                                type: 'count',
                                columnName: environmentColumnName,
                                donut: false,
                            },
                            sources: {
                                vector: workflow.operator,
                            },
                        };
                    } else {
                        plotWorkflow = {
                            type: 'Histogram',
                            params: {
                                // TODO: get params from selected data
                                attributeName: 'band',
                                buckets: {
                                    type: 'number',
                                    value: 20,
                                },
                                bounds: dataRange,
                                columnName: environmentColumnName,
                            },
                            sources: {
                                source: workflow.operator,
                            },
                        };
                    }

                    return this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: plotWorkflow,
                    });
                }),
                mergeMap((workflowId) =>
                    combineLatest([
                        of(workflowId),
                        this.userService.getSessionTokenForRequest(),
                        this.projectService.getTimeOnce(),
                        this.mapService.getViewportSizeStream(),
                        this.projectService.getSpatialReferenceStream(),
                    ]),
                ),
                mergeMap(([workflowId, sessionToken, time, viewport, crs]) =>
                    this.backend.getPlot(
                        workflowId,
                        {
                            time: time.toDict(),
                            bbox: extentToBboxDict(
                                this.spatialReferenceService.reprojectExtent(viewport.extent, crs, computationCrs.spatialReference),
                            ),
                            crs: computationCrs.spatialReference.srsString,
                            // TODO: set reasonable size
                            spatialResolution: [0.1, 0.1],
                        },
                        sessionToken,
                    ),
                ),
                first(),
            )
            .subscribe({
                next: (plotData) => {
                    this.plotData.next(plotData.data);
                    this.plotLoading.next(false);
                },
                error: () => {
                    // TODO: react on error?
                    this.plotLoading.next(false);
                },
            });
    }

    computeHistogram(): void {
        if (
            (!this.selectedFishSpecies && !this.selectDragonflySpecies) ||
            !this.selectedEnvironmentLayer ||
            !this.selectedEnvironmentDataset
        ) {
            return;
        }

        let speciesLayer$: Observable<VectorLayer | undefined>;
        let selectedSpecies: string | undefined;

        if (!this.selectedDragonflySpecies) {
            speciesLayer$ = this.dataSelectionService.speciesLayer2;
            selectedSpecies = this.selectedFishSpecies;
        } else if (!this.selectFishSpecies) {
            speciesLayer$ = this.dataSelectionService.speciesLayer1;
            selectedSpecies = this.selectedDragonflySpecies;
        } else if (this.plotLayerSelection === 'dragonfly') {
            speciesLayer$ = this.dataSelectionService.speciesLayer1;
            selectedSpecies = this.selectedDragonflySpecies;
        } /* if (this.plotLayerSelection === 'fish') */ else {
            speciesLayer$ = this.dataSelectionService.speciesLayer2;
            selectedSpecies = this.selectedFishSpecies;
        }

        const environmentColumnName = 'environment';

        const computationCrs = WGS_84;

        combineLatest([
            this.dataSelectionService.rasterLayer.pipe(
                mergeMap<RasterLayer | undefined, Observable<RasterLayer>>((layer) => (layer ? of(layer) : of())),
            ),
            speciesLayer$.pipe(mergeMap<VectorLayer | undefined, Observable<VectorLayer>>((layer) => (layer ? of(layer) : of()))),
        ])
            .pipe(
                first(),
                tap(() => {
                    this.plotLoading.next(true);
                    this.plotData.next(undefined);

                    this.plotSpecies = selectedSpecies ?? '';
                    this.plotEnvironmentLayer = this.selectedEnvironmentLayer ? this.selectedEnvironmentLayer.displayName : '';
                }),
                mergeMap(([rasterLayer, speciesLayer]) =>
                    combineLatest([
                        this.projectService.getWorkflow(rasterLayer.workflowId),
                        this.projectService.getWorkflow(speciesLayer.workflowId),
                        this.projectService.getLayerMetadata(rasterLayer),
                        this.projectService.getLayerMetadata(speciesLayer),
                    ]),
                ),
                mergeMap(([rasterWorkflow, speciesWorkflow, rasterMetadata, vectorMetadata]) => {
                    let rasterOperator = rasterWorkflow.operator;
                    if (!rasterMetadata.spatialReference.equals(computationCrs.spatialReference)) {
                        rasterOperator = {
                            type: 'Reprojection',
                            params: {
                                targetSpatialReference: computationCrs.spatialReference.srsString,
                            },
                            sources: {
                                source: rasterOperator,
                            },
                        };
                    }

                    let vectorOperator = speciesWorkflow.operator;
                    if (!vectorMetadata.spatialReference.equals(computationCrs.spatialReference)) {
                        vectorOperator = {
                            type: 'Reprojection',
                            params: {
                                targetSpatialReference: computationCrs.spatialReference.srsString,
                            },
                            sources: {
                                source: vectorOperator,
                            },
                        };
                    }

                    return this.projectService.registerWorkflow({
                        type: 'Vector',
                        operator: {
                            type: 'RasterVectorJoin',
                            params: {
                                names: {
                                    type: 'names',
                                    values: [environmentColumnName],
                                },
                                temporalAggregation: 'none',
                                featureAggregation: 'first',
                            },
                            sources: {
                                rasters: [rasterOperator],
                                vector: vectorOperator,
                            },
                        } as RasterVectorJoinDict,
                    });
                }),
                mergeMap((workflowId) =>
                    combineLatest([
                        this.projectService.getWorkflow(workflowId),
                        this.projectService.getWorkflowMetaData(workflowId) as Observable<VectorResultDescriptorDict>,
                        this.dataSelectionService.dataRange,
                    ]),
                ),
                mergeMap(([workflow, metadata, dataRange]) => {
                    let plotWorkflow: LegacyTypedOperatorOperator;

                    if (
                        environmentColumnName in metadata.columns &&
                        metadata.columns[environmentColumnName].measurement.type === 'classification'
                    ) {
                        plotWorkflow = {
                            type: 'ClassHistogram',
                            params: {
                                columnName: environmentColumnName,
                            },
                            sources: {
                                source: workflow.operator,
                            },
                        };
                    } else {
                        plotWorkflow = {
                            type: 'Histogram',
                            params: {
                                // TODO: get params from selected data
                                attributeName: 'band',
                                buckets: {
                                    type: 'number',
                                    value: 20,
                                },
                                bounds: dataRange,
                                columnName: environmentColumnName,
                            },
                            sources: {
                                source: workflow.operator,
                            },
                        };
                    }

                    return this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: plotWorkflow,
                    });
                }),
                mergeMap((workflowId) =>
                    combineLatest([
                        of(workflowId),
                        this.userService.getSessionTokenForRequest(),
                        this.projectService.getTimeOnce(),
                        this.mapService.getViewportSizeStream(),
                        this.projectService.getSpatialReferenceStream(),
                    ]),
                ),
                mergeMap(([workflowId, sessionToken, time, viewport, crs]) =>
                    this.backend.getPlot(
                        workflowId,
                        {
                            time: time.toDict(),
                            bbox: extentToBboxDict(
                                this.spatialReferenceService.reprojectExtent(viewport.extent, crs, computationCrs.spatialReference),
                            ),
                            crs: computationCrs.spatialReference.srsString,
                            // TODO: set reasonable size
                            spatialResolution: [0.1, 0.1],
                        },
                        sessionToken,
                    ),
                ),
                first(),
            )
            .subscribe({
                next: (plotData) => {
                    this.plotData.next(plotData.data);
                    this.plotLoading.next(false);
                },
                error: () => {
                    // TODO: react on error?
                    this.plotLoading.next(false);
                },
            });
    }

    thumbLabelMonthDisplay(value: number): string | number {
        switch (value) {
            case 1:
                return 'Januar';
            case 2:
                return 'Februar';
            case 3:
                return 'März';
            case 4:
                return 'April';
            case 5:
                return 'Mai';
            case 6:
                return 'Juni';
            case 7:
                return 'Juli';
            case 8:
                return 'August';
            case 9:
                return 'September';
            case 10:
                return 'Oktober';
            case 11:
                return 'November';
            case 12:
                return 'Dezember';
            default:
                return '';
        }
    }

    setMonth(value: number | null): void {
        if (!value) {
            return;
        }

        this.currentMonth = value;

        this.dataSelectionService.setTimeSteps(
            [...generateYearlyTimeSteps(START_YEAR, END_YEAR, this.currentMonth)],
            (currentTime: Time, timeStep: Time): boolean => currentTime.start.year() === timeStep.start.year(),
        );
    }
}

function* generateYearlyTimeSteps(yearStart: number, yearEnd: number, fixedMonth: number): IterableIterator<Time> {
    if (yearStart > yearEnd) {
        throw Error('start must be before end');
    }
    if (fixedMonth < 1 || fixedMonth > 12) {
        throw Error('month must be between 1 and 12');
    }

    const month = fixedMonth.toString().padStart(2, '0');
    const nextMonth = fixedMonth === 12 ? '01' : (fixedMonth + 1).toString().padStart(2, '0');

    for (let year = yearStart; year <= yearEnd; ++year) {
        const nextYear = fixedMonth === 12 ? year + 1 : year;

        const dateStart = `${year}-${month}-01T00:00:00.000Z`;
        const dateEnd = `${nextYear}-${nextMonth}-01T00:00:00.000Z`;

        yield new Time(moment.utc(dateStart), moment.utc(dateEnd));
    }
}
