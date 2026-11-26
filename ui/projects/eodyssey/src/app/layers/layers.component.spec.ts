import {WritableSignal, signal} from '@angular/core';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {provideNativeDateAdapter} from '@angular/material/core';
import type {MatDatepicker} from '@angular/material/datepicker';
import View from 'ol/View';
import {Observable, of} from 'rxjs';
import {beforeEach, describe, expect, it, type Mock, vi} from 'vitest';
import type {CollectionItem} from '@geoengine/api-client';
import {LAYER_DB_ROOT_COLLECTION_ID, LayersService, Time, TimeStepDuration} from '@geoengine/common';
import {FootprintsService, MapService, ProjectService} from '@geoengine/core';
import {formatDate, LayersComponent} from './layers.component';
import {EOdysseyLayerService} from './layers.service';

describe('formatDate', () => {
    it('names a date as precisely as the time step of the data', () => {
        const time = Date.UTC(2024, 3, 5, 6, 7, 8);
        expect(formatDate(time)).toBe('05.04.2024');
        expect(formatDate(time, 'years')).toBe('2024');
        expect(formatDate(time, 'month')).toBe('04.2024');
        expect(formatDate(time, 'days')).toBe('05.04.2024');
        expect(formatDate(time, 'hour')).toBe('05.04.2024 06:07');
        expect(formatDate(time, 'seconds')).toBe('05.04.2024 06:07:08');
    });
});

describe('LayersComponent', () => {
    let fixture: ComponentFixture<LayersComponent>;
    let eodysseyLayerService: EOdysseyLayerService;
    const getLayerCollectionItems =
        vi.fn<(_provider: string, collection: string, offset?: number, limit?: number) => Promise<{items: unknown[]}>>();
    const getLayer = vi.fn();
    const registerAndGetLayerProcessingGraphId = vi.fn();
    const getProcessingGraphIdMetadata = vi.fn();
    const setTime = vi.fn().mockResolvedValue(undefined);
    const setTimeStepDuration = vi.fn();
    let mapView: View;
    let mapService: MapService;
    const listings: Record<string, {items: unknown[]}> = {
        [LAYER_DB_ROOT_COLLECTION_ID]: {
            items: [{type: 'collection', name: 'EDV', id: {providerId: 'provider', collectionId: 'edv'}, description: ''}],
        },
        edv: {
            items: [
                {
                    type: 'collection',
                    name: 'Sentinel',
                    id: {providerId: 'provider', collectionId: 'dataset'},
                    description: '',
                    properties: [
                        ['edv:dataset', 'sentinel'],
                        ['edv:defaultTime', '1775001600000'],
                        ['edv:timeStep', '{"step":1,"granularity":"days"}'],
                    ],
                },
            ],
        },
        dataset: {
            items: [
                {
                    type: 'collection',
                    name: 'Global',
                    description: '',
                    id: {providerId: 'provider', collectionId: 'region'},
                    properties: [['edv:crs', 'EPSG:4326']],
                },
            ],
        },
        region: {
            items: [
                {
                    type: 'layer',
                    name: 'Default',
                    id: {providerId: 'provider', layerId: 'vv'},
                    description: '',
                    properties: [
                        ['edv:presetKey', 'Default'],
                        ['edv:order', '10'],
                    ],
                },
                {
                    type: 'layer',
                    name: 'Alternate',
                    id: {providerId: 'provider', layerId: 'alternate'},
                    description: '',
                    properties: [
                        ['edv:presetKey', 'Alternate'],
                        ['edv:order', '20'],
                    ],
                },
            ],
        },
    };

    const coverage = (west: number, south: number, east: number, north: number): string =>
        JSON.stringify({
            type: 'Polygon',
            coordinates: [
                [
                    [west, south],
                    [east, south],
                    [east, north],
                    [west, north],
                    [west, south],
                ],
            ],
        });

    function mockCoverageCatalogue(): void {
        const dataset = (key: string, name: string): CollectionItem => ({
            type: 'collection',
            name,
            description: '',
            id: {providerId: 'provider', collectionId: key},
            properties: [['edv:dataset', key]],
        });
        const variant = (source: string, epsg: number): CollectionItem => ({
            type: 'collection',
            name: `Region ${epsg % 100}${epsg >= 32700 ? 'S' : 'N'}`,
            description: '',
            id: {providerId: 'provider', collectionId: `${source}-${epsg}`},
            properties: [
                ['edv:crs', `EPSG:${epsg}`],
                [
                    'edv:coverage',
                    coverage(
                        -180 + 6 * ((epsg % 100) - 1),
                        source === 'landsat' ? -80 : epsg >= 32700 ? -80 : 0,
                        -180 + 6 * (epsg % 100),
                        source === 'landsat' ? 84 : epsg >= 32700 ? 0 : 84,
                    ),
                ],
            ],
        });
        const pages: Record<string, unknown[]> = {
            [LAYER_DB_ROOT_COLLECTION_ID]: listings[LAYER_DB_ROOT_COLLECTION_ID].items,
            edv: [dataset('sentinel', 'A Sentinel'), dataset('landsat', 'B Landsat')],
            sentinel: [32632, 32655, 32755].map((epsg) => variant('sentinel', epsg)),
            landsat: [32632, 32655].map((epsg) => variant('landsat', epsg)),
        };
        getLayerCollectionItems.mockImplementation((_provider, collection, offset = 0, limit = 20) =>
            Promise.resolve({items: (pages[collection] ?? []).slice(offset, offset + limit)}),
        );
    }

    beforeEach(async () => {
        vi.clearAllMocks();
        getLayerCollectionItems
            .mockReset()
            .mockImplementation((_provider, collection) => Promise.resolve(listings[collection] ?? {items: []}));
        // no raster symbology, so no legend is loaded
        getLayer.mockReset().mockResolvedValue({name: 'Layer', symbology: undefined});
        registerAndGetLayerProcessingGraphId.mockReset().mockResolvedValue('workflow-id');
        getProcessingGraphIdMetadata.mockReset();
        mapView = new View({projection: 'EPSG:4326'});
        await TestBed.configureTestingModule({
            imports: [LayersComponent],
            providers: [
                {
                    provide: FootprintsService,
                    useValue: {
                        dateClass: (): string => '',
                        calendarOpen: signal(false),
                        overlayVisible: signal(false),
                        overlayLight: signal(false),
                        tooLarge: signal(false),
                        hasError: signal(false),
                        stepsOfSelection: signal([]),
                        layer: signal({key: 'provider/layer'}),
                        calendarStartView: signal('month'),
                        time: signal(undefined),
                        datesNeeded: signal(false),
                        dateFilter: (): boolean => true,
                        dateWithData: vi.fn(),
                        dateBeyondLoaded: vi.fn(),
                        stepsAround: vi.fn().mockReturnValue([]),
                        selectedOutsideData: signal(undefined),
                        datesFailed: signal(false),
                        retryDates: vi.fn(),
                        timeRange: signal({start: -Infinity, end: Infinity}),
                        datesReachTimeRange: vi.fn().mockReturnValue(false),
                        timeStep: signal({durationAmount: 1, durationUnit: 'day'}),
                        isLoading: signal(false),
                        datesStale: signal(false),
                        overlayLoading: signal(false),
                        overlayBlocked: signal(false),
                        overlayTooLarge: signal(false),
                        selectedDateEmpty: signal(false),
                        viewCoverage: signal(undefined),
                        showDate: vi.fn(),
                    },
                },
                provideNativeDateAdapter(),
                {
                    provide: LayersService,
                    useValue: {getLayerCollectionItems, getLayer, registerAndGetLayerProcessingGraphId, getProcessingGraphIdMetadata},
                },
                EOdysseyLayerService,
                MapService,
                {
                    provide: ProjectService,
                    useValue: {
                        getTimeStream: (): Observable<Time> => of(new Time(new Date('2026-04-01T00:00:00Z'))),
                        getTimeStepDurationStream: (): Observable<TimeStepDuration> => of({durationAmount: 1, durationUnit: 'month'}),
                        setTime,
                        setTimeStepDuration,
                    },
                },
            ],
        }).compileComponents();
        mapService = TestBed.inject(MapService);
        mapService.setView(mapView);
        eodysseyLayerService = TestBed.inject(EOdysseyLayerService);
        fixture = TestBed.createComponent(LayersComponent);
    });

    function mockCategorizedCatalogue(): void {
        const category = (key: string, name: string): CollectionItem => ({
            type: 'collection',
            name,
            description: '',
            id: {providerId: 'provider', collectionId: key},
        });
        const source = listings.edv.items[0] as Extract<CollectionItem, {type: 'collection'}>;
        const pages: Record<string, unknown[]> = {
            [LAYER_DB_ROOT_COLLECTION_ID]: listings[LAYER_DB_ROOT_COLLECTION_ID].items,
            // Deliberately unsorted. Categories have no edv:dataset or edv:crs metadata.
            edv: [category('radar', 'Radar Imagery'), category('optical', 'Optical Imagery')],
            optical: [
                {
                    ...source,
                    properties: [
                        ...(source.properties ?? []),
                        ['edv:citation', 'Source citation'],
                        ['edv:maxMapExtent', '{"width":1000000,"height":1400000}'],
                    ],
                },
            ],
            radar: [
                {
                    ...source,
                    name: 'Radar',
                    id: {providerId: 'provider', collectionId: 'radarSource'},
                    properties: [['edv:dataset', 'radar']],
                },
            ],
            dataset: listings.dataset.items,
            region: listings.region.items,
            radarSource: [{...(listings.dataset.items[0] as CollectionItem), id: {providerId: 'provider', collectionId: 'radarRegion'}}],
            radarRegion: [{...(listings.region.items[0] as CollectionItem), id: {providerId: 'provider', layerId: 'radar-layer'}}],
        };
        getLayerCollectionItems.mockImplementation((_provider, collection, offset = 0, limit = 20) =>
            Promise.resolve({items: (pages[collection] ?? []).slice(offset, offset + limit)}),
        );
    }

    function mockInitialSelectionCatalogue(categoryName = 'Sentinel-2', sourceName = 'Sentinel-2 Global Mosaics'): void {
        const collection = (key: string, name: string): CollectionItem => ({
            type: 'collection',
            name,
            description: '',
            id: {providerId: 'provider', collectionId: key},
        });
        const source = (key: string, name: string): CollectionItem => ({
            ...collection(key, name),
            properties: [['edv:dataset', key]],
        });
        const pages: Record<string, unknown[]> = {
            [LAYER_DB_ROOT_COLLECTION_ID]: listings[LAYER_DB_ROOT_COLLECTION_ID].items,
            // Both preferred entries sort after another entry. Their ids do not resemble their names.
            edv: [collection('optical', categoryName), collection('land', 'CLMS')],
            optical: [source('sentinelFirst', 'A Sentinel source'), source('dataset', sourceName)],
            // A matching source name in a different category must not affect selection.
            land: [source('clmsFirst', 'A CLMS source'), source('decoy', 'Sentinel-2 Global Mosaics')],
            sentinelFirst: listings.dataset.items,
            clmsFirst: listings.dataset.items,
            decoy: listings.dataset.items,
            dataset: [
                ...(listings.dataset.items as CollectionItem[]),
                {
                    ...collection('secondRegion', 'Z Region'),
                    properties: [['edv:crs', 'EPSG:3857']],
                },
            ],
            region: listings.region.items,
            secondRegion: listings.region.items,
        };
        getLayerCollectionItems.mockImplementation((_provider, key, offset = 0, limit = 20) =>
            Promise.resolve({items: (pages[key] ?? []).slice(offset, offset + limit)}),
        );
    }

    it('preselects the named category and source, then their first variant and preset, using server-provided ids', async () => {
        mockInitialSelectionCatalogue();
        fixture.detectChanges();
        await fixture.whenStable();

        expect(eodysseyLayerService.selectedCategory()).toMatchObject({key: 'optical', name: 'Sentinel-2'});
        expect(eodysseyLayerService.selectedDataSource()).toMatchObject({key: 'dataset', name: 'Sentinel-2 Global Mosaics'});
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:4326');
        expect(eodysseyLayerService.selectedPreset()?.key).toBe('Default');
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        const panels = (fixture.nativeElement as HTMLElement).querySelectorAll('mat-expansion-panel');
        expect(panels[0].classList.contains('mat-expanded')).toBe(false);
        expect(panels[1].classList.contains('mat-expanded')).toBe(true);
        expect(getLayerCollectionItems.mock.calls.map((call) => call[1])).not.toContain('secondRegion');
    });

    it.each([
        {category: 'Other imagery', source: 'Sentinel-2 Global Mosaics', expectedCategory: 'land', expectedSource: 'clmsFirst'},
        {category: 'Sentinel-2', source: 'Other mosaics', expectedCategory: 'optical', expectedSource: 'sentinelFirst'},
    ])('falls back to the first entry when the configured path is missing: $category / $source', async (testCase) => {
        mockInitialSelectionCatalogue(testCase.category, testCase.source);
        fixture.detectChanges();
        await fixture.whenStable();

        expect(eodysseyLayerService.selectedCategory()?.key).toBe(testCase.expectedCategory);
        expect(eodysseyLayerService.selectedDataSource()?.key).toBe(testCase.expectedSource);
        expect(eodysseyLayerService.selectedPreset()?.key).toBe('Default');
    });

    it('preserves a manually selected source on catalogue reload and chooses the first source in another category', async () => {
        mockInitialSelectionCatalogue();
        fixture.detectChanges();
        await fixture.whenStable();
        eodysseyLayerService.setSelectedDataSource('sentinelFirst');
        fixture.detectChanges();
        await fixture.whenStable();
        eodysseyLayerService.retryCatalogue();
        fixture.detectChanges();
        await fixture.whenStable();

        expect(eodysseyLayerService.selectedCategory()?.key).toBe('optical');
        expect(eodysseyLayerService.selectedDataSource()?.key).toBe('sentinelFirst');
        eodysseyLayerService.setSelectedCategory('land');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedDataSource()?.key).toBe('clmsFirst');
        eodysseyLayerService.retryCatalogue();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedCategory()?.key).toBe('land');
        expect(eodysseyLayerService.selectedDataSource()?.key).toBe('clmsFirst');
    });

    it('expands the first category and preselects its first source, variant and preset with their metadata', async () => {
        mockCategorizedCatalogue();
        fixture.detectChanges();
        await fixture.whenStable();
        const element = fixture.nativeElement as HTMLElement;
        const panels = element.querySelectorAll('mat-expansion-panel');
        expect(panels).toHaveLength(2);
        expect(panels[0].classList.contains('mat-expanded')).toBe(true);
        expect(panels[1].classList.contains('mat-expanded')).toBe(false);
        expect(eodysseyLayerService.selectedCategory()?.name).toBe('Optical Imagery');
        expect(eodysseyLayerService.selectedDataSource()).toMatchObject({
            key: 'sentinel',
            citation: 'Source citation',
            maxMapExtent: {width: 1000000, height: 1400000},
        });
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:4326');
        expect(eodysseyLayerService.selectedPreset()?.key).toBe('Default');
        // the first preset is on the map at once
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe(eodysseyLayerService.selectedPreset()?.layerId);
        expect(getLayerCollectionItems.mock.calls.map((call) => call[1])).not.toContain('radarRegion');
        eodysseyLayerService.applySelectedPreset();
        expect(eodysseyLayerService.appliedMaxMapExtent()).toEqual({width: 1000000, height: 1400000});
        expect(setTime).not.toHaveBeenCalled();
        await fixture.whenStable();
        expect(setTimeStepDuration).toHaveBeenCalledWith({durationAmount: 1, durationUnit: 'day'});
    });

    it('switches categories through the accordion and shows the first preset of the new data source', async () => {
        mockCategorizedCatalogue();
        fixture.detectChanges();
        await fixture.whenStable();
        const element = fixture.nativeElement as HTMLElement;
        (element.querySelectorAll('mat-expansion-panel-header')[1] as HTMLElement).click();
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(eodysseyLayerService.selectedCategory()?.key).toBe('radar');
        expect(eodysseyLayerService.selectedDataSource()?.key).toBe('radar');
        await vi.waitFor(() => {
            fixture.detectChanges();
            expect(eodysseyLayerService.selectedPreset()?.layerId).toBe('radar-layer');
            expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe('radar-layer');
        });
        const panels = element.querySelectorAll('mat-expansion-panel');
        expect(panels[0].classList.contains('mat-expanded')).toBe(false);
        expect(panels[1].classList.contains('mat-expanded')).toBe(true);
        (element.querySelectorAll('mat-expansion-panel-header')[0] as HTMLElement).click();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedDataSource()?.key).toBe('sentinel');
        expect(eodysseyLayerService.selectedPreset()?.key).toBe('Default');
    });

    it('paginates category and data-source collections and permits an empty category', async () => {
        const collection = (key: string): CollectionItem => ({
            type: 'collection',
            name: key,
            description: '',
            id: {providerId: 'provider', collectionId: key},
        });
        const pages: Record<string, unknown[]> = {
            ...Object.fromEntries(Object.entries(listings).map(([key, page]) => [key, page.items])),
            edv: [...Array.from({length: 20}, (_, index) => collection(`Category ${index}`)), collection('Imagery')],
            Imagery: [...Array.from({length: 20}, (_, index) => ({type: 'layer', name: String(index)})), ...listings.edv.items],
        };
        getLayerCollectionItems.mockImplementation((_provider, key, offset = 0, limit = 20) =>
            Promise.resolve({items: (pages[key] ?? []).slice(offset, offset + limit)}),
        );
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.categories()).toHaveLength(21);
        expect(eodysseyLayerService.selectedDataSource()).toBeUndefined();
        expect(eodysseyLayerService.canApplyPreset()).toBe(false);
        expect(getLayerCollectionItems.mock.calls.some((call) => call[1] === 'edv' && call[2] === 20)).toBe(true);
        expect(getLayerCollectionItems.mock.calls.some((call) => call[1] === 'Imagery' && call[2] === 20)).toBe(true);
        eodysseyLayerService.setSelectedCategory('Imagery');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedCategory()?.key).toBe('Imagery');
        expect(eodysseyLayerService.selectedDataSource()?.key).toBe('sentinel');
        await vi.waitFor(() => expect(eodysseyLayerService.selectedPreset()?.key).toBe('Default'));
    });

    describe('picking a calendar cell of the size of the time step', () => {
        const picker = {close: vi.fn()};
        const startView = (view: string): void => {
            const footprints = TestBed.inject(FootprintsService) as unknown as {calendarStartView: WritableSignal<string>};
            footprints.calendarStartView.set(view);
        };

        it('selects the month and closes the picker for a time step of months', async () => {
            startView('year');
            picker.close.mockClear();

            await fixture.componentInstance.selectCoarseUnit(new Date(2026, 2, 1), 'year', picker as unknown as MatDatepicker<Date>);

            expect(picker.close).toHaveBeenCalledOnce();
            expect(setTime).toHaveBeenCalledWith(new Time(new Date(Date.UTC(2026, 2, 1))));
        });

        it('leaves the picker open for a time step of days, so a day can still be picked', async () => {
            startView('month');
            picker.close.mockClear();

            await fixture.componentInstance.selectCoarseUnit(new Date(2026, 2, 1), 'year', picker as unknown as MatDatepicker<Date>);

            expect(picker.close).not.toHaveBeenCalled();
            expect(setTime).not.toHaveBeenCalled();
        });
    });

    describe('dates of the layer', () => {
        interface FootprintsMock {
            time: WritableSignal<Time | undefined>;
            datesNeeded: WritableSignal<boolean>;
            dateWithData: Mock;
            dateBeyondLoaded: Mock;
            stepsAround: Mock;
            datesFailed: WritableSignal<boolean>;
            retryDates: Mock;
            selectedOutsideData: WritableSignal<{direction: 1 | -1; step: {time: {start: number; end: number}}} | undefined>;
            isLoading: WritableSignal<boolean>;
            datesStale: WritableSignal<boolean>;
            overlayVisible: WritableSignal<boolean>;
            overlayLoading: WritableSignal<boolean>;
            overlayBlocked: WritableSignal<boolean>;
            selectedDateEmpty: WritableSignal<boolean>;
            timeRange: WritableSignal<{start: number; end: number} | undefined>;
            datesReachTimeRange: Mock;
            stepsOfSelection: WritableSignal<unknown[]>;
            viewCoverage: WritableSignal<'full' | 'part' | 'none' | 'unknown' | undefined>;
        }
        const footprints = (): FootprintsMock => TestBed.inject(FootprintsService) as unknown as FootprintsMock;

        it('says how much of the view the date covers while the availability is on the map', async () => {
            footprints().time.set(new Time(Date.UTC(2026, 3, 1)));
            const element = fixture.nativeElement as HTMLElement;
            const status = (): string => element.querySelector('.coverage-status')?.textContent?.trim() ?? '';
            fixture.detectChanges();
            await fixture.whenStable();
            expect(footprints().datesNeeded()).toBe(true);
            expect(footprints().overlayVisible()).toBe(false);
            const statusLine = (): string => element.querySelector('.coverage-status[role=status]')?.textContent?.trim() ?? '';

            footprints().viewCoverage.set('part');
            fixture.detectChanges();
            expect(statusLine()).toBe('');

            element.querySelector<HTMLElement>('.availability-toggle button')!.click();
            fixture.detectChanges();
            expect(footprints().overlayVisible()).toBe(true);
            expect(status()).toContain('on 01.04.2026');
            expect(statusLine()).toBe('Scenes cover part of this view');
            expect(element.querySelector('.status-dot.status-part')).not.toBeNull();

            // the imagery is on, but its tiles load only further in than the boxes
            eodysseyLayerService.shown.set('preset');
            eodysseyLayerService.zoomRequired.set(true);
            fixture.detectChanges();
            expect(statusLine()).toBe('Scenes cover part of this view. Zoom in to load the imagery.');
            eodysseyLayerService.zoomRequired.set(false);
            fixture.detectChanges();
            expect(statusLine()).toBe('Scenes cover part of this view');

            footprints().viewCoverage.set('none');
            footprints().overlayLoading.set(true);
            fixture.detectChanges();
            expect(statusLine()).toBe('No scenes in this view on this date');
            expect(element.querySelector('.coverage-stale')).not.toBeNull();

            footprints().viewCoverage.set('unknown');
            fixture.detectChanges();
            expect(statusLine()).toBe('Not known for this visualization');

            footprints().overlayBlocked.set(true);
            fixture.detectChanges();
            expect(statusLine()).toBe('Zoom in to see where the scenes of this date are.');

            // the dates of the view say that the date has no scenes, so zooming in is no use
            footprints().selectedDateEmpty.set(true);
            fixture.detectChanges();
            expect(statusLine()).toBe('No scenes in this view on this date');
        });

        it('shows the dates around the selected one as bars that select their date', async () => {
            const day = (d: number, status: string): unknown => ({
                time: {start: Date.UTC(2026, 3, d), end: Date.UTC(2026, 3, d + 1)},
                status,
            });
            footprints().time.set(new Time(Date.UTC(2026, 3, 2)));
            footprints().stepsAround.mockReturnValue([day(1, 'data'), day(2, 'empty'), day(3, 'unknown')]);
            fixture.detectChanges();
            await fixture.whenStable();

            const ticks = (fixture.nativeElement as HTMLElement).querySelectorAll<HTMLButtonElement>('.date-strip .tick');
            expect(Array.from(ticks, (tick) => tick.className.replace('mat-mdc-tooltip-trigger ', ''))).toEqual([
                'tick tick-data',
                'tick tick-selected',
                'tick tick-unknown',
            ]);
            expect(ticks[0].getAttribute('aria-label')).toBe('01.04.2026: scenes');

            setTime.mockClear();
            ticks[0].click();
            expect(setTime).toHaveBeenCalledWith(new Time(Date.UTC(2026, 3, 1)));
        });

        it('reaches the bars of the strip with the keyboard, the selected one first', async () => {
            const day = (d: number, status: string): unknown => ({
                time: {start: Date.UTC(2026, 3, d), end: Date.UTC(2026, 3, d + 1)},
                status,
            });
            footprints().time.set(new Time(Date.UTC(2026, 3, 2)));
            footprints().stepsAround.mockReturnValue([day(1, 'data'), day(2, 'empty'), day(3, 'unknown')]);
            fixture.detectChanges();
            await fixture.whenStable();

            const ticks = (fixture.nativeElement as HTMLElement).querySelectorAll<HTMLButtonElement>('.date-strip .tick');
            expect(Array.from(ticks, (tick) => tick.tabIndex)).toEqual([-1, 0, -1]);
            ticks[1].focus();
            ticks[1].dispatchEvent(new KeyboardEvent('keydown', {key: 'ArrowRight'}));
            expect(document.activeElement).toBe(ticks[2]);
            ticks[2].dispatchEvent(new KeyboardEvent('keydown', {key: 'ArrowRight'}));
            expect(document.activeElement).toBe(ticks[2]);
            ticks[2].dispatchEvent(new KeyboardEvent('keydown', {key: 'ArrowLeft'}));
            expect(document.activeElement).toBe(ticks[1]);
        });

        it('keeps the place of the strip while the dates load, so the panel does not move', async () => {
            const day = (d: number, status: string): unknown => ({
                time: {start: Date.UTC(2026, 3, d), end: Date.UTC(2026, 3, d + 1)},
                status,
            });
            const element = fixture.nativeElement as HTMLElement;
            footprints().time.set(new Time(Date.UTC(2026, 3, 1)));

            // the first dates of a layer load: an empty strip with a bar, and the line below it keeps its height
            footprints().isLoading.set(true);
            fixture.detectChanges();
            await fixture.whenStable();
            expect(element.querySelector('.date-strip')).not.toBeNull();
            expect(element.querySelectorAll('.date-strip .tick')).toHaveLength(0);
            expect(element.querySelector('.date-strip .strip-loading')).not.toBeNull();
            expect(element.querySelector('.date-strip')?.getAttribute('aria-busy')).toBe('true');

            // the dates of an earlier view stay, faded, while those of a moved view load
            footprints().stepsAround.mockReturnValue([day(1, 'data'), day(2, 'empty')]);
            footprints().datesStale.set(true);
            // the strip is computed from signals, which the mocked `stepsAround` is not
            footprints().time.set(new Time(Date.UTC(2026, 3, 1)));
            fixture.detectChanges();
            expect(element.querySelectorAll('.date-strip .tick')).toHaveLength(2);
            expect(element.querySelector('.date-strip.strip-stale')).not.toBeNull();

            footprints().isLoading.set(false);
            footprints().datesStale.set(false);
            fixture.detectChanges();
            expect(element.querySelector('.strip-loading')).toBeNull();
            expect(element.querySelector('.strip-stale')).toBeNull();
        });

        it('shows the last dates of the data, none of them selected, where the selected one lies after them', async () => {
            const day = (d: number, status: string): {time: {start: number; end: number}; status: string} => ({
                time: {start: Date.UTC(2026, 3, d), end: Date.UTC(2026, 3, d + 1)},
                status,
            });
            const element = fixture.nativeElement as HTMLElement;
            footprints().stepsAround.mockReturnValue([day(1, 'data'), day(2, 'empty')]);
            footprints().selectedOutsideData.set({direction: 1, step: day(2, 'empty')});
            footprints().time.set(new Time(Date.UTC(2026, 3, 9)));
            fixture.detectChanges();
            await fixture.whenStable();
            // the last dates of the data, none of them selected
            expect(element.querySelectorAll('.date-strip .tick')).toHaveLength(2);
            expect(element.querySelector('.date-strip .tick-selected')).toBeNull();
        });

        it('moves a date after the data of a new source to its last date, silently at start, else telling the map', async () => {
            const day = (d: number, status: string): {time: {start: number; end: number}; status: string} => ({
                time: {start: Date.UTC(2026, 3, d), end: Date.UTC(2026, 3, d + 1)},
                status,
            });
            setTime.mockImplementation((time: Time) => {
                footprints().time.set(time);
                return Promise.resolve();
            });
            mockCategorizedCatalogue();
            const element = fixture.nativeElement as HTMLElement;
            footprints().stepsOfSelection.set([day(1, 'data'), day(2, 'data')]);
            footprints().stepsAround.mockReturnValue([day(1, 'data'), day(2, 'data')]);
            footprints().selectedOutsideData.set({direction: 1, step: day(2, 'data')});
            footprints().time.set(new Time(Date.UTC(2026, 3, 9)));
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();
            // the start date, which the user did not choose, moves without a note
            expect(setTime).toHaveBeenCalledTimes(1);
            expect(footprints().time()?.start.valueOf()).toBe(Date.UTC(2026, 3, 2));
            expect(eodysseyLayerService.dateMoved()).toBeUndefined();

            // a date the user chose moves with a note on the map, which offers the date before
            footprints().time.set(new Time(Date.UTC(2026, 3, 9)));
            (element.querySelectorAll('mat-expansion-panel-header')[1] as HTMLElement).click();
            await vi.waitFor(() => {
                fixture.detectChanges();
                expect(eodysseyLayerService.selectedDataSource()?.key).toBe('radar');
                expect(footprints().time()?.start.valueOf()).toBe(Date.UTC(2026, 3, 2));
            });
            expect(eodysseyLayerService.dateMoved()).toEqual({
                from: new Time(Date.UTC(2026, 3, 9)),
                to: Date.UTC(2026, 3, 2),
                direction: 1,
            });
        });

        it('moves a date before the data also without loaded dates, by the time of the data', async () => {
            setTime.mockImplementation((time: Time) => {
                footprints().time.set(time);
                return Promise.resolve();
            });
            mockCategorizedCatalogue();
            // no dates, e.g. a view that needs too many searches; the service tells from the time of the data
            footprints().stepsOfSelection.set([]);
            footprints().selectedOutsideData.set({
                direction: -1,
                step: {time: {start: Date.UTC(2026, 5, 1), end: Date.UTC(2026, 5, 1) + 1}},
            });
            footprints().time.set(new Time(Date.UTC(2026, 3, 9)));
            fixture.detectChanges();
            await fixture.whenStable();
            fixture.detectChanges();
            expect(footprints().time()?.start.valueOf()).toBe(Date.UTC(2026, 5, 1));
        });

        it('says that the dates could not be loaded and loads them again on retry', async () => {
            const element = fixture.nativeElement as HTMLElement;
            footprints().datesFailed.set(true);
            fixture.detectChanges();
            await fixture.whenStable();
            expect(element.querySelector('[role=alert] .catalogue-error')?.textContent).toContain('Could not load the dates');
            element.querySelector<HTMLButtonElement>('.retry-dates')?.click();
            expect(footprints().retryDates).toHaveBeenCalled();
        });

        it('marks the date of today in the strip', async () => {
            const now = Date.now();
            const start = now - (now % 86_400_000);
            footprints().time.set(new Time(start));
            footprints().stepsAround.mockReturnValue([
                {time: {start: start - 86_400_000, end: start}, status: 'data'},
                {time: {start, end: start + 86_400_000}, status: 'empty'},
            ]);
            fixture.detectChanges();
            await fixture.whenStable();

            const ticks = (fixture.nativeElement as HTMLElement).querySelectorAll<HTMLButtonElement>('.date-strip .tick');
            expect(ticks[1].getAttribute('aria-label')).toMatch(/: no scenes, today$/);
        });

        it('names where the buttons below the date go', async () => {
            footprints().time.set(new Time(Date.UTC(2026, 3, 1)));
            const label = (name: string): string =>
                (fixture.nativeElement as HTMLElement)
                    .querySelector(`.time-${name}`)
                    ?.textContent?.replace(/navigate_\w+/, '')
                    .trim() ?? '';
            fixture.detectChanges();
            await fixture.whenStable();
            // the data source cannot tell its dates
            expect(label('backwards')).toBe('1 day back');
            expect(label('forward')).toBe('1 day on');

            footprints().dateWithData.mockImplementation((direction: number) =>
                direction === 1 ? new Time(Date.UTC(2026, 3, 17)) : undefined,
            );
            footprints().dateBeyondLoaded.mockReturnValue(new Time(Date.UTC(2026, 2, 31)));
            footprints().time.set(new Time(Date.UTC(2026, 3, 2)));
            fixture.detectChanges();
            expect(label('forward')).toBe('17.04.2026');
            expect(label('backwards')).toBe('Earlier');

            setTime.mockClear();
            await fixture.componentInstance.timeBackwards();
            expect(setTime).toHaveBeenCalledWith(new Time(Date.UTC(2026, 2, 31)));
        });

        it('offers no later dates where the loaded dates reach now, and none beyond the time of the data', async () => {
            footprints().time.set(new Time(Date.UTC(2026, 3, 1)));
            const button = (name: string): HTMLButtonElement =>
                (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>(`.time-${name}`)!;
            footprints().datesReachTimeRange.mockImplementation((direction: number) => direction === 1);
            fixture.detectChanges();
            await fixture.whenStable();
            expect(button('forward').textContent).toContain('No later dates');
            expect(button('forward').disabled).toBe(true);
            expect(button('backwards').disabled).toBe(false);

            // no dates are loaded, e.g. for a date before the data starts: stepping stays within the data's time
            footprints().datesReachTimeRange.mockReturnValue(false);
            footprints().timeRange.set({start: Date.UTC(2026, 3, 1), end: Date.UTC(2026, 5, 1)});
            fixture.detectChanges();
            expect(button('backwards').textContent).toContain('No earlier dates');
            expect(button('backwards').disabled).toBe(true);
            expect(button('forward').disabled).toBe(false);
            setTime.mockClear();
            await fixture.componentInstance.timeBackwards();
            expect(setTime).not.toHaveBeenCalled();
        });

        it('steps on where the loaded dates reach now but the data source cannot tell them', async () => {
            footprints().time.set(new Time(Date.UTC(2026, 10, 20)));
            footprints().datesReachTimeRange.mockReturnValue(true);
            footprints().stepsOfSelection.set(
                [19, 20, 21, 22].map((d) => ({time: {start: Date.UTC(2026, 10, d), end: Date.UTC(2026, 10, d + 1)}, status: 'unknown'})),
            );
            const button = (name: string): HTMLButtonElement =>
                (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>(`.time-${name}`)!;
            fixture.detectChanges();
            await fixture.whenStable();
            expect(button('forward').disabled).toBe(false);
            expect(button('forward').textContent).toContain('1 day on');
            expect(button('backwards').disabled).toBe(false);

            // the steps after the selected one are known to be empty: there are no later dates
            footprints().stepsOfSelection.update((steps) =>
                steps.map((step, i) => (i < 2 ? step : {...(step as object), status: 'empty'})),
            );
            fixture.detectChanges();
            expect(button('forward').textContent).toContain('No later dates');
            expect(button('backwards').disabled).toBe(false);
        });

        it('goes nowhere while the dates of a newly selected source load', async () => {
            footprints().time.set(new Time(Date.UTC(2026, 3, 1)));
            footprints().isLoading.set(true);
            fixture.detectChanges();
            await fixture.whenStable();
            const button = (name: string): HTMLButtonElement =>
                (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>(`.time-${name}`)!;
            expect(button('backwards').disabled).toBe(true);
            expect(button('forward').disabled).toBe(true);
            setTime.mockClear();
            await fixture.componentInstance.timeBackwards();
            expect(setTime).not.toHaveBeenCalled();

            // the same until the metadata of the new layer tells the time of its data
            footprints().isLoading.set(false);
            footprints().timeRange.set(undefined);
            fixture.detectChanges();
            expect(button('forward').disabled).toBe(true);

            // a source without presets has no dates, so the buttons step by the time step
            (footprints() as unknown as {layer: WritableSignal<unknown>}).layer.set(undefined);
            fixture.detectChanges();
            expect(button('forward').disabled).toBe(false);
            expect(button('forward').textContent).toContain('1 day on');
        });

        it('goes to the next date with data', async () => {
            footprints().time.set(new Time(Date.UTC(2026, 3, 1)));
            footprints().dateWithData.mockImplementation((direction: number) =>
                direction === 1 ? new Time(Date.UTC(2026, 3, 17)) : undefined,
            );
            setTime.mockClear();

            await fixture.componentInstance.timeForward();

            expect(setTime).toHaveBeenCalledWith(new Time(Date.UTC(2026, 3, 17)));
        });

        it('steps by the time step when no date with data is known', async () => {
            footprints().time.set(new Time(Date.UTC(2026, 3, 1)));
            footprints().dateWithData.mockReturnValue(undefined);
            setTime.mockClear();

            await fixture.componentInstance.timeBackwards();

            expect(setTime).toHaveBeenCalledOnce();
        });
    });

    it('preselects the matching coverage on source switches and preserves the initial choice while panning', async () => {
        mockCoverageCatalogue();
        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32632');

        mapView.setCenter([148.5, -35.5]);
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32632');
        eodysseyLayerService.setSelectedDataSource('landsat');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32655');
        eodysseyLayerService.setSelectedDataSource('sentinel');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32755');
        expect(setTime).not.toHaveBeenCalled();
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
    });

    it('reads each source maximum map extent and changes the applied limit with the shown preset', async () => {
        const dataset = listings.edv.items[0] as Extract<CollectionItem, {type: 'collection'}>;
        getLayerCollectionItems.mockImplementation((_provider, collection) => {
            if (collection === 'edv') {
                return Promise.resolve({
                    items: [
                        {...dataset, properties: [...(dataset.properties ?? []), ['edv:maxMapExtent', '{"width":500000,"height":700000}']]},
                        {
                            ...dataset,
                            name: 'Z Other source',
                            id: {...dataset.id, collectionId: 'other-dataset'},
                            properties: [
                                ['edv:dataset', 'other'],
                                ['edv:maxMapExtent', '{"width":1000000,"height":1400000}'],
                            ],
                        },
                    ],
                });
            }
            if (collection === 'other-dataset') {
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Global',
                            description: '',
                            id: {providerId: 'provider', collectionId: 'other-region'},
                            properties: [['edv:crs', 'EPSG:4326']],
                        },
                    ],
                });
            }
            if (collection === 'other-region') {
                return Promise.resolve({
                    items: [{...(listings.region.items[0] as CollectionItem), id: {providerId: 'provider', layerId: 'other-layer'}}],
                });
            }
            return Promise.resolve(listings[collection] ?? {items: []});
        });
        fixture.detectChanges();
        await fixture.whenStable();
        // the selection is the subject here, so nothing is shown on its own: "None", which applies only on request
        fixture.componentInstance.show();
        fixture.detectChanges();
        expect(eodysseyLayerService.selectedDataSource()?.maxMapExtent).toEqual({width: 500000, height: 700000});
        expect(eodysseyLayerService.appliedDataSource()).toBeUndefined();
        eodysseyLayerService.applySelectedPreset();
        expect(eodysseyLayerService.appliedDataSource()?.maxMapExtent).toEqual({width: 500000, height: 700000});
        expect(eodysseyLayerService.appliedMaxMapExtent()).toEqual({width: 500000, height: 700000});
        // a new data source shows its first preset, and with it its limit
        eodysseyLayerService.setSelectedDataSource('other');
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(eodysseyLayerService.selectedDataSource()?.maxMapExtent).toEqual({width: 1000000, height: 1400000});
        expect(eodysseyLayerService.appliedDataSource()?.maxMapExtent).toEqual({width: 1000000, height: 1400000});
        expect(eodysseyLayerService.appliedMaxMapExtent()).toEqual({width: 1000000, height: 1400000});
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe('other-layer');
    });

    it('ignores obsolete preset limits and leaves sources without extent metadata unrestricted', async () => {
        getLayerCollectionItems.mockImplementation((_provider, collection) =>
            Promise.resolve(
                collection === 'region'
                    ? {
                          items: listings.region.items.map((item) => ({
                              ...(item as CollectionItem),
                              properties: [...((item as CollectionItem).properties ?? []), ['edv:minTileMatrixIndex', '-1']],
                          })),
                      }
                    : (listings[collection] ?? {items: []}),
            ),
        );
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedPreset()).toBeDefined();
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe(eodysseyLayerService.selectedPreset()?.layerId);
        expect(eodysseyLayerService.appliedMaxMapExtent()).toBeUndefined();
    });

    it.each([
        '',
        'null',
        '[]',
        '{}',
        '{"width":1}',
        '{"width":0,"height":1}',
        '{"width":1,"height":-1}',
        '{"width":"1","height":1}',
        '{"width":1e999,"height":1}',
        '{"width":1,"height":1e999}',
    ])('reports invalid maximum map extent metadata %j', async (value) => {
        const dataset = listings.edv.items[0] as Extract<CollectionItem, {type: 'collection'}>;
        getLayerCollectionItems.mockImplementation((_provider, collection) =>
            Promise.resolve(
                collection === 'edv'
                    ? {items: [{...dataset, properties: [...(dataset.properties ?? []), ['edv:maxMapExtent', value]]}]}
                    : (listings[collection] ?? {items: []}),
            ),
        );
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.catalogueError()).toContain('Invalid edv:maxMapExtent');
    });

    it('waits for the replacement view center and removes the old view listener', async () => {
        mockCoverageCatalogue();
        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        await fixture.whenStable();
        const replacement = new View({projection: 'EPSG:4326'});
        mapService.setView(replacement);
        fixture.detectChanges();
        expect(eodysseyLayerService.mapCenter()).toBeUndefined();
        mapView.setCenter([148.5, -35.5]);
        expect(eodysseyLayerService.mapCenter()).toBeUndefined();
        eodysseyLayerService.setSelectedDataSource('landsat');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32632');
        replacement.setCenter([148.5, -35.5]);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32655');
    });

    it('removes the map center listener when the service is destroyed', async () => {
        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.mapCenter()).toEqual({longitude: 9, latitude: 50});

        TestBed.resetTestingModule();
        mapView.setCenter([148.5, -35.5]);
        expect(eodysseyLayerService.mapCenter()).toEqual({longitude: 9, latitude: 50});
    });

    it('preserves a manual variant chosen before the map initializes', async () => {
        mockCoverageCatalogue();
        fixture.detectChanges();
        await fixture.whenStable();
        eodysseyLayerService.setSelectedVariant('EPSG:32655');
        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32655');
        expect(eodysseyLayerService.mapCenterVariantKey()).toBe('EPSG:32632');
    });

    it('shows loading indicators for both lists until catalogue discovery finishes', async () => {
        const element = fixture.nativeElement as HTMLElement;
        let resolveRoot!: (value: (typeof listings)[typeof LAYER_DB_ROOT_COLLECTION_ID]) => void;
        getLayerCollectionItems.mockImplementationOnce(
            () =>
                new Promise((resolve) => {
                    resolveRoot = resolve;
                }),
        );
        fixture.detectChanges();
        await Promise.resolve();
        fixture.detectChanges();
        expect(element.querySelectorAll('mat-spinner').length).toBe(2);
        expect(element.querySelector('geoengine-data-source-navigation mat-accordion')?.getAttribute('aria-busy')).toBe('true');
        expect(element.querySelector('.visualization-presets')?.getAttribute('aria-busy')).toBe('true');
        resolveRoot(listings[LAYER_DB_ROOT_COLLECTION_ID]);
        await fixture.whenStable();
        fixture.detectChanges();
        expect(element.querySelectorAll('mat-spinner').length).toBe(0);
        expect(element.textContent).toContain('Sentinel');
    });

    it('loads later coverage and preset pages and applies a map-center choice only when a preset is applied', async () => {
        const regionNumbers = [...Array.from({length: 19}, (_, index) => index + 1), 32];
        const regionCollections = regionNumbers.map((zone) => {
            return {
                type: 'collection',
                name: `Region ${String(zone).padStart(2, '0')}N`,
                id: {providerId: 'provider', collectionId: `region-${zone}`},
                description: '',
                properties: [
                    ['edv:crs', `EPSG:${32600 + zone}`],
                    ['edv:coverage', coverage(-180 + 6 * (zone - 1), 0, -180 + 6 * zone, 84)],
                ],
            };
        });
        regionCollections.push({
            type: 'collection',
            name: 'Region 55S',
            id: {providerId: 'provider', collectionId: 'region55s'},
            description: '',
            properties: [
                ['edv:crs', 'EPSG:32755'],
                ['edv:coverage', coverage(144, -80, 150, 0)],
            ],
        });
        const presets = Array.from({length: 25}, (_, index) => ({
            type: 'layer',
            name: `Preset ${String(index + 1).padStart(2, '0')}`,
            id: {providerId: 'provider', layerId: `preset-${index + 1}`},
            description: '',
            properties: [
                ['edv:presetKey', `preset-${index + 1}`],
                ['edv:order', String(index + 1)],
            ],
        }));
        getLayerCollectionItems.mockImplementation((_provider, collection, offset = 0, limit = 20) => {
            const pages: Record<string, unknown[]> = {
                [LAYER_DB_ROOT_COLLECTION_ID]: [
                    {type: 'collection', name: 'EDV', id: {providerId: 'provider', collectionId: 'edv'}, description: ''},
                ],
                edv: [
                    {
                        type: 'collection',
                        name: 'Sentinel',
                        id: {providerId: 'provider', collectionId: 'dataset'},
                        description: '',
                        properties: [
                            ['edv:dataset', 'sentinel'],
                            ['edv:defaultTime', '1775001600000'],
                            ['edv:timeStep', '{"step":1,"granularity":"days"}'],
                        ],
                    },
                ],
                dataset: regionCollections,
                region55s: presets,
            };
            return Promise.resolve({items: pages[collection]?.slice(offset, offset + limit) ?? []});
        });

        mapView.setCenter([148.5, -35.5]);
        fixture.detectChanges();
        await fixture.whenStable();
        // the selection is the subject here, so nothing is shown on its own: "None", which applies only on request
        fixture.componentInstance.show();
        fixture.detectChanges();
        await vi.waitFor(() => expect(eodysseyLayerService.currentPresets()).toHaveLength(25));
        fixture.detectChanges();
        expect(eodysseyLayerService.currentVariants()).toHaveLength(21);
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32755');
        expect(eodysseyLayerService.sortedVariants()[0].name).toBe('Region 01N');
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        expect(setTime).not.toHaveBeenCalled();

        eodysseyLayerService.setSelectedPreset('preset-1');
        eodysseyLayerService.applySelectedPreset();
        const appliedLayer = eodysseyLayerService.mapTileLayer();
        const timeCalls = setTime.mock.calls.length;
        expect(appliedLayer).toEqual({dataConnectorId: 'provider', layerId: 'preset-1'});

        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32755');
        const useCenterButton = [...(fixture.nativeElement as HTMLElement).querySelectorAll('button')].find((button) =>
            button.textContent?.includes('Map center'),
        );
        expect(useCenterButton).toBeDefined();
        useCenterButton?.click();
        fixture.detectChanges();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32632');
        expect(eodysseyLayerService.mapTileLayer()).toEqual(appliedLayer);
        expect(setTime).toHaveBeenCalledTimes(timeCalls);
    });

    it('ends loading on failure and displays both indicators again during retry', async () => {
        const element = fixture.nativeElement as HTMLElement;
        getLayerCollectionItems.mockRejectedValueOnce(new Error('Catalogue unavailable'));
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(element.textContent).toContain('Catalogue unavailable');
        expect(element.querySelectorAll('mat-spinner').length).toBe(0);
        eodysseyLayerService.retryCatalogue();
        fixture.detectChanges();
        expect(element.querySelectorAll('mat-spinner').length).toBe(2);
        await fixture.whenStable();
        fixture.detectChanges();
        expect(element.querySelectorAll('mat-spinner').length).toBe(0);
        expect(element.textContent).not.toContain('Catalogue unavailable');
    });

    it('offers retry for an empty catalogue and discovers sources added later', async () => {
        let emptyCatalogue = true;
        getLayerCollectionItems.mockImplementation((_provider, collection) =>
            Promise.resolve(collection === 'edv' && emptyCatalogue ? {items: []} : (listings[collection] ?? {items: []})),
        );
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        expect(element.textContent).toContain('No data sources are available');
        const retry = element.querySelector<HTMLButtonElement>('.catalogue-notice button');
        expect(retry).not.toBeNull();

        emptyCatalogue = false;
        retry!.click();
        fixture.detectChanges();
        expect(element.querySelector('geoengine-data-source-navigation mat-accordion')?.getAttribute('aria-busy')).toBe('true');
        await fixture.whenStable();
        fixture.detectChanges();
        expect(element.textContent).not.toContain('No data sources are available');
        expect(element.textContent).toContain('Sentinel');
        expect(eodysseyLayerService.dataSources()).toHaveLength(1);
        expect(eodysseyLayerService.mapTileLayer()).toBeDefined();
    });

    it('shows the first preset for a new data source, then the chosen one, with the availability alone or over it', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        const tiles = (): HTMLElement[] => Array.from(element.querySelectorAll<HTMLElement>('.visualization-presets mat-list-item'));
        const active = (): string[] =>
            tiles().flatMap((tile) => (tile.classList.contains('preset-active') ? [tile.textContent.replace('check', '').trim()] : []));
        const footprints = TestBed.inject(FootprintsService);
        const toggle = (): HTMLElement => element.querySelector<HTMLElement>('.availability-toggle button')!;
        const presets = eodysseyLayerService.currentPresets();
        expect(eodysseyLayerService.selectedPresetKey()).toBe('Default');
        // the first preset is on the map at once, without a click
        expect(active()).toEqual([presets[0].displayName]);
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        expect(setTime).not.toHaveBeenCalled();

        tiles()[0].click();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(active()).toEqual([presets[0].displayName]);
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        expect(setTime).not.toHaveBeenCalled();

        tiles()[1].click();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe(presets[1].layerId);

        // the availability lies over the imagery as outlines
        toggle().click();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(footprints.overlayVisible()).toBe(true);
        expect(footprints.overlayLight()).toBe(true);
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe(presets[1].layerId);

        // "None" takes the imagery off, keeps the availability and goes back to the default preset for the dates
        tiles()[2].click();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(active()).toEqual(['None']);
        expect(footprints.overlayVisible()).toBe(true);
        expect(footprints.overlayLight()).toBe(false);
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        expect(eodysseyLayerService.selectedPresetKey()).toBe(presets[0].key);

        toggle().click();
        fixture.detectChanges();
        expect(footprints.overlayVisible()).toBe(false);
    });

    it('removes the visualization from the map without resetting the time when it is applied again', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[0].key);
        eodysseyLayerService.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.mapTileLayer()).toBeDefined();

        setTime.mockClear();
        fixture.componentInstance.show();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        expect(eodysseyLayerService.canApplyPreset()).toBe(true);

        eodysseyLayerService.applySelectedPreset();
        expect(eodysseyLayerService.mapTileLayer()).toBeDefined();
        expect(setTime).not.toHaveBeenCalled();
    });

    it('preserves the applied layer and time when returning to the layers panel', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.componentInstance.show(eodysseyLayerService.currentPresets()[0].key);
        fixture.detectChanges();
        await fixture.whenStable();
        const appliedLayer = eodysseyLayerService.mapTileLayer();
        setTime.mockClear();
        setTimeStepDuration.mockClear();

        fixture.destroy();
        fixture = TestBed.createComponent(LayersComponent);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.mapTileLayer()).toBe(appliedLayer);
        expect(setTime).not.toHaveBeenCalled();
        expect(setTimeStepDuration).not.toHaveBeenCalled();
    });

    it('loads the legend of the shown preset and handles a failed request', async () => {
        getLayer.mockRejectedValue(new Error('Layer unavailable'));
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        // the first preset is shown at once, with its legend
        expect(getLayer).toHaveBeenCalledWith('provider', 'vv');
        expect(element.querySelector('.legend-error')?.textContent).toContain('Failed to load legend');
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});

        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[1].key);
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(getLayer).toHaveBeenLastCalledWith('provider', 'alternate');
    });

    it('loads source and region collections and fetches presets from the single selected region', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.dataSources()).toHaveLength(1);
        expect(eodysseyLayerService.selectedVariant()?.collectionId).toBe('region');
        expect(eodysseyLayerService.currentPresets().map((preset) => preset.displayName)).toEqual(['Default', 'Alternate']);
        expect(getLayerCollectionItems.mock.calls.map((call) => call[1])).toEqual([
            LAYER_DB_ROOT_COLLECTION_ID,
            'edv',
            'dataset',
            'region',
        ]);
    });

    it('reports a missing source key within a category without traversing another folder level', async () => {
        const category = {type: 'collection', name: 'Imagery', id: {providerId: 'provider', collectionId: 'category'}, description: ''};
        getLayerCollectionItems.mockImplementation((_provider, collection) =>
            Promise.resolve(
                collection === 'category'
                    ? {
                          items: [
                              {
                                  type: 'collection',
                                  name: 'Unannotated',
                                  description: '',
                                  id: {providerId: 'provider', collectionId: 'unannotated'},
                              },
                          ],
                      }
                    : collection === 'edv'
                      ? {items: [category]}
                      : (listings[collection] ?? {items: []}),
            ),
        );
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.catalogueError()).toBe('Missing edv:dataset on Unannotated');
        expect(getLayerCollectionItems.mock.calls.map((call) => call[1])).not.toContain('unannotated');
    });

    it('rejects duplicate source identities instead of merging their regions', async () => {
        getLayerCollectionItems.mockImplementation((_provider, collection) =>
            Promise.resolve(
                collection === 'edv' ? {items: [...listings.edv.items, ...listings.edv.items]} : (listings[collection] ?? {items: []}),
            ),
        );
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.catalogueError()).toBe('Duplicate data source: sentinel');
    });

    it('rejects duplicate region identities instead of merging their preset collections', async () => {
        getLayerCollectionItems.mockImplementation((_provider, collection) =>
            Promise.resolve(
                collection === 'dataset'
                    ? {items: [...listings.dataset.items, ...listings.dataset.items]}
                    : (listings[collection] ?? {items: []}),
            ),
        );
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.catalogueError()).toBe('Duplicate region in Sentinel: EPSG:4326');
    });

    it('requires region collections rather than synthesizing a region from source-level layers', async () => {
        getLayerCollectionItems.mockImplementation((_provider, collection) =>
            Promise.resolve(collection === 'dataset' ? listings.region : (listings[collection] ?? {items: []})),
        );
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.dataSources()).toHaveLength(0);
        expect(eodysseyLayerService.currentPresets()).toHaveLength(0);
    });

    it('keeps variant and preset selection when collection and layer ids change', async () => {
        let deployment = 0;
        const deploymentListings: Array<Record<string, {items: unknown[]}>> = [
            {
                root: {items: [{type: 'collection', name: 'EDV', id: {providerId: 'p0', collectionId: 'edv0'}, description: ''}]},
                edv0: {
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p0', collectionId: 'ds0'},
                            description: '',
                            properties: [['edv:dataset', 'sentinel']],
                        },
                    ],
                },
                ds0: {
                    items: [
                        {
                            type: 'collection',
                            name: 'Region 32N',
                            id: {providerId: 'p0', collectionId: 'v320'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32632']],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p0', collectionId: 'v330'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32633']],
                        },
                    ],
                },
                v320: {
                    items: [
                        {
                            type: 'layer',
                            name: 'Red',
                            id: {providerId: 'data0', layerId: 'red32-0'},
                            description: '',
                            properties: [
                                ['edv:presetKey', 'red_band'],
                                ['edv:order', '10'],
                            ],
                        },
                    ],
                },
                v330: {
                    items: [
                        {
                            type: 'layer',
                            name: 'Red',
                            id: {providerId: 'data0', layerId: 'red33-0'},
                            description: '',
                            properties: [
                                ['edv:presetKey', 'red_band'],
                                ['edv:order', '10'],
                            ],
                        },
                    ],
                },
            },
            {
                root: {items: [{type: 'collection', name: 'EDV', id: {providerId: 'p1', collectionId: 'edv1'}, description: ''}]},
                edv1: {
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p1', collectionId: 'ds1'},
                            description: '',
                            properties: [['edv:dataset', 'sentinel']],
                        },
                    ],
                },
                ds1: {
                    items: [
                        {
                            type: 'collection',
                            name: 'Region 32N',
                            id: {providerId: 'p1', collectionId: 'v321'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32632']],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p1', collectionId: 'v331'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32633']],
                        },
                    ],
                },
                v321: {
                    items: [
                        {
                            type: 'layer',
                            name: 'Red',
                            id: {providerId: 'data1', layerId: 'red32-1'},
                            description: '',
                            properties: [
                                ['edv:presetKey', 'red_band'],
                                ['edv:order', '10'],
                            ],
                        },
                    ],
                },
                v331: {
                    items: [
                        {
                            type: 'layer',
                            name: 'Red',
                            id: {providerId: 'data1', layerId: 'red33-1'},
                            description: '',
                            properties: [
                                ['edv:presetKey', 'red_band'],
                                ['edv:order', '10'],
                            ],
                        },
                    ],
                },
            },
        ];
        getLayerCollectionItems.mockImplementation((_provider, collection) => {
            const key = collection === LAYER_DB_ROOT_COLLECTION_ID ? 'root' : collection;
            return Promise.resolve(deploymentListings[deployment][key] ?? {items: []});
        });

        fixture.detectChanges();
        await fixture.whenStable();
        // the selection is the subject here, so nothing is shown on its own: "None", which applies only on request
        fixture.componentInstance.show();
        fixture.detectChanges();
        expect(eodysseyLayerService.currentVariants().map((variant) => variant.key)).toEqual(['EPSG:32632', 'EPSG:32633']);
        expect(getLayerCollectionItems.mock.calls.map(([, collection]) => collection)).not.toContain('v330');
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        eodysseyLayerService.setSelectedVariant('EPSG:32633');
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32633');
        await vi.waitFor(() => expect(eodysseyLayerService.currentPresets().length).toBe(1));
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        eodysseyLayerService.setSelectedPreset('red_band');
        eodysseyLayerService.applySelectedPreset();
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'data0', layerId: 'red33-0'});

        deployment = 1;
        eodysseyLayerService.retryCatalogue();
        await fixture.whenStable();
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32633');
        // the reloaded catalogue shows the kept preset with its new ids
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'data1', layerId: 'red33-1'});
        expect(eodysseyLayerService.selectedPresetKey()).toBe('red_band');
    });

    it('ignores a stale preset response after switching variants', async () => {
        let resolve32!: (value: {items: unknown[]}) => void;
        let resolve33!: (value: {items: unknown[]}) => void;
        const layer = (
            layerId: string,
        ): {type: string; name: string; id: {providerId: string; layerId: string}; description: string; properties: string[][]} => ({
            type: 'layer',
            name: 'Red',
            id: {providerId: 'data', layerId},
            description: '',
            properties: [['edv:presetKey', 'red_band']],
        });
        getLayerCollectionItems.mockImplementation((_provider, collection) => {
            if (collection === LAYER_DB_ROOT_COLLECTION_ID) {
                return Promise.resolve({
                    items: [{type: 'collection', name: 'EDV', id: {providerId: 'p', collectionId: 'edv'}, description: ''}],
                });
            }
            if (collection === 'edv') {
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p', collectionId: 'dataset'},
                            description: '',
                            properties: [['edv:dataset', 'sentinel']],
                        },
                    ],
                });
            }
            if (collection === 'dataset') {
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Region 32N',
                            id: {providerId: 'p', collectionId: 'v32'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32632']],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p', collectionId: 'v33'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32633']],
                        },
                    ],
                });
            }
            if (collection === 'v32') {
                return new Promise((resolve) => {
                    resolve32 = resolve;
                });
            }
            if (collection === 'v33') {
                return new Promise((resolve) => {
                    resolve33 = resolve;
                });
            }
            return Promise.resolve({items: []});
        });

        fixture.detectChanges();
        await fixture.whenStable();
        // the selection is the subject here, so nothing is shown on its own: "None", which applies only on request
        fixture.componentInstance.show();
        fixture.detectChanges();
        eodysseyLayerService.setSelectedVariant('EPSG:32633');
        fixture.detectChanges();
        resolve33({items: [layer('red33')]});
        await vi.waitFor(() => expect(eodysseyLayerService.currentPresets().length).toBe(1));
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        eodysseyLayerService.setSelectedPreset('red_band');
        eodysseyLayerService.applySelectedPreset();
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'data', layerId: 'red33'});
        resolve32({items: [layer('red32')]});
        await Promise.resolve();
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'data', layerId: 'red33'});
    });

    it('shows a variant loading error and retries the selected variant', async () => {
        let variant33Attempts = 0;
        const makeLayer = (
            layerId: string,
        ): {type: string; name: string; id: {providerId: string; layerId: string}; description: string; properties: string[][]} => ({
            type: 'layer',
            name: 'Red',
            id: {providerId: 'data', layerId},
            description: '',
            properties: [['edv:presetKey', 'red_band']],
        });
        getLayerCollectionItems.mockImplementation((_provider, collection) => {
            if (collection === LAYER_DB_ROOT_COLLECTION_ID) {
                return Promise.resolve({
                    items: [{type: 'collection', name: 'EDV', id: {providerId: 'p', collectionId: 'edv'}, description: ''}],
                });
            }
            if (collection === 'edv') {
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p', collectionId: 'dataset'},
                            description: '',
                            properties: [['edv:dataset', 'sentinel']],
                        },
                    ],
                });
            }
            if (collection === 'dataset') {
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Region 32N',
                            id: {providerId: 'p', collectionId: 'v32'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32632']],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p', collectionId: 'v33'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32633']],
                        },
                    ],
                });
            }
            if (collection === 'v32') {
                return Promise.resolve({items: [makeLayer('red32')]});
            }
            if (collection === 'v33') {
                variant33Attempts += 1;
                return variant33Attempts === 1
                    ? Promise.reject(new Error('variant unavailable'))
                    : Promise.resolve({items: [makeLayer('red33')]});
            }
            return Promise.resolve({items: []});
        });

        fixture.detectChanges();
        await fixture.whenStable();
        // the selection is the subject here, so nothing is shown on its own: "None", which applies only on request
        fixture.componentInstance.show();
        fixture.detectChanges();
        eodysseyLayerService.setSelectedVariant('EPSG:32633');
        fixture.detectChanges();
        await vi.waitFor(() => expect(eodysseyLayerService.variantError()).toBe('variant unavailable'));
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        eodysseyLayerService.retryVariant();
        await vi.waitFor(() => expect(eodysseyLayerService.currentPresets().length).toBe(1));
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        eodysseyLayerService.setSelectedPreset('red_band');
        eodysseyLayerService.applySelectedPreset();
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'data', layerId: 'red33'});
        expect(eodysseyLayerService.variantError()).toBeUndefined();
    });
    it('reconciles the selected preset when switching to a cached variant', async () => {
        const layer = (key: string, layerId: string): unknown => ({
            type: 'layer',
            name: key,
            id: {providerId: 'data', layerId},
            description: '',
            properties: [
                ['edv:presetKey', key],
                ['edv:order', key === 'red_band' ? '10' : '20'],
            ],
        });
        getLayerCollectionItems.mockImplementation((_provider, collection) => {
            if (collection === LAYER_DB_ROOT_COLLECTION_ID)
                return Promise.resolve({
                    items: [{type: 'collection', name: 'EDV', id: {providerId: 'p', collectionId: 'edv'}, description: ''}],
                });
            if (collection === 'edv')
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p', collectionId: 'dataset'},
                            description: '',
                            properties: [['edv:dataset', 'sentinel']],
                        },
                    ],
                });
            if (collection === 'dataset')
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Region 32N',
                            id: {providerId: 'p', collectionId: 'v32'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32632']],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p', collectionId: 'v33'},
                            description: '',
                            properties: [['edv:crs', 'EPSG:32633']],
                        },
                    ],
                });
            if (collection === 'v32') return Promise.resolve({items: [layer('red_band', 'red32'), layer('ndvi', 'ndvi32')]});
            if (collection === 'v33') return Promise.resolve({items: [layer('red_band', 'red33')]});
            return Promise.resolve({items: []});
        });

        fixture.detectChanges();
        await fixture.whenStable();
        // the selection is the subject here, so nothing is shown on its own: "None", which applies only on request
        fixture.componentInstance.show();
        fixture.detectChanges();
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        eodysseyLayerService.setSelectedPreset('ndvi');
        expect(eodysseyLayerService.canApplyPreset()).toBe(true);
        eodysseyLayerService.applySelectedPreset();
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe('ndvi32');
        const appliedLayer = eodysseyLayerService.mapTileLayer();
        eodysseyLayerService.setSelectedVariant('EPSG:32633');
        fixture.detectChanges();
        expect(eodysseyLayerService.mapTileLayer()).toBe(appliedLayer);
        await vi.waitFor(() => expect(eodysseyLayerService.currentPresets().length).toBe(1));
        eodysseyLayerService.setSelectedPreset('red_band');
        eodysseyLayerService.applySelectedPreset();
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe('red33');
        eodysseyLayerService.setSelectedVariant('EPSG:32632');
        fixture.detectChanges();
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe('red33');
        eodysseyLayerService.setSelectedVariant('EPSG:32633');
        fixture.detectChanges();
        expect(eodysseyLayerService.selectedPresetKey()).toBe('red_band');
        expect(eodysseyLayerService.canApplyPreset()).toBe(false);

        // A cached region allows choosing a preset before effects run.
        eodysseyLayerService.setSelectedVariant('EPSG:32632');
        expect(eodysseyLayerService.selectedPresetKey()).toBe('red_band');
        eodysseyLayerService.setSelectedPreset('ndvi');
        expect(eodysseyLayerService.selectedPresetIndex()).toBe(1);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedPresetKey()).toBe('ndvi');
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe('red33');
    });

    it('keeps an in-flight variant load alive when retrying another variant', async () => {
        const collection = (id: string, name: string, properties: string[][] = []): unknown => ({
            type: 'collection',
            name,
            id: {providerId: 'provider', collectionId: id},
            description: '',
            properties,
        });
        const layer = (id: string): unknown => ({
            type: 'layer',
            name: 'Red',
            id: {providerId: 'data', layerId: id},
            description: '',
            properties: [['edv:presetKey', 'red']],
        });
        const pages: Record<string, {items: unknown[]}> = {
            [LAYER_DB_ROOT_COLLECTION_ID]: {items: [collection('edv', 'EDV')]},
            edv: {
                items: [collection('dataset', 'Sentinel', [['edv:dataset', 'sentinel']])],
            },
            dataset: {
                items: [collection('a', 'A', [['edv:crs', 'EPSG:32632']]), collection('b', 'B', [['edv:crs', 'EPSG:32633']])],
            },
        };
        let resolveA!: (value: {items: unknown[]}) => void;
        let bAttempts = 0;
        getLayerCollectionItems.mockImplementation((_provider, collectionId) => {
            if (collectionId === 'a')
                return new Promise((resolve) => {
                    resolveA = resolve;
                });
            if (collectionId === 'b') {
                bAttempts += 1;
                return bAttempts === 1 ? Promise.reject(new Error('B unavailable')) : Promise.resolve({items: [layer('red-b')]});
            }
            return Promise.resolve(pages[collectionId] ?? {items: []});
        });

        fixture.detectChanges();
        await fixture.whenStable();
        // the selection is the subject here, so nothing is shown on its own: "None", which applies only on request
        fixture.componentInstance.show();
        fixture.detectChanges();
        eodysseyLayerService.setSelectedVariant('EPSG:32633');
        fixture.detectChanges();
        await vi.waitFor(() => expect(eodysseyLayerService.variantError()).toBe('B unavailable'));
        eodysseyLayerService.retryVariant();
        await vi.waitFor(() => expect(eodysseyLayerService.currentPresets().length).toBe(1));
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        eodysseyLayerService.setSelectedPreset('red');
        eodysseyLayerService.applySelectedPreset();
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe('red-b');
        eodysseyLayerService.setSelectedVariant('EPSG:32632');
        fixture.detectChanges();
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe('red-b');
        resolveA({items: [layer('red-a')]});
        await vi.waitFor(() => expect(eodysseyLayerService.currentPresets().length).toBe(1));
        expect(eodysseyLayerService.mapTileLayer()?.layerId).toBe('red-b');
    });
});
