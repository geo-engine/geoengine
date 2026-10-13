import {beforeEach, describe, expect, it, vi} from 'vitest';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {Observable, of} from 'rxjs';
import {provideNativeDateAdapter} from '@angular/material/core';
import {MapService, ProjectService} from '@geoengine/core';
import type {CollectionItem} from '@geoengine/api-client';
import {LAYER_DB_ROOT_COLLECTION_ID, LayersService, Time, TimeStepDuration} from '@geoengine/common';
import {LayersComponent} from './layers.component';
import {EOdysseyLayerService} from './layers.service';
import View from 'ol/View';

describe('LayersComponent', () => {
    let fixture: ComponentFixture<LayersComponent>;
    let eodysseyLayerService: EOdysseyLayerService;
    const getLayerCollectionItems =
        vi.fn<(_provider: string, collection: string, offset?: number, limit?: number) => Promise<{items: unknown[]}>>();
    const getLayer = vi.fn();
    const registerAndGetLayerWorkflowId = vi.fn();
    const getWorkflowIdMetadata = vi.fn();
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
        registerAndGetLayerWorkflowId.mockReset().mockResolvedValue('workflow-id');
        getWorkflowIdMetadata.mockReset();
        mapView = new View({projection: 'EPSG:4326'});
        await TestBed.configureTestingModule({
            imports: [LayersComponent],
            providers: [
                provideNativeDateAdapter(),
                {
                    provide: LayersService,
                    useValue: {getLayerCollectionItems, getLayer, registerAndGetLayerWorkflowId, getWorkflowIdMetadata},
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
        expect(element.querySelector('.data-sources')?.getAttribute('aria-busy')).toBe('true');
        expect(element.querySelector('.visualization-presets')?.getAttribute('aria-busy')).toBe('true');
        resolveRoot(listings[LAYER_DB_ROOT_COLLECTION_ID]);
        await fixture.whenStable();
        fixture.detectChanges();
        expect(element.querySelectorAll('mat-spinner').length).toBe(0);
        expect(element.textContent).toContain('Sentinel');
    });

    it('loads later coverage and preset pages and applies a map-center choice only on Apply', async () => {
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
        await vi.waitFor(() => expect(eodysseyLayerService.currentPresets()).toHaveLength(25));
        fixture.detectChanges();
        expect(eodysseyLayerService.currentVariants()).toHaveLength(21);
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32755');
        expect(eodysseyLayerService.sortedVariants()[0].name).toBe('Region 01N');
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        expect(setTime).not.toHaveBeenCalled();

        eodysseyLayerService.setSelectedPreset('preset-1');
        fixture.componentInstance.applySelectedPreset();
        const appliedLayer = eodysseyLayerService.mapTileLayer();
        const timeCalls = setTime.mock.calls.length;
        expect(appliedLayer).toEqual({dataConnectorId: 'provider', layerId: 'preset-1'});

        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        expect(eodysseyLayerService.selectedVariant()?.key).toBe('EPSG:32755');
        const useCenterButton = [...(fixture.nativeElement as HTMLElement).querySelectorAll('button')].find((button) =>
            button.textContent?.includes('Select at map center'),
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
        expect(element.querySelector('.data-sources')?.getAttribute('aria-busy')).toBe('true');
        await fixture.whenStable();
        fixture.detectChanges();
        expect(element.textContent).not.toContain('No data sources are available');
        expect(element.textContent).toContain('Sentinel');
        expect(eodysseyLayerService.dataSources()).toHaveLength(1);
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
    });

    it('selects the default preset and requires explicit apply, with the button tracking changes', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        const button = [...element.querySelectorAll('button')].find((candidate) => candidate.textContent?.includes('Apply visualization'))!;
        expect(eodysseyLayerService.dataSources().map((source) => source.key)).toEqual(['sentinel']);
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
        expect(eodysseyLayerService.selectedPresetKey()).toBe('Default');
        expect(button.disabled).toBe(false);
        expect(setTime).not.toHaveBeenCalled();
        expect(setTimeStepDuration).not.toHaveBeenCalled();

        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[0].key);
        fixture.detectChanges();
        expect(button.disabled).toBe(false);
        button.click();
        fixture.detectChanges();
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        expect(button.disabled).toBe(true);
        await fixture.whenStable();
        expect(setTime).toHaveBeenCalledWith(new Time(new Date(1775001600000)));
        expect(setTimeStepDuration).toHaveBeenCalledWith({durationAmount: 1, durationUnit: 'day'});

        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[1].key);
        fixture.detectChanges();
        expect(button.disabled).toBe(false);
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        button.click();
        fixture.detectChanges();
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'alternate'});
        expect(button.disabled).toBe(true);
        await fixture.whenStable();
        expect(setTime).toHaveBeenCalledTimes(1);
    });

    it('defaults to True Color Image by catalogue order and preserves a manual choice', async () => {
        const preset = (key: string, name: string, order: number): unknown => ({
            type: 'layer',
            name,
            id: {providerId: 'provider', layerId: key},
            description: '',
            properties: [
                ['edv:presetKey', key],
                ['edv:order', String(order)],
            ],
        });
        getLayerCollectionItems.mockImplementation((_provider, collection) =>
            Promise.resolve(
                collection === 'region'
                    ? {items: [preset('ndvi', 'NDVI', 30), preset('true_color_tci', 'True Color Image', 10)]}
                    : (listings[collection] ?? {items: []}),
            ),
        );
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedPreset()?.displayName).toBe('True Color Image');
        expect(eodysseyLayerService.selectedPresetIndex()).toBe(0);
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();

        eodysseyLayerService.setSelectedPreset('ndvi');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.selectedPresetKey()).toBe('ndvi');
        expect(eodysseyLayerService.mapTileLayer()).toBeUndefined();
    });

    it('keeps the applied layer and time while selecting another source until apply is clicked', async () => {
        const pages: Record<string, {items: unknown[]}> = {
            ...listings,
            edv: {
                items: [
                    ...listings.edv.items,
                    {
                        type: 'collection',
                        name: 'Z Other source',
                        id: {providerId: 'provider', collectionId: 'otherDataset'},
                        description: '',
                        properties: [
                            ['edv:dataset', 'other'],
                            ['edv:defaultTime', '1775088000000'],
                            ['edv:timeStep', '{"step":2,"granularity":"days"}'],
                        ],
                    },
                ],
            },
            otherDataset: {
                items: [
                    {
                        type: 'collection',
                        name: 'Global',
                        description: '',
                        id: {providerId: 'provider', collectionId: 'otherRegion'},
                        properties: [['edv:crs', 'EPSG:4326']],
                    },
                ],
            },
            otherRegion: {
                items: [
                    {
                        type: 'layer',
                        name: 'Other visualization',
                        id: {providerId: 'provider', layerId: 'other'},
                        description: '',
                        properties: [['edv:presetKey', 'other']],
                    },
                ],
            },
        };
        getLayerCollectionItems.mockImplementation((_provider, collection) => Promise.resolve(pages[collection] ?? {items: []}));
        fixture.detectChanges();
        await fixture.whenStable();
        const component = fixture.componentInstance;
        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[0].key);
        component.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        const appliedLayer = eodysseyLayerService.mapTileLayer();
        const appliedSource = eodysseyLayerService.appliedDataSource();
        expect(appliedLayer).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        expect(setTime).toHaveBeenCalledWith(new Time(new Date(1775001600000)));
        setTime.mockClear();
        setTimeStepDuration.mockClear();
        getLayer.mockClear();

        eodysseyLayerService.setSelectedDataSource('other');
        fixture.detectChanges();
        await fixture.whenStable();
        await vi.waitFor(() => expect(eodysseyLayerService.currentPresets()).toHaveLength(1));
        expect(eodysseyLayerService.selectedDataSource()?.key).toBe('other');
        expect(eodysseyLayerService.mapTileLayer()).toBe(appliedLayer);
        expect(eodysseyLayerService.appliedDataSource()).toBe(appliedSource);
        expect(eodysseyLayerService.selectedPresetKey()).toBe('other');
        expect(eodysseyLayerService.canApplyPreset()).toBe(true);
        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[0].key);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.canApplyPreset()).toBe(true);
        expect(eodysseyLayerService.mapTileLayer()).toBe(appliedLayer);
        expect(setTime).not.toHaveBeenCalled();
        expect(setTimeStepDuration).not.toHaveBeenCalled();
        expect(getLayer).not.toHaveBeenCalled();

        component.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'other'});
        expect(eodysseyLayerService.appliedDataSource()?.key).toBe('other');
        expect(setTime).toHaveBeenCalledWith(new Time(new Date(1775088000000)));
        expect(setTimeStepDuration).toHaveBeenCalledWith({durationAmount: 2, durationUnit: 'days'});
        expect(getLayer).toHaveBeenCalledWith('provider', 'other');

        setTime.mockClear();
        component.autoSelectTime.set(false);
        fixture.detectChanges();
        await fixture.whenStable();
        eodysseyLayerService.setSelectedDataSource('sentinel');
        fixture.detectChanges();
        await fixture.whenStable();
        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[0].key);
        component.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        expect(setTime).not.toHaveBeenCalled();
    });

    it('preserves the applied layer and time when returning to the layers panel', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[0].key);
        fixture.componentInstance.applySelectedPreset();
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

    it('loads the legend only after applying a preset and handles a failed request', async () => {
        getLayer.mockRejectedValue(new Error('Layer unavailable'));
        fixture.detectChanges();
        await fixture.whenStable();
        const component = fixture.componentInstance;
        const element = fixture.nativeElement as HTMLElement;
        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[0].key);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(getLayer).not.toHaveBeenCalled();
        expect(element.querySelector('.legend')).toBeNull();

        component.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(getLayer).toHaveBeenCalledWith('provider', 'vv');
        expect(element.querySelector('.legend-error')?.textContent).toContain('Failed to load legend');
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});

        eodysseyLayerService.setSelectedPreset(eodysseyLayerService.currentPresets()[1].key);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(getLayer).toHaveBeenCalledTimes(1);
        component.applySelectedPreset();
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

    it('reports a missing source key without traversing another folder level', async () => {
        getLayerCollectionItems.mockImplementation((_provider, collection) =>
            Promise.resolve(
                collection === 'edv'
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
        expect(eodysseyLayerService.mapTileLayer()).toEqual({dataConnectorId: 'data0', layerId: 'red33-0'});
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
