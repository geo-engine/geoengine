import {beforeEach, describe, expect, it, vi} from 'vitest';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {BehaviorSubject, Observable, of} from 'rxjs';
import {provideNativeDateAdapter} from '@angular/material/core';
import {MapService, ProjectService} from '@geoengine/core';
import type {CollectionItem} from '@geoengine/api-client';
import {LAYER_DB_ROOT_COLLECTION_ID, LayersService, Time, TimeStepDuration} from '@geoengine/common';
import {LayersComponent} from './layers.component';
import {EdvLayersService} from './layers.service';
import {AppConfig} from '../app-config.service';
import View from 'ol/View';

describe('LayersComponent', () => {
    let fixture: ComponentFixture<LayersComponent>;
    let edvLayersService: EdvLayersService;
    const getLayerCollectionItems =
        vi.fn<(_provider: string, collection: string, offset?: number, limit?: number) => Promise<{items: unknown[]}>>();
    const getLayer = vi.fn();
    const registerAndGetLayerWorkflowId = vi.fn();
    const getWorkflowIdMetadata = vi.fn();
    const setTime = vi.fn().mockResolvedValue(undefined);
    const setTimeStepDuration = vi.fn();
    let mapView: View;
    let mapViews: BehaviorSubject<View>;
    const listings: Record<string, {items: unknown[]}> = {
        [LAYER_DB_ROOT_COLLECTION_ID]: {
            items: [{type: 'collection', name: 'EDV', id: {providerId: 'provider', collectionId: 'edv'}, description: ''}],
        },
        edv: {
            items: [
                {
                    type: 'collection',
                    name: 'adHoc',
                    id: {providerId: 'provider', collectionId: 'adhoc'},
                    description: '',
                    properties: [['edv:category', 'adHoc']],
                },
            ],
        },
        adhoc: {
            items: [
                {
                    type: 'collection',
                    name: 'Sentinel',
                    id: {providerId: 'provider', collectionId: 'dataset'},
                    description: '',
                    properties: [
                        ['edv:type', 'dataset'],
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
                    type: 'layer',
                    name: 'Default',
                    id: {providerId: 'provider', layerId: 'vv'},
                    description: '',
                    properties: [
                        ['edv:type', 'preset'],
                        ['edv:preset', 'Default'],
                        ['edv:order', '10'],
                    ],
                },
                {
                    type: 'layer',
                    name: 'Alternate',
                    id: {providerId: 'provider', layerId: 'alternate'},
                    description: '',
                    properties: [
                        ['edv:type', 'preset'],
                        ['edv:preset', 'Alternate'],
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
            properties: [
                ['edv:type', 'dataset'],
                ['edv:dataset', key],
            ],
        });
        const variant = (source: string, epsg: number): CollectionItem => ({
            type: 'collection',
            name: `Region ${epsg % 100}${epsg >= 32700 ? 'S' : 'N'}`,
            description: '',
            id: {providerId: 'provider', collectionId: `${source}-${epsg}`},
            properties: [
                ['edv:type', 'variant'],
                ['edv:variant', `epsg${epsg}`],
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
            edv: listings.edv.items,
            adhoc: [dataset('sentinel', 'A Sentinel'), dataset('landsat', 'B Landsat')],
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
        mapViews = new BehaviorSubject(mapView);
        await TestBed.configureTestingModule({
            imports: [LayersComponent],
            providers: [
                provideNativeDateAdapter(),
                {
                    provide: LayersService,
                    useValue: {getLayerCollectionItems, getLayer, registerAndGetLayerWorkflowId, getWorkflowIdMetadata},
                },
                {provide: AppConfig, useValue: {EDV: {CATEGORY: 'adHoc'}}},
                EdvLayersService,
                {
                    provide: MapService,
                    useValue: {getViewStream: (): Observable<View> => mapViews.asObservable(), getView: (): View => mapViews.value},
                },
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
        edvLayersService = TestBed.inject(EdvLayersService);
        fixture = TestBed.createComponent(LayersComponent);
    });

    it('preselects the matching coverage on source switches and preserves the initial choice while panning', async () => {
        mockCoverageCatalogue();
        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32632');

        mapView.setCenter([148.5, -35.5]);
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32632');
        edvLayersService.setSelectedDataSource('landsat');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32655');
        edvLayersService.setSelectedDataSource('sentinel');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32755');
        expect(setTime).not.toHaveBeenCalled();
        expect(edvLayersService.mapTileLayer()).toBeUndefined();
    });

    it('waits for the replacement view center and removes the old view listener', async () => {
        mockCoverageCatalogue();
        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        await fixture.whenStable();
        const replacement = new View({projection: 'EPSG:4326'});
        mapViews.next(replacement);
        expect(edvLayersService.mapCenter()).toBeUndefined();
        mapView.setCenter([148.5, -35.5]);
        expect(edvLayersService.mapCenter()).toBeUndefined();
        edvLayersService.setSelectedDataSource('landsat');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32632');
        replacement.setCenter([148.5, -35.5]);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32655');
    });

    it('preserves a manual variant chosen before the map initializes', async () => {
        mockCoverageCatalogue();
        fixture.detectChanges();
        await fixture.whenStable();
        edvLayersService.setSelectedVariant('epsg32655');
        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32655');
        expect(edvLayersService.mapCenterVariantKey()).toBe('epsg32632');
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
                    ['edv:type', 'variant'],
                    ['edv:variant', `epsg${32600 + zone}`],
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
                ['edv:type', 'variant'],
                ['edv:variant', 'epsg32755'],
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
                ['edv:type', 'preset'],
                ['edv:presetKey', `preset-${index + 1}`],
                ['edv:preset', `Preset ${String(index + 1).padStart(2, '0')}`],
                ['edv:order', String(index + 1)],
            ],
        }));
        getLayerCollectionItems.mockImplementation((_provider, collection, offset = 0, limit = 20) => {
            const pages: Record<string, unknown[]> = {
                [LAYER_DB_ROOT_COLLECTION_ID]: [
                    {type: 'collection', name: 'EDV', id: {providerId: 'provider', collectionId: 'edv'}, description: ''},
                ],
                edv: [{type: 'collection', name: 'adHoc', id: {providerId: 'provider', collectionId: 'adhoc'}, description: ''}],
                adhoc: [
                    {
                        type: 'collection',
                        name: 'Sentinel',
                        id: {providerId: 'provider', collectionId: 'dataset'},
                        description: '',
                        properties: [
                            ['edv:type', 'dataset'],
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
        await vi.waitFor(() => expect(edvLayersService.currentPresets()).toHaveLength(25));
        fixture.detectChanges();
        expect(edvLayersService.currentVariants()).toHaveLength(21);
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32755');
        expect(edvLayersService.sortedVariants()[0].name).toBe('Region 01N');
        expect(edvLayersService.mapTileLayer()).toBeUndefined();
        expect(setTime).not.toHaveBeenCalled();

        edvLayersService.setSelectedPreset('preset-1');
        fixture.componentInstance.applySelectedPreset();
        const appliedLayer = edvLayersService.mapTileLayer();
        const timeCalls = setTime.mock.calls.length;
        expect(appliedLayer).toEqual({dataConnectorId: 'provider', layerId: 'preset-1'});

        mapView.setCenter([9, 50]);
        fixture.detectChanges();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32755');
        const useCenterButton = [...(fixture.nativeElement as HTMLElement).querySelectorAll('button')].find((button) =>
            button.textContent?.includes('Select at map center'),
        );
        expect(useCenterButton).toBeDefined();
        useCenterButton?.click();
        fixture.detectChanges();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32632');
        expect(edvLayersService.mapTileLayer()).toEqual(appliedLayer);
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
        fixture.componentInstance.retryCatalogue();
        fixture.detectChanges();
        expect(element.querySelectorAll('mat-spinner').length).toBe(2);
        await fixture.whenStable();
        fixture.detectChanges();
        expect(element.querySelectorAll('mat-spinner').length).toBe(0);
        expect(element.textContent).not.toContain('Catalogue unavailable');
    });

    it('requires a preset selection and explicit apply, with the button tracking changes', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        const button = [...element.querySelectorAll('button')].find((candidate) => candidate.textContent?.includes('Apply visualization'))!;
        expect(fixture.componentInstance.dataSources().map((source) => source.key)).toEqual(['sentinel']);
        expect(fixture.componentInstance.currentPresets()[0].category).toBe('adHoc');
        expect(fixture.componentInstance.mapTileLayer()).toBeUndefined();
        expect(button.disabled).toBe(true);
        expect(setTime).not.toHaveBeenCalled();
        expect(setTimeStepDuration).not.toHaveBeenCalled();

        fixture.componentInstance.selectPreset(fixture.componentInstance.currentPresets()[0]);
        fixture.detectChanges();
        expect(button.disabled).toBe(false);
        button.click();
        fixture.detectChanges();
        expect(fixture.componentInstance.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        expect(button.disabled).toBe(true);
        await fixture.whenStable();
        expect(setTime).toHaveBeenCalledWith(new Time(new Date(1775001600000)));
        expect(setTimeStepDuration).toHaveBeenCalledWith({durationAmount: 1, durationUnit: 'day'});

        fixture.componentInstance.selectPreset(fixture.componentInstance.currentPresets()[1]);
        fixture.detectChanges();
        expect(button.disabled).toBe(false);
        expect(fixture.componentInstance.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        button.click();
        fixture.detectChanges();
        expect(fixture.componentInstance.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'alternate'});
        expect(button.disabled).toBe(true);
        await fixture.whenStable();
        expect(setTime).toHaveBeenCalledTimes(1);
    });

    it('keeps the applied layer and time while selecting another source until apply is clicked', async () => {
        const pages: Record<string, {items: unknown[]}> = {
            ...listings,
            adhoc: {
                items: [
                    ...listings.adhoc.items,
                    {
                        type: 'collection',
                        name: 'Z Other source',
                        id: {providerId: 'provider', collectionId: 'otherDataset'},
                        description: '',
                        properties: [
                            ['edv:type', 'dataset'],
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
                        type: 'layer',
                        name: 'Other visualization',
                        id: {providerId: 'provider', layerId: 'other'},
                        description: '',
                        properties: [
                            ['edv:type', 'preset'],
                            ['edv:preset', 'Other'],
                        ],
                    },
                ],
            },
        };
        getLayerCollectionItems.mockImplementation((_provider, collection) => Promise.resolve(pages[collection] ?? {items: []}));
        fixture.detectChanges();
        await fixture.whenStable();
        const component = fixture.componentInstance;
        component.selectPreset(component.currentPresets()[0]);
        component.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        const appliedLayer = component.mapTileLayer();
        const appliedSource = edvLayersService.appliedDataSource();
        expect(appliedLayer).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        expect(setTime).toHaveBeenCalledWith(new Time(new Date(1775001600000)));
        setTime.mockClear();
        setTimeStepDuration.mockClear();
        getLayer.mockClear();

        component.setSelectedDataSource('other');
        fixture.detectChanges();
        await fixture.whenStable();
        expect(component.selectedDataSource()?.key).toBe('other');
        expect(component.mapTileLayer()).toBe(appliedLayer);
        expect(edvLayersService.appliedDataSource()).toBe(appliedSource);
        expect(component.selectedPreset()).toBeUndefined();
        expect(component.canApplyPreset()).toBe(false);
        component.selectPreset(component.currentPresets()[0]);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(component.canApplyPreset()).toBe(true);
        expect(component.mapTileLayer()).toBe(appliedLayer);
        expect(setTime).not.toHaveBeenCalled();
        expect(setTimeStepDuration).not.toHaveBeenCalled();
        expect(getLayer).not.toHaveBeenCalled();

        component.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(component.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'other'});
        expect(edvLayersService.appliedDataSource()?.key).toBe('other');
        expect(setTime).toHaveBeenCalledWith(new Time(new Date(1775088000000)));
        expect(setTimeStepDuration).toHaveBeenCalledWith({durationAmount: 2, durationUnit: 'days'});
        expect(getLayer).toHaveBeenCalledWith('provider', 'other');

        setTime.mockClear();
        component.autoSelectTime.set(false);
        fixture.detectChanges();
        await fixture.whenStable();
        component.setSelectedDataSource('sentinel');
        fixture.detectChanges();
        await fixture.whenStable();
        component.selectPreset(component.currentPresets()[0]);
        component.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(component.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        expect(setTime).not.toHaveBeenCalled();
    });

    it('preserves the applied layer and time when returning to the layers panel', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.componentInstance.selectPreset(fixture.componentInstance.currentPresets()[0]);
        fixture.componentInstance.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        const appliedLayer = edvLayersService.mapTileLayer();
        setTime.mockClear();
        setTimeStepDuration.mockClear();

        fixture.destroy();
        fixture = TestBed.createComponent(LayersComponent);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.mapTileLayer()).toBe(appliedLayer);
        expect(setTime).not.toHaveBeenCalled();
        expect(setTimeStepDuration).not.toHaveBeenCalled();
    });

    it('loads the legend only after applying a preset and handles a failed request', async () => {
        getLayer.mockRejectedValue(new Error('Layer unavailable'));
        fixture.detectChanges();
        await fixture.whenStable();
        const component = fixture.componentInstance;
        const element = fixture.nativeElement as HTMLElement;
        component.selectPreset(component.currentPresets()[0]);
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
        expect(component.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});

        component.selectPreset(component.currentPresets()[1]);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(getLayer).toHaveBeenCalledTimes(1);
        component.applySelectedPreset();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(getLayer).toHaveBeenLastCalledWith('provider', 'alternate');
    });

    it('loads all configured categories when debug mode is enabled', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        edvLayersService.debug.set(true);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.presetGroups().map((group) => group.category)).toEqual(['adHoc']);
    });
    it('keeps variant and preset selection when collection and layer ids change', async () => {
        let deployment = 0;
        const deploymentListings: Array<Record<string, {items: unknown[]}>> = [
            {
                root: {items: [{type: 'collection', name: 'EDV', id: {providerId: 'p0', collectionId: 'edv0'}, description: ''}]},
                edv0: {items: [{type: 'collection', name: 'adHoc', id: {providerId: 'p0', collectionId: 'cat0'}, description: ''}]},
                cat0: {
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p0', collectionId: 'ds0'},
                            description: '',
                            properties: [
                                ['edv:type', 'dataset'],
                                ['edv:dataset', 'sentinel'],
                            ],
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
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32632'],
                                ['edv:crs', 'EPSG:32632'],
                            ],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p0', collectionId: 'v330'},
                            description: '',
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32633'],
                                ['edv:crs', 'EPSG:32633'],
                            ],
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
                                ['edv:type', 'preset'],
                                ['edv:presetKey', 'red_band'],
                                ['edv:preset', 'Red Band'],
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
                                ['edv:type', 'preset'],
                                ['edv:presetKey', 'red_band'],
                                ['edv:preset', 'Red Band'],
                                ['edv:order', '10'],
                            ],
                        },
                    ],
                },
            },
            {
                root: {items: [{type: 'collection', name: 'EDV', id: {providerId: 'p1', collectionId: 'edv1'}, description: ''}]},
                edv1: {items: [{type: 'collection', name: 'adHoc', id: {providerId: 'p1', collectionId: 'cat1'}, description: ''}]},
                cat1: {
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p1', collectionId: 'ds1'},
                            description: '',
                            properties: [
                                ['edv:type', 'dataset'],
                                ['edv:dataset', 'sentinel'],
                            ],
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
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32632'],
                                ['edv:crs', 'EPSG:32632'],
                            ],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p1', collectionId: 'v331'},
                            description: '',
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32633'],
                                ['edv:crs', 'EPSG:32633'],
                            ],
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
                                ['edv:type', 'preset'],
                                ['edv:presetKey', 'red_band'],
                                ['edv:preset', 'Red Band'],
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
                                ['edv:type', 'preset'],
                                ['edv:presetKey', 'red_band'],
                                ['edv:preset', 'Red Band'],
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
        expect(edvLayersService.currentVariants().map((variant) => variant.key)).toEqual(['epsg32632', 'epsg32633']);
        expect(getLayerCollectionItems.mock.calls.map(([, collection]) => collection)).not.toContain('v330');
        expect(edvLayersService.mapTileLayer()).toBeUndefined();
        edvLayersService.setSelectedVariant('epsg32633');
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32633');
        await vi.waitFor(() => expect(edvLayersService.currentPresets().length).toBe(1));
        expect(edvLayersService.mapTileLayer()).toBeUndefined();
        edvLayersService.setSelectedPreset('red_band');
        edvLayersService.applySelectedPreset();
        expect(edvLayersService.mapTileLayer()).toEqual({dataConnectorId: 'data0', layerId: 'red33-0'});

        deployment = 1;
        edvLayersService.retryCatalogue();
        await fixture.whenStable();
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32633');
        expect(edvLayersService.mapTileLayer()).toEqual({dataConnectorId: 'data0', layerId: 'red33-0'});
        expect(edvLayersService.selectedPresetKey()).toBeUndefined();
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
            properties: [
                ['edv:type', 'preset'],
                ['edv:presetKey', 'red_band'],
                ['edv:preset', 'Red Band'],
            ],
        });
        getLayerCollectionItems.mockImplementation((_provider, collection) => {
            if (collection === LAYER_DB_ROOT_COLLECTION_ID) {
                return Promise.resolve({
                    items: [{type: 'collection', name: 'EDV', id: {providerId: 'p', collectionId: 'edv'}, description: ''}],
                });
            }
            if (collection === 'edv') {
                return Promise.resolve({
                    items: [{type: 'collection', name: 'adHoc', id: {providerId: 'p', collectionId: 'category'}, description: ''}],
                });
            }
            if (collection === 'category') {
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p', collectionId: 'dataset'},
                            description: '',
                            properties: [
                                ['edv:type', 'dataset'],
                                ['edv:dataset', 'sentinel'],
                            ],
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
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32632'],
                            ],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p', collectionId: 'v33'},
                            description: '',
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32633'],
                            ],
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
        edvLayersService.setSelectedVariant('epsg32633');
        fixture.detectChanges();
        resolve33({items: [layer('red33')]});
        await vi.waitFor(() => expect(edvLayersService.currentPresets().length).toBe(1));
        expect(edvLayersService.mapTileLayer()).toBeUndefined();
        edvLayersService.setSelectedPreset('red_band');
        edvLayersService.applySelectedPreset();
        expect(edvLayersService.mapTileLayer()).toEqual({dataConnectorId: 'data', layerId: 'red33'});
        resolve32({items: [layer('red32')]});
        await Promise.resolve();
        expect(edvLayersService.mapTileLayer()).toEqual({dataConnectorId: 'data', layerId: 'red33'});
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
            properties: [
                ['edv:type', 'preset'],
                ['edv:presetKey', 'red_band'],
                ['edv:preset', 'Red Band'],
            ],
        });
        getLayerCollectionItems.mockImplementation((_provider, collection) => {
            if (collection === LAYER_DB_ROOT_COLLECTION_ID) {
                return Promise.resolve({
                    items: [{type: 'collection', name: 'EDV', id: {providerId: 'p', collectionId: 'edv'}, description: ''}],
                });
            }
            if (collection === 'edv') {
                return Promise.resolve({
                    items: [{type: 'collection', name: 'adHoc', id: {providerId: 'p', collectionId: 'category'}, description: ''}],
                });
            }
            if (collection === 'category') {
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p', collectionId: 'dataset'},
                            description: '',
                            properties: [
                                ['edv:type', 'dataset'],
                                ['edv:dataset', 'sentinel'],
                            ],
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
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32632'],
                            ],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p', collectionId: 'v33'},
                            description: '',
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32633'],
                            ],
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
        edvLayersService.setSelectedVariant('epsg32633');
        fixture.detectChanges();
        await vi.waitFor(() => expect(edvLayersService.variantError()).toBe('variant unavailable'));
        expect(edvLayersService.mapTileLayer()).toBeUndefined();
        edvLayersService.retryVariant();
        await vi.waitFor(() => expect(edvLayersService.currentPresets().length).toBe(1));
        expect(edvLayersService.mapTileLayer()).toBeUndefined();
        edvLayersService.setSelectedPreset('red_band');
        edvLayersService.applySelectedPreset();
        expect(edvLayersService.mapTileLayer()).toEqual({dataConnectorId: 'data', layerId: 'red33'});
        expect(edvLayersService.variantError()).toBeUndefined();
    });
    it('reconciles the selected preset when switching to a cached variant', async () => {
        const layer = (key: string, layerId: string): unknown => ({
            type: 'layer',
            name: key,
            id: {providerId: 'data', layerId},
            description: '',
            properties: [
                ['edv:type', 'preset'],
                ['edv:presetKey', key],
                ['edv:preset', key],
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
                    items: [{type: 'collection', name: 'adHoc', id: {providerId: 'p', collectionId: 'category'}, description: ''}],
                });
            if (collection === 'category')
                return Promise.resolve({
                    items: [
                        {
                            type: 'collection',
                            name: 'Sentinel',
                            id: {providerId: 'p', collectionId: 'dataset'},
                            description: '',
                            properties: [
                                ['edv:type', 'dataset'],
                                ['edv:dataset', 'sentinel'],
                            ],
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
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32632'],
                            ],
                        },
                        {
                            type: 'collection',
                            name: 'Region 33N',
                            id: {providerId: 'p', collectionId: 'v33'},
                            description: '',
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32633'],
                            ],
                        },
                    ],
                });
            if (collection === 'v32') return Promise.resolve({items: [layer('red_band', 'red32'), layer('ndvi', 'ndvi32')]});
            if (collection === 'v33') return Promise.resolve({items: [layer('red_band', 'red33')]});
            return Promise.resolve({items: []});
        });

        fixture.detectChanges();
        await fixture.whenStable();
        expect(edvLayersService.mapTileLayer()).toBeUndefined();
        edvLayersService.setSelectedPreset('ndvi');
        expect(edvLayersService.canApplyPreset()).toBe(true);
        edvLayersService.applySelectedPreset();
        expect(edvLayersService.mapTileLayer()?.layerId).toBe('ndvi32');
        const appliedLayer = edvLayersService.mapTileLayer();
        edvLayersService.setSelectedVariant('epsg32633');
        fixture.detectChanges();
        expect(edvLayersService.mapTileLayer()).toBe(appliedLayer);
        await vi.waitFor(() => expect(edvLayersService.currentPresets().length).toBe(1));
        edvLayersService.setSelectedPreset('red_band');
        edvLayersService.applySelectedPreset();
        expect(edvLayersService.mapTileLayer()?.layerId).toBe('red33');
        edvLayersService.setSelectedVariant('epsg32632');
        fixture.detectChanges();
        expect(edvLayersService.mapTileLayer()?.layerId).toBe('red33');
        edvLayersService.setSelectedVariant('epsg32633');
        fixture.detectChanges();
        expect(edvLayersService.selectedPresetKey()).toBeUndefined();
        expect(edvLayersService.canApplyPreset()).toBe(false);
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
            properties: [
                ['edv:type', 'preset'],
                ['edv:presetKey', 'red'],
            ],
        });
        const pages: Record<string, {items: unknown[]}> = {
            [LAYER_DB_ROOT_COLLECTION_ID]: {items: [collection('edv', 'EDV')]},
            edv: {items: [collection('category', 'adHoc')]},
            category: {
                items: [
                    collection('dataset', 'Sentinel', [
                        ['edv:type', 'dataset'],
                        ['edv:dataset', 'sentinel'],
                    ]),
                ],
            },
            dataset: {
                items: [
                    collection('a', 'A', [
                        ['edv:type', 'variant'],
                        ['edv:variant', 'a'],
                    ]),
                    collection('b', 'B', [
                        ['edv:type', 'variant'],
                        ['edv:variant', 'b'],
                    ]),
                ],
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
        edvLayersService.setSelectedVariant('b');
        fixture.detectChanges();
        await vi.waitFor(() => expect(edvLayersService.variantError()).toBe('B unavailable'));
        edvLayersService.retryVariant();
        await vi.waitFor(() => expect(edvLayersService.currentPresets().length).toBe(1));
        expect(edvLayersService.mapTileLayer()).toBeUndefined();
        edvLayersService.setSelectedPreset('red');
        edvLayersService.applySelectedPreset();
        expect(edvLayersService.mapTileLayer()?.layerId).toBe('red-b');
        edvLayersService.setSelectedVariant('a');
        fixture.detectChanges();
        expect(edvLayersService.mapTileLayer()?.layerId).toBe('red-b');
        resolveA({items: [layer('red-a')]});
        await vi.waitFor(() => expect(edvLayersService.currentPresets().length).toBe(1));
        expect(edvLayersService.mapTileLayer()?.layerId).toBe('red-b');
    });
});
