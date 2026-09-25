import {beforeEach, describe, expect, it, vi} from 'vitest';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {Observable, of} from 'rxjs';
import {provideNativeDateAdapter} from '@angular/material/core';
import {ProjectService} from '@geoengine/core';
import {LAYER_DB_ROOT_COLLECTION_ID, LayersService, Time, TimeStepDuration} from '@geoengine/common';
import {LayersComponent} from './layers.component';
import {EdvLayersService} from './layers.service';
import {AppConfig} from '../app-config.service';

describe('LayersComponent', () => {
    let fixture: ComponentFixture<LayersComponent>;
    let edvLayersService: EdvLayersService;
    const getLayerCollectionItems = vi.fn<(_provider: string, collection: string) => Promise<{items: unknown[]}>>();
    const setTime = vi.fn().mockResolvedValue(undefined);
    const setTimeStepDuration = vi.fn();
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
            ],
        },
    };

    beforeEach(async () => {
        vi.clearAllMocks();
        getLayerCollectionItems.mockImplementation((_provider, collection) => Promise.resolve(listings[collection] ?? {items: []}));
        await TestBed.configureTestingModule({
            imports: [LayersComponent],
            providers: [
                provideNativeDateAdapter(),
                {
                    provide: LayersService,
                    useValue: {getLayerCollectionItems},
                },
                {provide: AppConfig, useValue: {EDV: {CATEGORY: 'adHoc'}}},
                EdvLayersService,
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

    it('loads the configured category and returned layer id', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.dataSources().map((source) => source.key)).toEqual(['sentinel']);
        expect(fixture.componentInstance.currentPresets()[0].category).toBe('adHoc');
        expect(fixture.componentInstance.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});
        expect(setTime).toHaveBeenCalledWith(new Time(new Date(1775001600000)));
        expect(setTimeStepDuration).toHaveBeenCalledWith({durationAmount: 1, durationUnit: 'day'});
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
                            name: 'UTM 32N',
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
                            name: 'UTM 33N',
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
                            name: 'UTM 32N',
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
                            name: 'UTM 33N',
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
        edvLayersService.setSelectedVariant('epsg32633');
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32633');
        await vi.waitFor(() => expect(edvLayersService.mapTileLayer()).toEqual({dataConnectorId: 'data0', layerId: 'red33-0'}));

        deployment = 1;
        edvLayersService.retryCatalogue();
        await fixture.whenStable();
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        expect(edvLayersService.selectedVariant()?.key).toBe('epsg32633');
        await vi.waitFor(() => expect(edvLayersService.mapTileLayer()).toEqual({dataConnectorId: 'data1', layerId: 'red33-1'}));
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
                            name: 'UTM 32N',
                            id: {providerId: 'p', collectionId: 'v32'},
                            description: '',
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32632'],
                            ],
                        },
                        {
                            type: 'collection',
                            name: 'UTM 33N',
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
        await vi.waitFor(() => expect(edvLayersService.mapTileLayer()).toEqual({dataConnectorId: 'data', layerId: 'red33'}));
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
                            name: 'UTM 32N',
                            id: {providerId: 'p', collectionId: 'v32'},
                            description: '',
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32632'],
                            ],
                        },
                        {
                            type: 'collection',
                            name: 'UTM 33N',
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
        await vi.waitFor(() => expect(edvLayersService.mapTileLayer()).toEqual({dataConnectorId: 'data', layerId: 'red33'}));
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
                            name: 'UTM 32N',
                            id: {providerId: 'p', collectionId: 'v32'},
                            description: '',
                            properties: [
                                ['edv:type', 'variant'],
                                ['edv:variant', 'epsg32632'],
                            ],
                        },
                        {
                            type: 'collection',
                            name: 'UTM 33N',
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
        await vi.waitFor(() => expect(edvLayersService.mapTileLayer()?.layerId).toBe('red32'));
        edvLayersService.setSelectedPreset('ndvi');
        edvLayersService.setSelectedVariant('epsg32633');
        fixture.detectChanges();
        await vi.waitFor(() => expect(edvLayersService.mapTileLayer()?.layerId).toBe('red33'));
        edvLayersService.setSelectedVariant('epsg32632');
        fixture.detectChanges();
        edvLayersService.setSelectedPreset('ndvi');
        edvLayersService.setSelectedVariant('epsg32633');
        fixture.detectChanges();
        expect(edvLayersService.selectedPresetKey()).toBe('red_band');
        expect(edvLayersService.mapTileLayer()?.layerId).toBe('red33');
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
        await vi.waitFor(() => expect(edvLayersService.mapTileLayer()?.layerId).toBe('red-b'));
        edvLayersService.setSelectedVariant('a');
        fixture.detectChanges();
        resolveA({items: [layer('red-a')]});
        await vi.waitFor(() => expect(edvLayersService.mapTileLayer()?.layerId).toBe('red-a'));
    });
});
