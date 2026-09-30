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
});
