import {NO_ERRORS_SCHEMA, signal} from '@angular/core';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatIconRegistry} from '@angular/material/icon';
import {MatListModule} from '@angular/material/list';
import {MatMenuModule} from '@angular/material/menu';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {By, DomSanitizer} from '@angular/platform-browser';
import {provideRouter} from '@angular/router';
import {BehaviorSubject, of} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {SpatialReference, Time, UserService} from '@geoengine/common';
import {
    CoreModule,
    DEFAULT_CORE_CONFIG,
    MapContainerComponent,
    MapService,
    ProjectService,
    SpatialReferenceService,
    ViewportSize,
} from '@geoengine/core';
import {AppConfig} from '../app-config.service';
import {MainComponent} from './main.component';
import {EOdysseyLayerService} from '../layers/layers.service';
import type {ViewportPixelBudget} from '../layers/viewport-pixel-budget';

const pixelModel: ViewportPixelBudget = {
    tileMatrixSet: 'CustomWebMercator',
    crs: 'EPSG:3857',
    maxSourcePixels: 80_000_000,
    maxStacSearches: 10_000,
    searchGrids: [
        {
            datasetId: 'one',
            projection: 'EPSG:3857',
            projString: 'EPSG:3857',
            extent: [0, -51200000, 51200000, 0],
            cellSize: 512000,
            columns: 100,
            rows: 100,
        },
    ],
    matrices: [
        {
            id: '0',
            cellSize: 4000,
            tileWidth: 128,
            tileHeight: 128,
            matrixWidth: 100,
            matrixHeight: 100,
            sourcePixels: 8_000_000,
            pointOfOrigin: [0, 0],
        },
    ],
};

// Keep the real toolbar and bindings; the map and its HTTP requests are tested in core.
describe('EOdyssey debug reprojection', () => {
    let fixture: ComponentFixture<MainComponent>;
    const pixelBudget = signal<ViewportPixelBudget | undefined>(undefined);
    const mapCenter = signal([0, 0]);
    let viewport: BehaviorSubject<ViewportSize>;
    let mapWidth: number;
    let mapHeight: number;
    let resizeCallbacks: Array<() => void>;
    let mapTarget: HTMLElement;
    const mapTargets = signal<Array<{nativeElement: HTMLElement}>>([]);
    const disconnect = vi.fn();

    beforeEach(async () => {
        pixelBudget.set(undefined);
        mapCenter.set([0, 0]);
        viewport = new BehaviorSubject<ViewportSize>({extent: [0, 0, 1, 1], resolution: 1});
        mapWidth = mapHeight = 1000;
        resizeCallbacks = [];
        disconnect.mockClear();
        vi.stubGlobal(
            'ResizeObserver',
            class {
                constructor(callback: () => void) {
                    resizeCallbacks.push(callback);
                }
                observe(): void {
                    /* No layout observer is needed for this toolbar test. */
                }
                disconnect(): void {
                    disconnect();
                }
            },
        );
        await TestBed.configureTestingModule({
            imports: [MainComponent],
            providers: [
                provideRouter([]),
                {provide: AppConfig, useValue: DEFAULT_CORE_CONFIG},
                {
                    provide: MapService,
                    useValue: {
                        registerMapComponent: vi.fn(),
                        zoomTo: vi.fn(),
                        view: (): unknown => ({getCenter: mapCenter}),
                        getViewportSizeStream: (): unknown => viewport.asObservable(),
                    },
                },
                {
                    provide: ProjectService,
                    useValue: {
                        getSpatialReferenceStream: vi.fn().mockReturnValue(of(new SpatialReference('EPSG:3857'))),
                        getSpatialReferenceOnce: vi.fn().mockReturnValue(of(new SpatialReference('EPSG:3857'))),
                        getTimeStream: vi.fn().mockReturnValue(of(new Time(new Date('2026-04-01T00:00:00Z')))),
                    },
                },
                {
                    provide: UserService,
                    useValue: {
                        getSessionTokenStream: vi.fn().mockReturnValue(of('session')),
                        isGuestUserStream: vi.fn().mockReturnValue(of(true)),
                    },
                },
                {provide: SpatialReferenceService, useValue: {reprojectExtent: vi.fn()}},
                {
                    provide: EOdysseyLayerService,
                    useValue: {
                        mapTileLayer: vi.fn().mockReturnValue({dataConnectorId: 'data', layerId: 'layer'}),
                        catalogueLoading: vi.fn().mockReturnValue(false),
                        appliedMinTileMatrixIndex: vi.fn().mockReturnValue(4),
                        appliedViewportPixelBudget: pixelBudget,
                        mapCenter,
                    },
                },
            ],
        })
            .overrideComponent(MainComponent, {
                remove: {imports: [CoreModule, MapContainerComponent]},
                add: {imports: [MatButtonToggleModule, MatMenuModule, MatListModule, MatProgressBarModule], schemas: [NO_ERRORS_SCHEMA]},
            })
            .compileComponents();
        TestBed.inject(MatIconRegistry).addSvgIconLiteral(
            'cogs',
            TestBed.inject(DomSanitizer).bypassSecurityTrustHtml('<svg xmlns="http://www.w3.org/2000/svg"></svg>'),
        );
        fixture = TestBed.createComponent(MainComponent);
        mapTarget = document.createElement('div');
        Object.defineProperties(mapTarget, {
            clientWidth: {get: (): number => mapWidth},
            clientHeight: {get: (): number => mapHeight},
        });
        mapTargets.set([{nativeElement: mapTarget}]);
        vi.spyOn(fixture.componentInstance, 'mapComponent').mockReturnValue({
            mapContainers: mapTargets,
        } as unknown as MapContainerComponent);
    });

    afterEach(() => {
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
    });

    function tms(): unknown {
        return fixture.debugElement.query(By.css('geoengine-ol-ogc-api-map-tile-layer')).properties['tmsId'];
    }

    it('computes the viewport cutoff on Apply and resize and passes it to the existing guard', () => {
        mapTargets.set([]);
        fixture.detectChanges();
        expect(fixture.componentInstance.maxViewResolution()).toBeUndefined();
        pixelBudget.set(pixelModel);
        fixture.detectChanges();
        expect(fixture.componentInstance.maxViewResolution()).toBe(0);
        // Projection/layout initialization may supply the rendered map after the main view.
        mapTargets.set([{nativeElement: mapTarget}]);
        fixture.detectChanges();
        expect(fixture.componentInstance.maxViewResolution()).toBeCloseTo(1024, 8);
        expect(fixture.debugElement.query(By.css('geoengine-ol-ogc-api-map-tile-layer')).properties['maxViewResolution']).toBeCloseTo(
            1024,
            8,
        );
        mapWidth = mapHeight = 2000;
        for (const callback of resizeCallbacks) callback();
        fixture.detectChanges();
        expect(fixture.componentInstance.maxViewResolution()).toBeCloseTo(512, 8);
        fixture.destroy();
        expect(disconnect).toHaveBeenCalled();
    });

    it('recomputes the search-limited cutoff on pan without applying the preset again', () => {
        pixelBudget.set({
            tileMatrixSet: 'CustomWebMercator',
            crs: 'EPSG:3857',
            maxSourcePixels: 1_000_000,
            maxStacSearches: 36,
            matrices: [
                {
                    id: '0',
                    cellSize: 4,
                    tileWidth: 128,
                    tileHeight: 128,
                    matrixWidth: 8,
                    matrixHeight: 8,
                    pointOfOrigin: [-2048, 2048],
                    sourcePixels: 1,
                },
            ],
            searchGrids: [
                {
                    datasetId: 'one',
                    projection: 'EPSG:3857',
                    projString: 'EPSG:3857',
                    extent: [-2048, -2048, 2048, 2048],
                    cellSize: 256,
                    columns: 16,
                    rows: 16,
                },
            ],
        });
        fixture.detectChanges();
        const initial = fixture.componentInstance.maxViewResolution()!;
        expect(initial).toBeGreaterThan(0);
        mapCenter.set([1800, 1800]);
        fixture.detectChanges();
        expect(fixture.componentInstance.maxViewResolution()).toBeGreaterThan(initial);
        // The temporary native-matrix debug path does not match this cost model.
        fixture.componentRef.setInput('debug', 'true');
        fixture.componentInstance.frontendReprojection.set(true);
        fixture.detectChanges();
        expect(fixture.componentInstance.maxViewResolution()).toBe(0);
    });

    it('shows estimated pixels and searches beside their limits and updates on zoom', () => {
        pixelBudget.set(pixelModel);
        viewport.next({extent: [0, 0, 1, 1], resolution: 2000});
        fixture.componentInstance.zoomRequired.set(true);
        fixture.detectChanges();
        const prompt = (): string => (fixture.nativeElement as HTMLElement).querySelector('.zoom-prompt')!.textContent.replace(/\s+/g, ' ');
        expect(prompt()).toContain('Pixel limit exceeded.');
        expect(prompt()).toContain('Estimated pixels: 200,000,000 / 80,000,000 limit (exceeded)');
        expect(prompt()).toContain('STAC searches: 9 / 10,000 limit (within limit)');
        viewport.next({extent: [0, 0, 1, 1], resolution: 1500});
        fixture.detectChanges();
        expect(prompt()).toContain('Estimated pixels: 128,000,000 / 80,000,000 limit');
        viewport.next({extent: [0, 0, 1, 1], resolution: 500});
        fixture.detectChanges();
        expect(prompt()).toContain('Estimated pixels: 32,000,000 / 80,000,000 limit (within limit)');
        fixture.componentInstance.zoomRequired.set(false);
        fixture.detectChanges();
        expect((fixture.nativeElement as HTMLElement).querySelector('.zoom-prompt')).toBeNull();
    });

    it('identifies the STAC limit and updates search counts on pan', () => {
        pixelBudget.set({...pixelModel, maxStacSearches: 1, matrices: [{...pixelModel.matrices[0], sourcePixels: 1}]});
        viewport.next({extent: [0, 0, 1, 1], resolution: 2000});
        fixture.componentInstance.zoomRequired.set(true);
        fixture.detectChanges();
        expect(fixture.componentInstance.zoomDiagnostics()?.reason).toBe('STAC search limit exceeded.');
        expect(fixture.componentInstance.zoomDiagnostics()?.current).toEqual({sourcePixels: 25, stacSearches: 9});
        mapCenter.set([2_000_000, -2_000_000]);
        fixture.detectChanges();
        expect(fixture.componentInstance.zoomDiagnostics()?.current?.stacSearches).toBeGreaterThan(9);
        const text = (fixture.nativeElement as HTMLElement).querySelector('.zoom-prompt')!.textContent.replace(/\s+/g, ' ');
        expect(text).toContain('/ 1 limit (exceeded)');
    });

    it('explains when a cheaper coarser view is blocked by an unsafe finer view', () => {
        pixelBudget.set({
            ...pixelModel,
            maxSourcePixels: 10,
            matrices: [4, 2, 1].map((cellSize, index) => ({
                ...pixelModel.matrices[0],
                id: String(index),
                cellSize,
                matrixWidth: 1,
                matrixHeight: 1,
                sourcePixels: index === 1 ? 100 : 1,
            })),
        });
        viewport.next({extent: [0, 0, 1, 1], resolution: 4});
        fixture.componentInstance.zoomRequired.set(true);
        fixture.detectChanges();
        expect(fixture.componentInstance.zoomDiagnostics()?.reason).toBe('Pixel limit exceeded at the zoom cutoff.');
        expect(fixture.componentInstance.zoomDiagnostics()?.current?.sourcePixels).toBe(1);
        expect(fixture.componentInstance.zoomDiagnostics()?.blocking?.sourcePixels).toBe(100);
        const text = (fixture.nativeElement as HTMLElement).querySelector('.zoom-prompt')!.textContent.replace(/\s+/g, ' ');
        expect(text).toContain('Estimated pixels: 1 / 10 limit (within limit)');
        expect(text).toContain('Blocking view: pixels: 100 / 10 limit');
    });

    function selectFrontend(): void {
        const button = (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>('mat-button-toggle:last-child button');
        expect(button).not.toBeNull();
        button!.click();
        fixture.detectChanges();
    }

    it('keeps backend reprojection and hides the controls without debug', () => {
        fixture.detectChanges();
        expect((fixture.nativeElement as HTMLElement).querySelector('mat-button-toggle-group')).toBeNull();
        expect(tms()).toBe('CustomWebMercator');
    });

    it('switches between backend and frontend without changing the map projection, layer or time', () => {
        fixture.componentRef.setInput('debug', 'true');
        fixture.detectChanges();
        expect(tms()).toBe('CustomWebMercator');
        const layer = fixture.componentInstance.mapTileLayer();
        const time = fixture.componentInstance.currentTime();

        selectFrontend();
        expect(tms()).toBe('Custom');
        const layerProperties = fixture.debugElement.query(By.css('geoengine-ol-ogc-api-map-tile-layer')).properties;
        expect(layerProperties['minTileMatrixIndex']).toBe(4);
        expect(layerProperties['minTileMatrixSetId']).toBe('CustomWebMercator');
        expect(fixture.componentInstance.spatialReference()?.srsString).toBe('EPSG:3857');
        expect(fixture.componentInstance.mapTileLayer()).toBe(layer);
        expect(fixture.componentInstance.currentTime()).toBe(time);

        (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>('mat-button-toggle:first-child button')!.click();
        fixture.detectChanges();
        expect(tms()).toBe('CustomWebMercator');
    });

    it('returns to backend reprojection when debug is removed or false', () => {
        fixture.componentRef.setInput('debug', '');
        fixture.detectChanges();
        selectFrontend();
        expect(tms()).toBe('Custom');
        fixture.componentRef.setInput('debug', undefined);
        fixture.detectChanges();
        expect(tms()).toBe('CustomWebMercator');
        expect((fixture.nativeElement as HTMLElement).querySelector('mat-button-toggle-group')).toBeNull();
        fixture.componentRef.setInput('debug', 'false');
        fixture.detectChanges();
        expect(tms()).toBe('CustomWebMercator');
    });
});
