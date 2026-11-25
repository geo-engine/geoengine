import {ESCAPE} from '@angular/cdk/keycodes';
import {NO_ERRORS_SCHEMA, signal, WritableSignal} from '@angular/core';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatIconRegistry} from '@angular/material/icon';
import {MatListModule} from '@angular/material/list';
import {MatMenuModule} from '@angular/material/menu';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {MatDrawer} from '@angular/material/sidenav';
import {By, DomSanitizer} from '@angular/platform-browser';
import {provideRouter} from '@angular/router';
import OlLayerVector from 'ol/layer/Vector';
import OlMap from 'ol/Map';
import View from 'ol/View';
import {of} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, Mock, vi} from 'vitest';
import {LayersService, SpatialReference, Time, UserService} from '@geoengine/common';
import {
    CoreModule,
    DEFAULT_CORE_CONFIG,
    FootprintsService,
    MapContainerComponent,
    MapService,
    ProjectService,
    SpatialReferenceService,
} from '@geoengine/core';
import {AppConfig} from '../app-config.service';
import {MainComponent} from './main.component';
import {EOdysseyLayerService} from '../layers/layers.service';

// Keep the real toolbar and bindings; the map and its HTTP requests are tested in core.
describe('EOdyssey debug reprojection', () => {
    let fixture: ComponentFixture<MainComponent>;
    let map: OlMap;
    let view: View;

    beforeEach(async () => {
        vi.stubGlobal(
            'ResizeObserver',
            class {
                observe(): void {
                    /* No layout observer is needed for this toolbar test. */
                }
                unobserve(): void {
                    /* No observer to remove. */
                }
                disconnect(): void {
                    /* No observer to disconnect. */
                }
            },
        );
        view = new View({projection: 'EPSG:3857', center: [0, 0], resolution: 2000});
        map = new OlMap({view});
        map.setSize([1000, 700]);
        const activeView = signal(view);
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
                        view: activeView,
                        setView: (next: View): void => activeView.set(next),
                    },
                },
                {
                    provide: ProjectService,
                    useValue: {
                        getSpatialReferenceStream: vi.fn().mockReturnValue(of(new SpatialReference('EPSG:3857'))),
                        getSpatialReferenceOnce: vi.fn().mockReturnValue(of(new SpatialReference('EPSG:3857'))),
                        getTimeStream: vi.fn().mockReturnValue(of(new Time(new Date('2026-04-01T00:00:00Z')))),
                        setTime: vi.fn().mockResolvedValue(undefined),
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
                {provide: LayersService, useValue: {}},
                {
                    provide: FootprintsService,
                    useValue: {
                        layer: signal(undefined),
                        overlayLayer: new OlLayerVector(),
                        overlayBlocked: signal(false),
                        timeStepOverride: signal(undefined),
                        overlayVisible: signal(false),
                        overlayTooLarge: signal(false),
                        selectedDateEmpty: signal(false),
                        overlayLoading: signal(false),
                        selectedOutsideData: signal(undefined),
                        datesStale: signal(false),
                        timeStep: signal({durationAmount: 1, durationUnit: 'day'}),
                        time: signal(new Time(Date.UTC(2026, 3, 10))),
                        dateWithData: vi.fn(),
                    },
                },
                {
                    provide: EOdysseyLayerService,
                    useValue: {
                        mapTileLayer: vi.fn().mockReturnValue({dataConnectorId: 'data', layerId: 'layer'}),
                        catalogueLoading: vi.fn().mockReturnValue(false),
                        appliedMaxMapExtent: signal({width: 1000000, height: 1400000}),
                        selectedMaxMapExtent: signal({width: 500000, height: 700000}),
                        selectedPreset: vi.fn().mockReturnValue(undefined),
                        selectedDataSource: vi.fn().mockReturnValue(undefined),
                        appliedDataSource: vi.fn().mockReturnValue(undefined),
                        dataSourceOfTime: vi.fn().mockReturnValue(undefined),
                        zoomRequired: signal(false),
                        dateMoved: signal(undefined),
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
        // The empty map element stands in for the queried map component during layout setup.
        vi.spyOn(fixture.componentInstance, 'mapComponent').mockReturnValue({
            dataOverlayLayer: signal(undefined),
            getMap: (): OlMap => map,
        } as unknown as MapContainerComponent);
    });

    afterEach(() => {
        map.dispose();
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
    });

    function tms(): unknown {
        return fixture.debugElement.query(By.css('geoengine-ol-ogc-api-map-tile-layer')).properties['tmsId'];
    }

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
        expect(layerProperties['maxResolution']).toBe(1000);
        expect(fixture.componentInstance.spatialReference()?.srsString).toBe('EPSG:3857');
        expect(fixture.componentInstance.mapTileLayer()).toBe(layer);
        expect(fixture.componentInstance.currentTime()).toBe(time);

        (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>('mat-button-toggle:first-child button')!.click();
        fixture.detectChanges();
        expect(tms()).toBe('CustomWebMercator');
    });

    it('updates the resolution cutoff when the map is resized or rotated', () => {
        fixture.detectChanges();
        fixture.detectChanges();
        expect(fixture.componentInstance.maxResolution()).toBe(1000);
        map.setSize([2000, 700]);
        fixture.detectChanges();
        expect(fixture.componentInstance.maxResolution()).toBe(500);
        map.setSize([1000, 700]);
        view.setRotation(Math.PI / 2);
        fixture.detectChanges();
        expect(fixture.componentInstance.maxResolution()).toBeCloseTo(1400);
    });

    it('starts observing the real map after asynchronous project initialization replaces the placeholder', async () => {
        map.setSize(undefined);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.maxResolution()).toBe(0);

        const placeholder = map;
        view = new View({projection: 'EPSG:3857', center: [0, 0], resolution: 2000});
        map = new OlMap({view});
        // The real map can already be laid out before the parent subscribes to it.
        map.setSize([1000, 700]);
        TestBed.inject(MapService).setView(view);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.maxResolution()).toBe(1000);

        placeholder.setSize([5000, 5000]);
        expect(fixture.componentInstance.maxResolution()).toBe(1000);
        placeholder.dispose();
        map.setSize([2000, 700]);
        fixture.detectChanges();
        expect(fixture.componentInstance.maxResolution()).toBe(500);
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

    it('keeps the selected layer hidden while it is not shown and asks for no coverage where its tiles would not load', () => {
        const layers = TestBed.inject(EOdysseyLayerService) as unknown as {mapTileLayer: Mock; selectedPreset: Mock};
        layers.mapTileLayer.mockReturnValue(undefined);
        layers.selectedPreset.mockReturnValue({connectorId: 'selected', layerId: 'preset'});
        fixture.detectChanges();
        const tileLayer = fixture.debugElement.query(By.css('geoengine-ol-ogc-api-map-tile-layer'));
        expect(tileLayer.properties['dataLayerId']).toBe('preset');
        expect(tileLayer.properties['isVisible']).toBe(false);
        expect(fixture.componentInstance.maxResolution()).toBe(500);
        expect(tileLayer.properties['tilesLoadableLevelsOut']).toBeUndefined();

        const overlayBlocked = TestBed.inject(FootprintsService).overlayBlocked;
        // until the layer knows its zoom limit
        expect(overlayBlocked()).toBe(true);
        tileLayer.triggerEventHandler('tilesLoadable', true);
        TestBed.tick();
        expect(overlayBlocked()).toBe(false);
        tileLayer.triggerEventHandler('tilesLoadable', false);
        TestBed.tick();
        expect(overlayBlocked()).toBe(true);
    });

    it('gives imagery and footprint polygons the same exclusive cutoff, including before move end and after resizing', () => {
        fixture.detectChanges();
        fixture.detectChanges();
        const overlay = TestBed.inject(FootprintsService).overlayLayer;
        const rasterCutoff = (): number =>
            fixture.debugElement.query(By.css('geoengine-ol-ogc-api-map-tile-layer')).properties['maxResolution'] as number;
        expect(overlay.getMaxResolution()).toBe(rasterCutoff());
        expect(rasterCutoff()).toBe(1000);
        // No viewport publication is needed: OpenLayers applies the cutoff on each frame.
        view.setResolution(999);
        expect(overlay.isVisible(view)).toBe(true);
        view.setResolution(1000);
        expect(overlay.isVisible(view)).toBe(false);
        view.setResolution(1001);
        expect(overlay.isVisible(view)).toBe(false);

        map.setSize([2000, 700]);
        fixture.detectChanges();
        expect(rasterCutoff()).toBe(500);
        expect(overlay.getMaxResolution()).toBe(rasterCutoff());
        view.setResolution(499);
        expect(overlay.isVisible(view)).toBe(true);
        view.setResolution(500);
        expect(overlay.isVisible(view)).toBe(false);

        fixture.componentRef.setInput('debug', true);
        fixture.detectChanges();
        selectFrontend();
        expect(overlay.getMaxResolution()).toBe(rasterCutoff());
        expect(rasterCutoff()).toBe(500);
    });

    it('gives the dates no layer of the previous data source while the presets of a new one load', () => {
        const layers = TestBed.inject(EOdysseyLayerService) as unknown as {appliedDataSource: Mock; selectedDataSource: Mock};
        // the map still shows the previous source, the new one has no preset yet
        layers.appliedDataSource.mockReturnValue({key: 'previous'});
        layers.selectedDataSource.mockReturnValue({key: 'new'});
        fixture.detectChanges();
        expect(TestBed.inject(FootprintsService).layer()).toBeUndefined();
    });

    it('asks to zoom in on the map when the boxes of the coverage need too many catalog searches', () => {
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        expect(element.querySelector('.map-notice')).toBeNull();

        // only while the coverage is on the map; the panel says it otherwise
        (TestBed.inject(FootprintsService).overlayTooLarge as WritableSignal<boolean>).set(true);
        fixture.detectChanges();
        expect(element.querySelector('.map-notice')).toBeNull();
        TestBed.inject(FootprintsService).overlayVisible.set(true);
        fixture.detectChanges();

        expect(element.querySelector('.map-notice')?.textContent).toContain('The data availability shows from a closer zoom');
    });

    it('says so while the date of the layer on the map has certainly no scenes in the view, and still loads its tiles', () => {
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        const isVisible = (): unknown => fixture.debugElement.query(By.css('geoengine-ol-ogc-api-map-tile-layer')).properties['isVisible'];
        expect(isVisible()).toBe(true);
        expect(element.querySelector('.map-notice')).toBeNull();

        (TestBed.inject(FootprintsService).selectedDateEmpty as WritableSignal<boolean>).set(true);
        fixture.detectChanges();

        expect(isVisible()).toBe(true);
        expect(element.querySelector('.map-notice')?.textContent).toContain('No scenes here on 10.04.2026');
    });

    it('offers the nearest date with scenes where the date has none in the view, or the nearest date of the data', () => {
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        const footprints = TestBed.inject(FootprintsService) as unknown as {
            selectedDateEmpty: WritableSignal<boolean>;
            selectedOutsideData: WritableSignal<unknown>;
            dateWithData: Mock;
        };
        footprints.dateWithData.mockImplementation((direction: number) =>
            direction === 1 ? new Time(Date.UTC(2026, 3, 13)) : new Time(Date.UTC(2026, 3, 8)),
        );
        footprints.selectedDateEmpty.set(true);
        fixture.detectChanges();
        const action = (): HTMLButtonElement | null => element.querySelector('.map-notice .notice-action');
        expect(action()?.textContent).toContain('Nearest date: 08.04.2026');
        action()?.click();
        expect((TestBed.inject(ProjectService) as unknown as {setTime: Mock}).setTime).toHaveBeenCalledWith(new Time(Date.UTC(2026, 3, 8)));

        footprints.selectedOutsideData.set({direction: 1, step: {time: {start: Date.UTC(2025, 11, 31), end: Date.UTC(2026, 0, 1)}}});
        fixture.detectChanges();
        expect(element.querySelector('.map-notice')?.textContent).toContain('has no data after 31.12.2025');
        expect(action()?.textContent).toContain('Show 31.12.2025');
    });

    it('tells on the map that a new data source moved the date, offers the date before, and closes', () => {
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        const layers = TestBed.inject(EOdysseyLayerService);
        const setTime = (TestBed.inject(ProjectService) as unknown as {setTime: Mock}).setTime;
        const notice = (): string => element.querySelector('.map-notice')?.textContent ?? '';
        // the date was moved to 10.04.2026, the selected one
        const moved = {from: new Time(Date.UTC(2026, 10, 23)), to: Date.UTC(2026, 3, 10), direction: 1 as const};
        layers.dateMoved.set(moved);
        fixture.detectChanges();
        expect(notice()).toContain('Date moved to 10.04.2026');
        expect(notice()).toContain('Back to 23.11.2026');
        element.querySelector<HTMLButtonElement>('.map-notice .notice-action')?.click();
        expect(setTime).toHaveBeenCalledWith(moved.from);
        fixture.detectChanges();
        expect(element.querySelector('.map-notice')).toBeNull();

        // a date with a time of day, e.g. of hourly data, keeps it
        layers.dateMoved.set({...moved, from: new Time(Date.UTC(2026, 2, 12, 14))});
        fixture.detectChanges();
        expect(notice()).toContain('Back to 12.03.2026 14:00');

        layers.dateMoved.set(moved);
        fixture.detectChanges();
        element.querySelector<HTMLButtonElement>('.map-notice .notice-close')?.click();
        fixture.detectChanges();
        expect(element.querySelector('.map-notice')).toBeNull();
        expect(layers.dateMoved()).toBeUndefined();
    });

    it('offers to zoom in where the imagery does not load at this zoom', () => {
        TestBed.inject(EOdysseyLayerService).zoomRequired.set(true);
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        expect(element.querySelector('.map-notice')?.textContent).toContain('The imagery shows from a closer zoom');
        expect(element.querySelector('.map-notice .notice-action')?.textContent).toContain('Zoom in');
    });

    it('shows that it loads while the boxes of the date load, also before the tiles of the layer load', () => {
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        expect(element.querySelector('.loading-indicator')).toBeNull();

        (TestBed.inject(FootprintsService).overlayLoading as WritableSignal<boolean>).set(true);
        fixture.detectChanges();

        expect(element.querySelector('.loading-indicator')).not.toBeNull();
    });

    it('asks to zoom in on the map when the availability is shown where the tiles of the layer do not load', () => {
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        const footprints = TestBed.inject(FootprintsService);
        // blocked until a layer says that its tiles load
        expect(footprints.overlayBlocked()).toBe(true);
        expect(element.querySelector('.map-notice')).toBeNull();

        footprints.overlayVisible.set(true);
        fixture.detectChanges();

        expect(element.querySelector('.map-notice')?.textContent).toContain('The data availability shows from a closer zoom');

        // the dates of the view say that the date has no scenes, so zooming in is no use
        (footprints.selectedDateEmpty as WritableSignal<boolean>).set(true);
        fixture.detectChanges();
        expect(element.querySelector('.map-notice')?.textContent).toContain('No scenes here on 10.04.2026');
    });

    it('keeps the sidebar open when Escape is pressed inside it', () => {
        fixture.detectChanges();
        const drawer = fixture.debugElement.query(By.directive(MatDrawer));
        // An open select keeps focus on its trigger in the drawer, so its Escape bubbles up to the drawer.
        (drawer.nativeElement as HTMLElement)
            .querySelector('.drawer-main')!
            .dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape', keyCode: ESCAPE, bubbles: true}));
        fixture.detectChanges();
        expect(drawer.injector.get(MatDrawer).opened).toBe(true);
    });
});
