import {NO_ERRORS_SCHEMA, signal, WritableSignal} from '@angular/core';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatIconRegistry} from '@angular/material/icon';
import {MatListModule} from '@angular/material/list';
import {MatMenuModule} from '@angular/material/menu';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {By, DomSanitizer} from '@angular/platform-browser';
import {provideRouter} from '@angular/router';
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

    beforeEach(async () => {
        vi.stubGlobal(
            'ResizeObserver',
            class {
                observe(): void {
                    /* No layout observer is needed for this toolbar test. */
                }
                disconnect(): void {
                    /* No observer to disconnect. */
                }
            },
        );
        await TestBed.configureTestingModule({
            imports: [MainComponent],
            providers: [
                provideRouter([]),
                {provide: AppConfig, useValue: DEFAULT_CORE_CONFIG},
                {provide: MapService, useValue: {registerMapComponent: vi.fn(), zoomTo: vi.fn()}},
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
                {provide: LayersService, useValue: {}},
                {
                    provide: FootprintsService,
                    useValue: {
                        layer: signal(undefined),
                        overlayLight: signal(false),
                        overlayBlocked: signal(false),
                        timeStepOverride: signal(undefined),
                        overlayVisible: signal(false),
                        overlayTooLarge: signal(false),
                    },
                },
                {
                    provide: EOdysseyLayerService,
                    useValue: {
                        mapTileLayer: vi.fn().mockReturnValue({dataConnectorId: 'data', layerId: 'layer'}),
                        catalogueLoading: vi.fn().mockReturnValue(false),
                        appliedMinTileMatrixIndex: vi.fn().mockReturnValue(4),
                        selectedMinTileMatrixIndex: vi.fn().mockReturnValue(6),
                        selectedPreset: vi.fn().mockReturnValue(undefined),
                        selectedDataSource: vi.fn().mockReturnValue(undefined),
                        dataSourceOfTime: vi.fn().mockReturnValue(undefined),
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
        } as unknown as MapContainerComponent);
    });

    afterEach(() => {
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

    it('keeps the selected layer hidden while its data is off and asks for no coverage where its tiles would not load', () => {
        const layers = TestBed.inject(EOdysseyLayerService) as unknown as {mapTileLayer: Mock; selectedPreset: Mock};
        layers.mapTileLayer.mockReturnValue(undefined);
        layers.selectedPreset.mockReturnValue({connectorId: 'selected', layerId: 'preset'});
        fixture.detectChanges();
        const tileLayer = fixture.debugElement.query(By.css('geoengine-ol-ogc-api-map-tile-layer'));
        expect(tileLayer.properties['dataLayerId']).toBe('preset');
        expect(tileLayer.properties['isVisible']).toBe(false);
        expect(tileLayer.properties['minTileMatrixIndex']).toBe(6);

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

    it('asks to zoom in on the map when the boxes of the coverage need too many catalog searches', () => {
        fixture.detectChanges();
        const element = fixture.nativeElement as HTMLElement;
        expect(element.querySelector('.zoom-prompt')).toBeNull();

        // only while the coverage is on the map; the panel says it otherwise
        (TestBed.inject(FootprintsService).overlayTooLarge as WritableSignal<boolean>).set(true);
        fixture.detectChanges();
        expect(element.querySelector('.zoom-prompt')).toBeNull();
        TestBed.inject(FootprintsService).overlayVisible.set(true);
        fixture.detectChanges();

        expect(element.querySelector('.zoom-prompt')?.textContent).toContain('Zoom in to see where the scenes of this date are');
    });
});
