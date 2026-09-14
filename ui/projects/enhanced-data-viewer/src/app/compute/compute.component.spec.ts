import {beforeEach, describe, expect, it, vi} from 'vitest';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {provideZonelessChangeDetection, Signal, signal} from '@angular/core';
import OlFeature from 'ol/Feature';
import OlGeomPolygon from 'ol/geom/Polygon';
import OlLayerVector from 'ol/layer/Vector';
import OlSourceVector from 'ol/source/Vector';
import OlGeometry from 'ol/geom/Geometry';
import {BackendService, MapService, ProjectService} from '@geoengine/core';
import {LayersService, NotificationService, PlotsService, UserService} from '@geoengine/common';
import {ComputeComponent} from './compute.component';

describe('ComputeComponent', () => {
    let fixture: ComponentFixture<ComputeComponent>;
    let component: ComputeComponent;
    let overlayLayer: ReturnType<typeof signal<OlLayerVector<OlSourceVector<OlFeature>> | undefined>>;

    const createBoxOverlay = (): OlLayerVector<OlSourceVector<OlFeature>> => {
        const geometry = new OlGeomPolygon([
            [
                [0, 0],
                [1, 0],
                [1, 1],
                [0, 1],
                [0, 0],
            ],
        ]);
        const feature = new OlFeature({geometry});
        const source = new OlSourceVector({features: [feature]});

        return new OlLayerVector({source});
    };

    beforeEach(async () => {
        overlayLayer = signal<OlLayerVector<OlSourceVector<OlFeature>> | undefined>(undefined);
        globalThis.ResizeObserver = class {
            observe(): void {
                // no-op in tests
            }
            disconnect(): void {
                // no-op in tests
            }
            unobserve(): void {
                // no-op in tests
            }
        };

        const layersService = {
            registerAndGetLayerWorkflowId: vi.fn().mockResolvedValue('workflow-id'),
            getWorkflowIdMetadata: vi.fn().mockResolvedValue({
                layerType: 'raster',
                bands: [{name: 'red'}],
                pixelSizeX: 1,
                pixelSizeY: 1,
                spatialReference: {srid: 4326},
            }),
        };

        await TestBed.configureTestingModule({
            providers: [
                provideZonelessChangeDetection(),
                {provide: BackendService, useValue: {}},
                {provide: LayersService, useValue: layersService},
                {
                    provide: MapService,
                    useValue: {
                        getLayerOverlay: (): Signal<OlLayerVector<OlSourceVector<OlFeature<OlGeometry>>> | undefined> => overlayLayer,
                        getView: (): {getProjection: () => {getCode: () => string}} => ({
                            getProjection: (): {getCode: () => string} => ({
                                getCode: (): string => 'EPSG:4326',
                            }),
                        }),
                    },
                },
                {provide: NotificationService, useValue: {error: vi.fn()}},
                {provide: PlotsService, useValue: {}},
                {provide: ProjectService, useValue: {getTimeOnce: vi.fn()}},
                {provide: UserService, useValue: {getSessionToken: vi.fn()}},
            ],
            imports: [ComputeComponent],
        }).compileComponents();

        fixture = TestBed.createComponent(ComputeComponent);
        component = fixture.componentInstance;
        fixture.componentRef.setInput('selectedRasterLayer', {dataConnectorId: 'provider-id', layerId: 'layer-id'});
        fixture.detectChanges();
        await fixture.whenStable();
    });

    it('derives a bbox from the polygon overlay and allows histogram computation when the data is ready', async () => {
        overlayLayer.set(createBoxOverlay());
        fixture.detectChanges();
        await fixture.whenStable();

        expect(component.selectedRasterLayer()).toEqual({dataConnectorId: 'provider-id', layerId: 'layer-id'});
        expect(component.selectedBand()).toBe('red');
        expect(component.computationBbox()).toEqual({
            bbox: expect.objectContaining({xmin: 0, ymin: 0, xmax: 1, ymax: 1}),
            spatialReference: expect.objectContaining({srsString: 'EPSG:4326'}),
        });
        expect(component.cannotComputeHistogram()).toBe(false);

        const host = fixture.nativeElement as HTMLElement;
        const button = host.querySelector('button');

        expect(button).toBeTruthy();
        expect(button?.disabled).toBe(false);
    });

    it('updates band names and the workflow when the selected layer changes', async () => {
        const layersService = TestBed.inject(LayersService);
        const registerWorkflow = vi.spyOn(layersService, 'registerAndGetLayerWorkflowId').mockResolvedValue('ndvi-workflow');
        const getMetadata = vi.spyOn(layersService, 'getWorkflowIdMetadata').mockResolvedValue({
            layerType: 'raster',
            bands: [{name: 'NDVI', measurement: {type: 'unitless'}}],
            pixelSizeX: 1,
            pixelSizeY: 1,
        } as never);
        component.plotData.set({outputFormat: 'json-vega', data: {vega: 'plot'}} as never);

        fixture.componentRef.setInput('selectedRasterLayer', {dataConnectorId: 'other-provider', layerId: 'ndvi-layer'});
        fixture.detectChanges();
        await fixture.whenStable();

        expect(registerWorkflow).toHaveBeenLastCalledWith('other-provider', 'ndvi-layer');
        expect(getMetadata).toHaveBeenLastCalledWith('ndvi-workflow');
        expect(component.bands()).toEqual(['NDVI']);
        expect(component.selectedBand()).toBe('NDVI');
        expect(component.plotData()).toBeUndefined();
        expect((fixture.nativeElement as HTMLElement).querySelector('mat-select')?.textContent).toContain('NDVI');
    });

    it('clears the bands and disables computation when no layer is selected', async () => {
        overlayLayer.set(createBoxOverlay());
        fixture.componentRef.setInput('selectedRasterLayer', undefined);
        fixture.detectChanges();
        await fixture.whenStable();

        expect(component.bands()).toBeUndefined();
        expect(component.selectedBand()).toBeUndefined();
        expect(component.cannotComputeHistogram()).toBe(true);
    });

    it('blocks histogram computation until a bounding box has been drawn', async () => {
        overlayLayer.set(undefined);
        fixture.detectChanges();
        await fixture.whenStable();

        expect(component.computationBbox()).toBeUndefined();
        expect(component.cannotComputeHistogram()).toBe(true);

        const host = fixture.nativeElement as HTMLElement;
        const button = host.querySelector('button');

        expect(button).toBeTruthy();
        expect(button?.disabled).toBe(true);
    });

    it('clears the computed plot when the map overlay changes', async () => {
        component.plotData.set({
            outputFormat: 'json-vega',
            data: {vega: 'plot'},
        } as never);

        overlayLayer.set(createBoxOverlay());
        fixture.detectChanges();
        await fixture.whenStable();

        expect(component.plotData()).toBeUndefined();
    });
});
