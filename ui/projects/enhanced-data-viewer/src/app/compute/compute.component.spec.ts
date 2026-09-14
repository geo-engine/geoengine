import {beforeEach, describe, expect, it, vi} from 'vitest';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {provideZonelessChangeDetection, Signal, signal} from '@angular/core';
import OlFeature from 'ol/Feature';
import OlGeomPolygon from 'ol/geom/Polygon';
import OlLayerVector from 'ol/layer/Vector';
import OlSourceVector from 'ol/source/Vector';
import OlGeometry from 'ol/geom/Geometry';
import {MatDialog, MatDialogModule} from '@angular/material/dialog';
import {BackendService, CoreConfig, MapService, ProjectService} from '@geoengine/core';
import {
    BoundingBox2D,
    CommonConfig,
    Coordinate2D,
    GeoTransform,
    GridBoundingBox2D,
    GridIdx2D,
    LayersService,
    NotificationService,
    PlotsService,
    RasterDataTypes,
    RasterLayerMetadata,
    SpatialGridDefinition,
    SpatialGridDescriptor,
    SpatialReference,
    UserService,
} from '@geoengine/common';
import {PlotOutputFormat, WrappedPlotOutput} from '@geoengine/api-client';
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
            getWorkflowIdMetadata: vi
                .fn()
                .mockResolvedValue(
                    new RasterLayerMetadata(
                        RasterDataTypes.Float32,
                        new SpatialReference('EPSG:4326'),
                        [{name: 'red', measurement: {type: 'unitless'}}],
                        new SpatialGridDescriptor(
                            new SpatialGridDefinition(
                                new GeoTransform(new Coordinate2D([0, 0]), 1, 1),
                                new GridBoundingBox2D(new GridIdx2D(0, 0), new GridIdx2D(1, 1)),
                            ),
                            'source',
                        ),
                    ),
                ),
        } satisfies Pick<LayersService, 'registerAndGetLayerWorkflowId' | 'getWorkflowIdMetadata'>;

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
                {provide: CoreConfig, useValue: {PLOTS: {THEME: 'excel'}}},
                {provide: CommonConfig, useExisting: CoreConfig},
                {provide: UserService, useValue: {getSessionToken: vi.fn()}},
            ],
            imports: [ComputeComponent, MatDialogModule],
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
            bbox: new BoundingBox2D([0, 0, 1, 1]),
            spatialReference: new SpatialReference('EPSG:4326'),
        });
        expect(component.cannotComputeHistogram()).toBe(false);

        const host = fixture.nativeElement as HTMLElement;
        const button = host.querySelector('button');

        expect(button).toBeTruthy();
        expect(button?.disabled).toBe(false);
    });

    it('updates band names and the workflow when the selected layer changes', async () => {
        const layersService = TestBed.inject(LayersService);
        const registerWorkflow = vi
            .spyOn(layersService, 'registerAndGetLayerWorkflowId')
            .mockImplementation(() => Promise.resolve('ndvi-workflow'));
        const getMetadata = vi
            .spyOn(layersService, 'getWorkflowIdMetadata')
            .mockImplementation(() =>
                Promise.resolve(
                    new RasterLayerMetadata(
                        RasterDataTypes.Float32,
                        new SpatialReference('EPSG:4326'),
                        [{name: 'NDVI', measurement: {type: 'unitless'}}],
                        new SpatialGridDescriptor(
                            new SpatialGridDefinition(
                                new GeoTransform(new Coordinate2D([0, 0]), 1, 1),
                                new GridBoundingBox2D(new GridIdx2D(0, 0), new GridIdx2D(1, 1)),
                            ),
                            'source',
                        ),
                    ),
                ),
            );
        const plotOutput: WrappedPlotOutput = {
            outputFormat: PlotOutputFormat.JsonVega,
            plotType: 'histogram',
            data: {vega: 'plot'},
        };
        component.plotData.set(plotOutput);

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
        const plotOutput: WrappedPlotOutput = {
            outputFormat: PlotOutputFormat.JsonVega,
            plotType: 'histogram',
            data: {vega: 'plot'},
        };
        component.plotData.set(plotOutput);

        overlayLayer.set(createBoxOverlay());
        fixture.detectChanges();
        await fixture.whenStable();

        expect(component.plotData()).toBeUndefined();
    });

    it('opens a fullscreen histogram dialog when plot data exists', async () => {
        const openSpy = vi.spyOn(MatDialog.prototype, 'open').mockImplementation(
            (): ReturnType<MatDialog['open']> =>
                ({
                    afterClosed: () => ({subscribe: () => undefined}),
                    componentInstance: undefined,
                    componentRef: undefined,
                    close: vi.fn(),
                    updatePosition: vi.fn(),
                }) as unknown as ReturnType<MatDialog['open']>,
        );
        const plotOutput: WrappedPlotOutput = {
            outputFormat: PlotOutputFormat.JsonVega,
            plotType: 'histogram',
            data: {vegaString: '{"mark":"bar"}', metadata: {selectionName: 'selection'}},
        };

        component.plotData.set(plotOutput);
        fixture.detectChanges();
        await fixture.whenStable();

        component.openHistogramDialog(plotOutput);

        expect(openSpy).toHaveBeenCalledTimes(1);
        const [dialogComponent, dialogConfig] = openSpy.mock.calls[0] ?? [undefined, undefined];

        expect(dialogComponent).toBeDefined();
        expect(dialogConfig).toMatchObject({
            maxWidth: '100vw',
            maxHeight: '100vh',
        });
        expect(dialogConfig?.data).toMatchObject({
            vegaString: '{"mark":"bar"}',
            metadata: {selectionName: 'selection'},
        });
    });
});
