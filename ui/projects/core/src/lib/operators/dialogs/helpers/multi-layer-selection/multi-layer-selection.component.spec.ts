import {vi, type Mock} from 'vitest';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {MultiLayerSelectionComponent} from './multi-layer-selection.component';
import {ProjectService} from '../../../../project/project.service';
import {inputBinding, provideZonelessChangeDetection, signal, WritableSignal} from '@angular/core';
import {of} from 'rxjs';
import {DialogSectionHeadingComponent} from '../../../../dialogs/dialog-section-heading/dialog-section-heading.component';
import {MATERIAL_MODULES} from '../../../../core.module';
import {By} from '@angular/platform-browser';
import {WGS_84} from '../../../../spatial-references/spatial-reference.service';
import {
    GeoTransform,
    Layer,
    RasterDataTypes,
    RasterLayer,
    RasterLayerMetadata,
    RasterSymbology,
    SpatialGridDefinition,
    SpatialGridDescriptor,
    Coordinate2D,
    GridBoundingBox2D,
    GridIdx2D,
    ResultType,
    ResultTypes,
    UnitlessMeasurement,
} from '@geoengine/common';
import {RasterBandDescriptor} from '@geoengine/api-client';

describe('MultiLayerSelectionComponent', () => {
    let component: MultiLayerSelectionComponent;
    let fixture: ComponentFixture<MultiLayerSelectionComponent>;
    let layersInput: WritableSignal<Array<Layer>>;
    let minInput: WritableSignal<number>;
    let maxInput: WritableSignal<number>;
    let typesInput: WritableSignal<Array<ResultType>>;

    /** Mock Layers **/
    const layer1: Layer = new RasterLayer({
        name: 'test-layer1',
        workflowId: '1',
        isLegendVisible: true,
        isVisible: true,
        symbology: RasterSymbology.fromRasterSymbologyDict({
            type: 'raster',
            opacity: 1.0,
            rasterColorizer: {
                type: 'singleBand',
                band: 0,
                bandColorizer: {
                    type: 'linearGradient',
                    breakpoints: [
                        {value: 1, color: [0, 0, 0, 255]},
                        {value: 255, color: [255, 255, 255, 255]},
                    ],
                    overColor: [255, 255, 255, 127],
                    underColor: [0, 0, 0, 127],
                    noDataColor: [0, 0, 0, 0],
                },
            },
        }),
    });
    const layer2: Layer = new RasterLayer({
        name: 'test-layer2',
        workflowId: '2',
        isLegendVisible: true,
        isVisible: true,
        symbology: RasterSymbology.fromRasterSymbologyDict({
            type: 'raster',
            opacity: 1.0,
            rasterColorizer: {
                type: 'singleBand',
                band: 0,
                bandColorizer: {
                    type: 'linearGradient',
                    breakpoints: [
                        {value: 1, color: [0, 0, 0, 255]},
                        {value: 255, color: [255, 255, 255, 255]},
                    ],
                    overColor: [255, 255, 255, 127],
                    underColor: [0, 0, 0, 127],
                    noDataColor: [0, 0, 0, 0],
                },
            },
        }),
    });
    const layer3: Layer = new RasterLayer({
        name: 'test-layer3',
        workflowId: '3',
        isLegendVisible: true,
        isVisible: true,
        symbology: RasterSymbology.fromRasterSymbologyDict({
            type: 'raster',
            opacity: 1.0,
            rasterColorizer: {
                type: 'singleBand',
                band: 0,
                bandColorizer: {
                    type: 'linearGradient',
                    breakpoints: [
                        {value: 1, color: [0, 0, 0, 255]},
                        {value: 255, color: [255, 255, 255, 255]},
                    ],
                    overColor: [255, 255, 255, 127],
                    underColor: [0, 0, 0, 127],
                    noDataColor: [0, 0, 0, 0],
                },
            },
        }),
    });
    const mockLayers: Array<Layer> = [layer1, layer2, layer3];

    /** Mock project Service **/
    let projectServiceSpy: {
        getLayerStream: Mock;
        getLayerMetadata: Mock;
    };

    beforeEach(async () => {
        projectServiceSpy = {
            getLayerStream: vi.fn().mockName('ProjectService.getLayerStream'),
            getLayerMetadata: vi.fn().mockName('ProjectService.getLayerMetadata'),
        } as {
            getLayerStream: Mock;
            getLayerMetadata: Mock;
        };

        /** ProjectService returns Mock Layers **/
        projectServiceSpy.getLayerStream.mockReturnValue(of<Array<Layer>>(mockLayers));
        projectServiceSpy.getLayerMetadata.mockReturnValue(
            of<RasterLayerMetadata>(
                new RasterLayerMetadata(
                    RasterDataTypes.Byte,
                    WGS_84.spatialReference,
                    [{name: 'band', measurement: new UnitlessMeasurement().toDict()} as RasterBandDescriptor],
                    new SpatialGridDescriptor(
                        new SpatialGridDefinition(
                            new GeoTransform(new Coordinate2D([0.0, 0.0]), 1.0, -1.0),
                            new GridBoundingBox2D(new GridIdx2D(0, 0), new GridIdx2D(100, 100)),
                        ),
                        'source',
                    ),
                ),
            ),
        );

        await TestBed.configureTestingModule({
            providers: [provideZonelessChangeDetection(), {provide: ProjectService, useValue: projectServiceSpy}],
            imports: [...MATERIAL_MODULES, MultiLayerSelectionComponent, DialogSectionHeadingComponent],
        }).compileComponents();

        layersInput = signal([]);
        minInput = signal(1);
        maxInput = signal(1);
        typesInput = signal(ResultTypes.ALL_TYPES);

        fixture = TestBed.createComponent(MultiLayerSelectionComponent, {
            bindings: [
                inputBinding('layers', layersInput),
                inputBinding('min', minInput),
                inputBinding('max', maxInput),
                inputBinding('types', typesInput),
            ],
        });
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    /** create app **/
    it('should create the app', async () => {
        await expect(component).toBeTruthy();
    });

    /** initially no Layers -> 'No Input Available' should be displayed **/
    it('should display "No Input Available", if no Layer is available', async () => {
        const html = (fixture.nativeElement as HTMLElement).querySelector('p');
        if (!html) throw new Error('no paragraph element found in DOM');
        await expect(component.value()).toEqual([]);
        await expect(html.textContent).toEqual('No Input Available');
    });

    /** Adding three layers
     * checking the number of possible layers to select from
     * checking the default layer displayed **/
    it('should display the first of selectedLayers per default, min = max = 1', async () => {
        layersInput.set(mockLayers);
        minInput.set(1);
        maxInput.set(1);

        await fixture.whenStable();

        const layers = layerNamesFromDom(fixture);

        await expect(layers.length).toEqual(1);
        await expect(component.value().length).toEqual(1);
        await expect(layers[0]).toEqual(component.value()[0].name);
    });

    /** checking the layer displayed after changing the selected layer **/
    it('should update the layer displayed to equal selected layer, min = max = 1', async () => {
        layersInput.set(mockLayers);
        minInput.set(1);
        maxInput.set(1);

        await fixture.whenStable();

        for (let i = mockLayers.length; i > 0; i--) {
            component.updateLayer(0, mockLayers[i - 1]);

            await fixture.whenStable();

            const layers = layerNamesFromDom(fixture);

            await expect(component.value().length).toEqual(1);
            await expect(layers[0]).toEqual(component.value()[0].name);
            await expect(layers[0]).toEqual(mockLayers[i - 1].name);
        }
    });

    /** Adding three layers
     * adding two more input fields
     * checking the default layers displayed and the number of possible layers to select **/
    it('should display the selectedLayers per default, max = 3', async () => {
        layersInput.set(mockLayers);
        minInput.set(1);
        maxInput.set(3);

        await fixture.whenStable();

        component.add();
        component.add();

        await fixture.whenStable();

        const layers = layerNamesFromDom(fixture);

        await expect(component.value().length).toEqual(3);
        await expect(layers.length).toEqual(3);

        for (let i = 0; i < mockLayers.length; i++) {
            await expect(layers[i]).toEqual(component.value()[i].name);
        }
    });

    /** checking the layer displayed after changing the selected layer **/
    it('should update the layer displayed to equal selected layer, max = 3', async () => {
        async function testInputs(numLayers: number): Promise<void> {
            for (let j = 0; j < numLayers; j++) {
                await fixture.whenStable();

                const layers = layerNamesFromDom(fixture);

                for (let k = 0; k < numLayers; k++) {
                    await expect(layers[k]).toEqual(component.value()[k].name);
                }
            }
        }

        layersInput.set(mockLayers);
        minInput.set(1);
        maxInput.set(3);

        await fixture.whenStable();

        component.add();
        component.add();

        const numLayers = component.value().length;
        await expect(numLayers).toEqual(mockLayers.length);
        await testInputs(numLayers);

        for (let i = 0; i < numLayers; i++) {
            component.updateLayer(i, mockLayers[2]);
            await testInputs(numLayers);

            component.updateLayer(i, mockLayers[1]);
            await testInputs(numLayers);

            component.updateLayer(i, mockLayers[0]);
            await testInputs(numLayers);
        }
    });
});

function layerNamesFromDom(fixture: ComponentFixture<MultiLayerSelectionComponent>): Array<string> {
    const layers: Array<string> = [];
    const selects = fixture.debugElement.queryAll(By.css('mat-select'));
    for (const select of selects) {
        const element = select.nativeElement as HTMLElement;
        layers.push(element.textContent.trim());
    }
    return layers;
}
