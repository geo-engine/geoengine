import {ChangeDetectionStrategy, Component, signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {form, FormField} from '@angular/forms/signals';
import {Layer, LayerMetadata, ResultTypes} from '@geoengine/common';
import {Observable, of} from 'rxjs';
import {describe, expect, it} from 'vitest';
import {ProjectService} from '../../../../project/project.service';
import {LayerSelectionComponent} from './layer-selection.component';

const rasterLayer = {name: 'raster layer'} as Layer;

@Component({
    selector: 'geoengine-test-host',
    template: `<geoengine-layer-selection [types]="types" [formField]="layerForm.layer"></geoengine-layer-selection>`,
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [LayerSelectionComponent, FormField],
})
class TestHostComponent {
    readonly types = [ResultTypes.RASTER];
    readonly model = signal<{layer: Layer | null}>({layer: null});
    readonly layerForm = form(this.model);
}

describe('LayerSelectionComponent', () => {
    it('can be bound to a signal form field and emits the selected layer', async () => {
        TestBed.configureTestingModule({
            imports: [TestHostComponent],
            providers: [
                {
                    provide: ProjectService,
                    useValue: {
                        getLayerStream: (): Observable<Array<Layer>> => of([rasterLayer]),
                        getLayerMetadata: (): Observable<LayerMetadata> => of({resultType: ResultTypes.RASTER} as LayerMetadata),
                    },
                },
            ],
        });

        const fixture = TestBed.createComponent(TestHostComponent);
        await fixture.whenStable();

        expect(fixture.componentInstance.model().layer).toBe(rasterLayer);
    });
});
