import {Component, signal, untracked} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {By} from '@angular/platform-browser';
import {defaults as defaultInteractions, DoubleClickZoom} from 'ol/interaction';
import OlMap from 'ol/Map';
import {Observable, of} from 'rxjs';
import {describe, expect, it, beforeAll, vi} from 'vitest';
import {SpatialReference} from '@geoengine/common';
import {CoreConfig, DEFAULT_CORE_CONFIG} from '../../config.service';
import {BasemapService} from '../../layers/basemap.service';
import {ProjectService} from '../../project/project.service';
import {SpatialReferenceService} from '../../spatial-references/spatial-reference.service';
import {MapLayerComponent} from '../map-layer.component';
import {MapService} from '../map.service';
import {MapContainerComponent} from './map-container.component';
import {setMapDoubleClickZoom} from './map-utils';

beforeAll(() => {
    globalThis.ResizeObserver = class ResizeObserver {
        observe(): void {
            /* empty */
        }
        unobserve(): void {
            /* empty */
        }
        disconnect(): void {
            /* empty */
        }
    };
});

describe('setMapDoubleClickZoom', () => {
    it('toggles the OpenLayers double-click zoom interaction', () => {
        const map = new OlMap({
            interactions: defaultInteractions({doubleClickZoom: true}),
        });

        const interaction = map
            .getInteractions()
            .getArray()
            .find((candidate) => candidate instanceof DoubleClickZoom);

        expect(interaction).toBeDefined();
        expect(interaction?.getActive()).toBe(true);

        setMapDoubleClickZoom(map, false);
        expect(interaction?.getActive()).toBe(false);

        setMapDoubleClickZoom(map, true);
        expect(interaction?.getActive()).toBe(true);
    });
});

@Component({
    selector: 'geoengine-test-layer',
    template: '',
    providers: [{provide: MapLayerComponent, useExisting: TestLayerComponent}],
})
class TestLayerComponent {
    readonly isVisible = signal(false);
}

@Component({
    template: '<geoengine-map-container [grid]="false"><geoengine-test-layer /></geoengine-map-container>',
    imports: [MapContainerComponent, TestLayerComponent],
})
class HostComponent {}

describe('MapContainerComponent', () => {
    it('adds a layer to the map that is shown after the map drew its layers', async () => {
        // only the layer list is under test, not drawing with OpenLayers
        vi.spyOn(MapContainerComponent.prototype, 'ngAfterViewInit').mockImplementation(() => undefined);
        TestBed.configureTestingModule({
            providers: [
                {provide: CoreConfig, useValue: DEFAULT_CORE_CONFIG},
                {provide: MapService, useValue: {}},
                {
                    provide: ProjectService,
                    useValue: {getSpatialReferenceStream: (): Observable<SpatialReference> => of(new SpatialReference('EPSG:3857'))},
                },
                {provide: SpatialReferenceService, useValue: {}},
                {provide: BasemapService, useValue: {basemap: signal(undefined)}},
            ],
        });
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const redraw = vi.spyOn(MapContainerComponent.prototype as any, 'redrawLayers').mockImplementation(function (
            this: MapContainerComponent,
        ) {
            // untracked: the redraw must not be what makes the effect follow the visibility
            this.mapLayers = untracked(() => this.mapLayersRaw().filter((mapLayer) => mapLayer.isVisible()));
        });
        const fixture = TestBed.createComponent(HostComponent);
        fixture.detectChanges();
        await fixture.whenStable();
        const container = fixture.debugElement.query(By.directive(MapContainerComponent)).componentInstance as MapContainerComponent;
        // the container projects no content, so the layer is only reachable through its query
        const layer = container.mapLayersRaw()[0] as unknown as TestLayerComponent;
        expect(container.mapLayers).toEqual([]);

        layer.isVisible.set(true);
        fixture.detectChanges();
        await fixture.whenStable();

        expect(redraw).toHaveBeenCalled();
        expect(container.mapLayers).toEqual([layer]);

        // a layer on the map hides and shows itself, which redraws no map
        redraw.mockClear();
        layer.isVisible.set(false);
        fixture.detectChanges();
        await fixture.whenStable();
        layer.isVisible.set(true);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(redraw).not.toHaveBeenCalled();
        redraw.mockRestore();
    });
});
