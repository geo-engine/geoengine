import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {BehaviorSubject, Observable, of} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {RasterFootprints, RasterFootprintsPrecision} from '@geoengine/api-client';
import {LayersService, ProcessingGraphsService, RasterLayerMetadata, Time} from '@geoengine/common';
import {MapService, ProjectService, SpatialReferenceService} from '@geoengine/core';
import {FootprintsService} from './footprints.service';
import {EOdysseyLayerService} from './layers.service';

const FOOTPRINTS: RasterFootprints = {spatialReference: 'EPSG:4326', steps: [], next: null, tooLarge: false};

describe('FootprintsService overlay', () => {
    const getFootprints =
        vi.fn<(id: string, query: {precision?: RasterFootprintsPrecision; limit?: number}) => Promise<RasterFootprints>>();

    beforeEach(() => {
        getFootprints.mockReset().mockResolvedValue(FOOTPRINTS);
        const metadata = Object.assign(Object.create(RasterLayerMetadata.prototype) as object, {
            spatialReference: {srsString: 'EPSG:4326'},
        }) as RasterLayerMetadata;

        TestBed.configureTestingModule({
            providers: [
                {provide: EOdysseyLayerService, useValue: {mapTileLayer: signal({dataConnectorId: 'provider', layerId: 'layer'})}},
                {
                    provide: LayersService,
                    useValue: {
                        registerAndGetLayerProcessingGraphId: vi.fn().mockResolvedValue('graph'),
                        getProcessingGraphIdMetadata: vi.fn().mockResolvedValue(metadata),
                    },
                },
                {provide: ProcessingGraphsService, useValue: {getFootprints}},
                {
                    provide: ProjectService,
                    useValue: {
                        getSpatialReferenceStream: (): Observable<unknown> => of({srsString: 'EPSG:3857'}),
                        getTimeStream: (): Observable<Time> => of(new Time(Date.UTC(2026, 3, 1))),
                        getTimeStepDurationStream: (): Observable<unknown> => of({durationAmount: 1, durationUnit: 'days'}),
                    },
                },
                {
                    provide: MapService,
                    useValue: {
                        getViewportSizeStream: (): Observable<unknown> => new BehaviorSubject({extent: [0, 0, 1, 1], resolution: 1}),
                    },
                },
                {
                    provide: SpatialReferenceService,
                    useValue: {
                        getSpatialReferenceSpecification: (): Observable<unknown> => of({}),
                        clipBoundsIfAvailable: (extent: number[]): number[] => extent,
                        reprojectExtent: (extent: number[]): number[] => extent,
                    },
                },
            ],
        });
    });

    afterEach(() => TestBed.resetTestingModule());

    /** The viewport is debounced, then the requests of the resources run. */
    async function settle(): Promise<void> {
        for (let i = 0; i < 4; i++) {
            TestBed.tick();
            await new Promise((resolve) => setTimeout(resolve, 300));
        }
    }

    const boxesRequests = (): number =>
        getFootprints.mock.calls.filter(([, query]) => query.precision === RasterFootprintsPrecision.Boxes).length;

    it('asks for the boxes of its step only where the map loads tiles', async () => {
        const service = TestBed.inject(FootprintsService);

        service.zoomRequired.set(true);
        service.overlayVisible.set(true);
        await settle();
        expect(boxesRequests()).toBe(0);

        service.zoomRequired.set(false);
        await settle();
        expect(boxesRequests()).toBe(1);
        expect(getFootprints).toHaveBeenCalledWith('graph', expect.objectContaining({limit: 1}), expect.anything());
    });

    it('asks the calendar for availability only', async () => {
        const service = TestBed.inject(FootprintsService);

        service.overlayVisible.set(true);
        await settle();

        const windowRequests = getFootprints.mock.calls.filter(([, query]) => query.limit === 100);
        expect(windowRequests.length).toBeGreaterThan(0);
        expect(windowRequests.every(([, query]) => query.precision === RasterFootprintsPrecision.Availability)).toBe(true);
    });
});
