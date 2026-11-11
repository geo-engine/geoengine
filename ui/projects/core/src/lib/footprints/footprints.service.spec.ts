import {TestBed} from '@angular/core/testing';
import {BehaviorSubject, Observable, of} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {RasterFootprints, RasterFootprintsPrecision, TimeStepFootprintStatus} from '@geoengine/api-client';
import {LayersService, ProcessingGraphsService, RasterLayerMetadata, Time} from '@geoengine/common';
import {FootprintsService} from './footprints.service';
import {MapService} from '../map/map.service';
import {ProjectService} from '../project/project.service';
import {SpatialReferenceService} from '../spatial-references/spatial-reference.service';

const FOOTPRINTS: RasterFootprints = {spatialReference: 'EPSG:4326', steps: [], next: null, tooLarge: false};

describe('FootprintsService overlay', () => {
    const registerLayer = vi.fn<(connector: string, layer: string) => Promise<string>>();
    const getFootprints =
        vi.fn<
            (
                id: string,
                query: {precision?: RasterFootprintsPrecision; limit?: number; spatialBounds?: string},
            ) => Promise<RasterFootprints>
        >();

    beforeEach(() => {
        getFootprints.mockReset().mockResolvedValue(FOOTPRINTS);
        registerLayer.mockReset().mockResolvedValue('graph');
        const metadata = Object.assign(Object.create(RasterLayerMetadata.prototype) as object, {
            spatialReference: {srsString: 'EPSG:4326'},
        }) as RasterLayerMetadata;

        TestBed.configureTestingModule({
            providers: [
                {
                    provide: LayersService,
                    useValue: {
                        registerAndGetLayerProcessingGraphId: registerLayer,
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

    /** The service as an app sets it up: with the layer to show. */
    function createService(connector = 'provider', layer = 'layer'): FootprintsService {
        const service = TestBed.inject(FootprintsService);
        service.layer.set({key: `${connector}/${layer}`, processingGraphId: () => registerLayer(connector, layer)});
        return service;
    }

    /** The calendar header opens the calendar and asks for the window of its month. */
    function openCalendar(service: FootprintsService): void {
        service.calendarOpen.set(true);
        service.showDate(new Date(2026, 3, 1));
    }

    /** The viewport is debounced, then the requests of the resources run. */
    async function settle(): Promise<void> {
        for (let i = 0; i < 4; i++) {
            TestBed.tick();
            await new Promise((resolve) => setTimeout(resolve, 300));
        }
    }

    const boxesRequests = (): number =>
        getFootprints.mock.calls.filter(([, query]) => query.precision === RasterFootprintsPrecision.Coverage).length;

    it('asks for the boxes of its step alone', async () => {
        const service = createService();

        service.overlayVisible.set(true);
        await settle();
        expect(boxesRequests()).toBe(1);
        expect(getFootprints).toHaveBeenCalledWith('graph', expect.objectContaining({limit: 1}), expect.anything());
    });

    it('asks the calendar for availability only', async () => {
        const service = createService();

        openCalendar(service);
        await settle();

        const windowRequests = getFootprints.mock.calls.filter(([, query]) => query.limit === 100);
        expect(windowRequests.length).toBeGreaterThan(0);
        expect(windowRequests.every(([, query]) => query.precision === RasterFootprintsPrecision.Availability)).toBe(true);
    });

    it('asks for the graph of the layer the app set', async () => {
        const service = createService('selected-provider', 'selected-layer');

        service.overlayVisible.set(true);
        await settle();

        expect(registerLayer).toHaveBeenCalledWith('selected-provider', 'selected-layer');
        expect(registerLayer).not.toHaveBeenCalledWith('provider', 'layer');
    });

    it('asks for the boxes of an area around the view, so scenes do not end at the edge of the view', async () => {
        const service = createService();

        service.overlayVisible.set(true);
        openCalendar(service);
        await settle();

        const bounds = (precision: RasterFootprintsPrecision): string | undefined =>
            getFootprints.mock.calls.find(([, query]) => query.precision === precision)?.[1].spatialBounds;
        expect(bounds(RasterFootprintsPrecision.Coverage)).toBe('-0.5,-0.5,1.5,1.5');
        expect(bounds(RasterFootprintsPrecision.Availability)).toBe('0,0,1,1');
    });

    it('asks for the graph of a layer once, however often the view changes', async () => {
        const service = createService();

        service.overlayVisible.set(true);
        openCalendar(service);
        await settle();
        await settle();

        expect(getFootprints.mock.calls.length).toBeGreaterThan(1);
        expect(registerLayer).toHaveBeenCalledTimes(1);
    });

    it('does not load the days of the calendar while only the overlay is on', async () => {
        const service = createService();

        service.overlayVisible.set(true);
        await settle();

        expect(boxesRequests()).toBe(1);
        expect(getFootprints.mock.calls.filter(([, query]) => query.limit === 100)).toHaveLength(0);
    });

    it('reports a failed request of the overlay instead of throwing', async () => {
        getFootprints.mockRejectedValue(new Error('server error'));
        const service = createService();

        service.overlayVisible.set(true);
        await settle();

        expect(service.hasError()).toBe(true);
        expect(() => service.tooLarge()).not.toThrow();
        expect(service.overlayAreaCount()).toBeUndefined();
    });

    it("uses the time step the app sets instead of the project's", () => {
        const service = createService();
        expect(service.timeStep()?.durationUnit).toBe('days');

        service.timeStepOverride.set({durationAmount: 1, durationUnit: 'months'});
        expect(service.timeStep()?.durationUnit).toBe('months');
    });

    it("asks for the boxes of the time the app sets instead of the project's", async () => {
        const service = createService();
        service.timeOverride.set(new Time(Date.UTC(2024, 5, 1)));

        service.overlayVisible.set(true);
        await settle();

        expect(getFootprints).toHaveBeenCalledWith(
            'graph',
            expect.objectContaining({timeInterval: new Time(Date.UTC(2024, 5, 1)).asRequestString()}),
            expect.anything(),
        );
    });

    describe('dates of the layer next to the calendar', () => {
        const day = (d: number, status: TimeStepFootprintStatus): RasterFootprints['steps'][number] => ({
            time: {start: Date.UTC(2026, 3, d), end: Date.UTC(2026, 3, d + 1)},
            status,
            bounds: [],
        });
        const steps = [
            day(-3, TimeStepFootprintStatus.Data),
            day(-1, TimeStepFootprintStatus.Empty),
            day(1, TimeStepFootprintStatus.Data),
            day(3, TimeStepFootprintStatus.Data),
            day(9, TimeStepFootprintStatus.Data),
        ];

        it('loads the window of the selected time without the calendar when the app needs the dates', async () => {
            const service = createService();
            expect(service.dateStatus()).toBeUndefined();

            service.datesNeeded.set(true);
            await settle();

            const windowRequests = getFootprints.mock.calls.filter(([, query]) => query.limit === 100);
            expect(windowRequests.length).toBeGreaterThan(0);
            expect(windowRequests.every(([, query]) => query.precision === RasterFootprintsPrecision.Availability)).toBe(true);
        });

        it('tells whether the selected time has data and finds the nearest dates with data around it', async () => {
            getFootprints.mockResolvedValueOnce({...FOOTPRINTS, steps}).mockResolvedValue(FOOTPRINTS);
            const service = createService();
            service.datesNeeded.set(true);
            await settle();

            expect(service.dateStatus()).toBe(TimeStepFootprintStatus.Data);
            expect(service.dateWithData(1)?.start.valueOf()).toBe(Date.UTC(2026, 3, 3));
            expect(service.dateWithData(-1)?.start.valueOf()).toBe(Date.UTC(2026, 3, -3));
        });

        it('finds no date with data beyond the loaded window', async () => {
            getFootprints
                .mockResolvedValueOnce({...FOOTPRINTS, steps: [day(1, TimeStepFootprintStatus.Data)]})
                .mockResolvedValue(FOOTPRINTS);
            const service = createService();
            service.datesNeeded.set(true);
            await settle();

            expect(service.dateWithData(1)).toBeUndefined();
            expect(service.dateWithData(-1)).toBeUndefined();
        });
    });
});
