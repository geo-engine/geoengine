import {TestBed} from '@angular/core/testing';
import {BehaviorSubject, Observable, of} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {RasterFootprints, RasterFootprintsPrecision, TimeStepFootprintStatus} from '@geoengine/api-client';
import {LayersService, ProcessingGraphsService, RasterLayerMetadata, Time} from '@geoengine/common';
import {FootprintsService} from './footprints.service';
import {MapService} from '../map/map.service';
import {ProjectService} from '../project/project.service';
import {SpatialReferenceService} from '../spatial-references/spatial-reference.service';

const FOOTPRINTS: RasterFootprints = {spatialReference: 'EPSG:4326', steps: [], next: null};

describe('FootprintsService overlay', () => {
    const registerLayer = vi.fn<(connector: string, layer: string) => Promise<string>>();
    const getFootprints =
        vi.fn<
            (
                id: string,
                query: {precision?: RasterFootprintsPrecision; limit?: number; spatialBounds?: string; timeInterval?: string},
            ) => Promise<RasterFootprints>
        >();

    let viewport: BehaviorSubject<unknown>;

    beforeEach(() => {
        viewport = new BehaviorSubject<unknown>({extent: [0, 0, 1, 1], resolution: 1});
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
                        getViewportSizeStream: (): Observable<unknown> => viewport,
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
        service.showDate(new Date(2026, 3, 1), 'month');
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

    it('asks for no boxes where the map does not load the tiles of the layer', async () => {
        const service = createService();
        service.overlayBlocked.set(true);
        service.overlayVisible.set(true);
        await settle();
        expect(boxesRequests()).toBe(0);

        service.overlayBlocked.set(false);
        await settle();
        expect(boxesRequests()).toBe(1);
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
        expect(bounds(RasterFootprintsPrecision.Coverage)).toBe('-0.25,-0.25,1.25,1.25');
        expect(bounds(RasterFootprintsPrecision.Availability)).toBe('0,0,1,1');
    });

    it('keeps the boxes of the date while those of a moved view load', async () => {
        const box = {upperLeftCoordinate: {x: 0, y: 1}, lowerRightCoordinate: {x: 1, y: 0}};
        getFootprints.mockResolvedValueOnce({
            ...FOOTPRINTS,
            steps: [{time: {start: Date.UTC(2026, 3, 1), end: Date.UTC(2026, 3, 2)}, status: TimeStepFootprintStatus.Data, bounds: [box]}],
        });
        const service = createService();
        const boxesOnMap = (): number => service.overlayLayer.getSource()?.getFeatures().length ?? 0;
        service.overlayVisible.set(true);
        await settle();
        expect(boxesOnMap()).toBe(1);

        // the boxes of the moved view do not arrive
        getFootprints.mockReturnValue(new Promise(() => undefined));
        viewport.next({extent: [0, 0, 2, 2], resolution: 1});
        await settle();

        expect(boxesRequests()).toBe(2);
        expect(boxesOnMap()).toBe(1);
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

    describe('views coarser than the time step', () => {
        const DAY = 24 * 60 * 60 * 1000;
        /** A server with data on every day, which answers `stepsPerAnswer` days and the cursor to the next one. */
        const answerDays =
            (stepsPerAnswer: number) =>
            (_: string, query: {timeInterval?: string}): Promise<RasterFootprints> => {
                const [start, end] = (query.timeInterval ?? '').split('/').map((time) => Date.parse(time));
                const answerEnd = Math.min(start + stepsPerAnswer * DAY, end);
                const steps = [];
                for (let time = start; time < answerEnd; time += DAY) {
                    steps.push({time: {start: time, end: time + DAY}, status: TimeStepFootprintStatus.Data, bounds: []});
                }
                return Promise.resolve({...FOOTPRINTS, steps, next: answerEnd < end ? {start: answerEnd, end: answerEnd + DAY} : null});
            };

        it('loads the whole year of daily data in the year view by following the cursor', async () => {
            getFootprints.mockImplementation(answerDays(100));
            const service = createService();
            service.calendarOpen.set(true);
            service.showDate(new Date(2026, 3, 1), 'year');
            await settle();

            // January, then four answers of 100 days
            expect(getFootprints).toHaveBeenCalledTimes(5);
            expect(service.dateClass(new Date(2026, 11, 1), 'year')).not.toEqual([]);
            expect(service.steps().length).toBe(365);
        });

        it('stops following the cursor after eight requests', async () => {
            getFootprints.mockImplementation(answerDays(1));
            const service = createService();
            service.calendarOpen.set(true);
            service.showDate(new Date(2026, 3, 1), 'year');
            await settle();

            expect(getFootprints).toHaveBeenCalledTimes(8);
        });

        it('asks nothing and marks nothing in the view of years for daily data', async () => {
            getFootprints.mockImplementation(answerDays(100));
            const service = createService();
            service.calendarOpen.set(true);
            service.showDate(new Date(2026, 3, 1), 'multi-year');
            await settle();

            expect(getFootprints).not.toHaveBeenCalled();
            expect(service.calendarHint('multi-year')).toBeDefined();

            // the steps of an earlier view do not mark their year alone
            service.showDate(new Date(2026, 3, 1), 'month');
            await settle();
            expect(service.dateClass(new Date(2026, 3, 1), 'month')).not.toEqual([]);
            expect(service.dateClass(new Date(2026, 0, 1), 'multi-year')).toEqual([]);
        });
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
            await settle();
            expect(getFootprints).not.toHaveBeenCalled();

            service.datesNeeded.set(true);
            await settle();

            const windowRequests = getFootprints.mock.calls.filter(([, query]) => query.limit === 100);
            expect(windowRequests.length).toBeGreaterThan(0);
            expect(windowRequests.every(([, query]) => query.precision === RasterFootprintsPrecision.Availability)).toBe(true);
        });

        it('offers only the dates with data in the calendar and finds the nearest ones around the selected time', async () => {
            getFootprints.mockResolvedValueOnce({...FOOTPRINTS, steps}).mockResolvedValue(FOOTPRINTS);
            const service = createService();
            service.datesNeeded.set(true);
            await settle();

            expect(service.dateFilter(new Date(2026, 3, 1))).toBe(true);
            expect(service.dateFilter(new Date(2026, 2, 30))).toBe(false);
            // not loaded, so it may have data
            expect(service.dateFilter(new Date(2026, 5, 1))).toBe(true);
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
