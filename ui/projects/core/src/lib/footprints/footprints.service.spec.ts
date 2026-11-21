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
        expect(bounds(RasterFootprintsPrecision.Availability)).toBe('-0.05,-0.05,1.05,1.05');
    });

    it('asks no dates for a view moved inside the buffer of the asked one, but for a view leaving it', async () => {
        const datesRequests = (): number =>
            getFootprints.mock.calls.filter(([, query]) => query.precision === RasterFootprintsPrecision.Availability).length;
        const service = createService();
        service.datesNeeded.set(true);
        await settle();
        const asked = datesRequests();
        expect(asked).toBeGreaterThan(0);

        viewport.next({extent: [0.04, -0.04, 1.04, 0.96], resolution: 1});
        await settle();
        expect(datesRequests()).toBe(asked);

        viewport.next({extent: [0.1, 0, 1.1, 1], resolution: 1});
        await settle();
        expect(datesRequests()).toBeGreaterThan(asked);
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
        expect(service.overlayLayer.getOpacity()).toBe(1);

        // the boxes of the moved view do not arrive
        getFootprints.mockReturnValue(new Promise(() => undefined));
        viewport.next({extent: [0, 0, 2, 2], resolution: 1});
        await settle();

        expect(boxesRequests()).toBe(2);
        expect(boxesOnMap()).toBe(1);
        // faded, so they are not taken for the boxes of the moved view
        expect(service.overlayLayer.getOpacity()).toBeLessThan(1);
    });

    it('clears the boxes of a layer at once when another layer is set on the same date', async () => {
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

        // the boxes of the other layer do not arrive
        getFootprints.mockReturnValue(new Promise(() => undefined));
        service.layer.set({key: 'provider/other', processingGraphId: () => registerLayer('provider', 'other')});
        await settle();

        expect(boxesOnMap()).toBe(0);
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

    it('asks once when the app sets the same layer and time step again', async () => {
        const service = createService();
        service.overlayVisible.set(true);
        openCalendar(service);
        await settle();
        const requests = getFootprints.mock.calls.length;

        service.layer.set({key: 'provider/layer', processingGraphId: () => registerLayer('provider', 'layer')});
        service.timeStepOverride.set({durationAmount: 1, durationUnit: 'days'});
        await settle();
        expect(getFootprints.mock.calls.length).toBe(requests);
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
        expect(service.viewCoverage()).toBeUndefined();
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

        it('asks for the rest after the last step, so a step over the end of the first month is not loaded twice', async () => {
            // steps of ten days from the fifth of April, so 25 April to 5 May crosses the end of the
            // first request (the month); answered whole and without a cursor
            const TEN_DAYS = 10 * DAY;
            const origin = Date.UTC(2026, 3, 5);
            getFootprints.mockImplementation((_, query) => {
                const [start, end] = (query.timeInterval ?? '').split('/').map((time) => Date.parse(time));
                const steps = [];
                for (let time = origin + Math.floor((start - origin) / TEN_DAYS) * TEN_DAYS; time < end; time += TEN_DAYS) {
                    steps.push({time: {start: time, end: time + TEN_DAYS}, status: TimeStepFootprintStatus.Data, bounds: []});
                }
                return Promise.resolve({...FOOTPRINTS, steps});
            });
            const service = createService();
            service.timeStepOverride.set({durationAmount: 10, durationUnit: 'days'});
            service.datesNeeded.set(true);
            await settle();

            const starts = service.steps().map((step) => step.time.start);
            expect(starts.length).toBeGreaterThan(3);
            expect(new Set(starts).size).toBe(starts.length);
        });

        it('moves the window of dates with the layer, so a layer with longer steps has its steps before the time too', async () => {
            const service = createService();
            service.datesNeeded.set(true);
            await settle();
            expect(getFootprints.mock.calls[0][1].timeInterval?.split('/')[0]).toBe('2026-03-01T00:00:00.000Z');

            // another layer with yearly steps, on the same selected 1 April 2026
            getFootprints.mockClear();
            service.layer.set({key: 'provider/yearly', processingGraphId: () => registerLayer('provider', 'yearly')});
            service.timeStepOverride.set({durationAmount: 1, durationUnit: 'years'});
            await settle();
            // a first request with the old window may go out before the window moves; it is aborted
            const starts = getFootprints.mock.calls.map(([, query]) => query.timeInterval?.split('/')[0]);
            expect(starts).toContain('2019-04-01T00:00:00.000Z');
            expect(service.steps()).toEqual([]);
        });

        it('asks the month first for daily steps, but the whole window at once for steps of a month or more', async () => {
            const service = createService();
            service.datesNeeded.set(true);
            await settle();
            const end = (call: number): number => Date.parse((getFootprints.mock.calls[call][1].timeInterval ?? '').split('/')[1]);
            // the month of the start of the window, which begins a week before the selected 1 April
            expect(getFootprints.mock.calls[0][1].timeInterval?.split('/')[0]).toBe('2026-03-01T00:00:00.000Z');
            expect(end(0)).toBe(Date.UTC(2026, 3, 1));

            // a quarter fills the first month alone, which would show one long step in a strip of dates
            getFootprints.mockClear();
            service.timeStepOverride.set({durationAmount: 3, durationUnit: 'months'});
            await settle();
            expect(end(0)).toBeGreaterThan(Date.UTC(2026, 4, 1));
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

        /** The first request of the dates answers `steps`. The coverage of the selected date is asked for next to it. */
        function answerDates(dates: RasterFootprints['steps'], coverage: RasterFootprints = FOOTPRINTS): void {
            let isFirst = true;
            getFootprints.mockImplementation((_id, query) => {
                if (query.precision === RasterFootprintsPrecision.Coverage) return Promise.resolve(coverage);
                const answer = isFirst ? {...FOOTPRINTS, steps: dates} : FOOTPRINTS;
                isFirst = false;
                return Promise.resolve(answer);
            });
        }

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

        it('does not load the dates again when the calendar opens on dates the app shows already', async () => {
            answerDates(Array.from({length: 100}, (_, i) => day(1 + i, TimeStepFootprintStatus.Data)));
            const service = createService();
            service.datesNeeded.set(true);
            await settle();
            const requests = getFootprints.mock.calls.length;

            openCalendar(service);
            await settle();

            expect(getFootprints.mock.calls.length).toBe(requests);
        });

        it('offers only the dates with data in the calendar and finds the nearest ones around the selected time', async () => {
            answerDates(steps);
            const service = createService();
            service.datesNeeded.set(true);
            await settle();

            expect(service.dateFilter(new Date(2026, 3, 1))).toBe(true);
            expect(service.dateFilter(new Date(2026, 2, 30))).toBe(false);
            // not loaded, so it may have data
            expect(service.dateFilter(new Date(2026, 5, 1))).toBe(true);
            expect(service.dateWithData(1)?.start.valueOf()).toBe(Date.UTC(2026, 3, 3));
            expect(service.dateWithData(-1)?.start.valueOf()).toBe(Date.UTC(2026, 3, -3));
            // where to look on once no date with data is left in the loaded ones
            expect(service.dateBeyondLoaded(1)?.start.valueOf()).toBe(Date.UTC(2026, 3, 10));
            expect(service.dateBeyondLoaded(-1)?.start.valueOf()).toBe(Date.UTC(2026, 3, -4));
            expect(service.stepsAround(3).map((step) => step.time.start)).toEqual(steps.slice(1, 4).map((step) => step.time.start));
        });

        it('finds no date with data beyond the loaded window', async () => {
            answerDates([day(1, TimeStepFootprintStatus.Data)]);
            const service = createService();
            service.datesNeeded.set(true);
            await settle();

            expect(service.dateWithData(1)).toBeUndefined();
            expect(service.dateWithData(-1)).toBeUndefined();
            expect(service.dateBeyondLoaded(1)).toBeUndefined();
        });

        it('keeps the dates of the layer while those of a moved view load, but not those of another layer', async () => {
            answerDates(steps);
            const service = createService();
            service.datesNeeded.set(true);
            await settle();
            expect(service.steps()).toHaveLength(steps.length);
            expect(service.datesStale()).toBe(false);

            // the dates of the moved view do not arrive
            getFootprints.mockReturnValue(new Promise(() => undefined));
            viewport.next({extent: [0, 0, 2, 2], resolution: 1});
            await settle();
            expect(service.isLoading()).toBe(true);
            expect(service.steps()).toHaveLength(steps.length);
            expect(service.datesStale()).toBe(true);
            expect(service.freshSteps()).toEqual([]);

            service.layer.set({key: 'provider/other', processingGraphId: () => registerLayer('provider', 'other')});
            await settle();
            expect(service.steps()).toEqual([]);
            expect(service.datesStale()).toBe(false);
        });

        it('never shows the dates of the previous layer, also not right after the layer changes', async () => {
            // the dates come in two answers, so a partial answer exists during the load
            answerDates(steps);
            const service = createService();
            service.datesNeeded.set(true);
            await settle();
            expect(service.steps()).toHaveLength(steps.length);

            // read at once, as the template of an app does, before the load of the other layer starts
            getFootprints.mockReturnValue(new Promise(() => undefined));
            service.layer.set({key: 'provider/other', processingGraphId: () => registerLayer('provider', 'other')});
            service.timeStepOverride.set({durationAmount: 1, durationUnit: 'years'});
            expect(service.steps()).toEqual([]);
            await settle();
            expect(service.steps()).toEqual([]);
            expect(service.datesStale()).toBe(false);
        });

        it('says that the selected date has no data only from the dates of this view', async () => {
            answerDates([day(1, TimeStepFootprintStatus.Empty), day(2, TimeStepFootprintStatus.Data)]);
            const service = createService();
            service.datesNeeded.set(true);
            await settle();
            expect(service.selectedDateEmpty()).toBe(true);
            // shown, like in an app's strip of dates, so they are kept below
            expect(service.steps()).toHaveLength(2);

            // the dates of the moved view load: the kept ones of the earlier view decide nothing
            getFootprints.mockReturnValue(new Promise(() => undefined));
            viewport.next({extent: [0, 0, 2, 2], resolution: 1});
            await settle();
            expect(service.steps()).toHaveLength(2);
            expect(service.selectedDateEmpty()).toBe(false);
        });

        it('does not call a date empty that the layer cannot tell', async () => {
            answerDates([day(1, TimeStepFootprintStatus.Unknown)]);
            const service = createService();
            service.datesNeeded.set(true);
            await settle();
            expect(service.selectedDateEmpty()).toBe(false);
        });

        it('tells how much of the view the selected date covers while the overlay is on, and asks nothing while it is off', async () => {
            // one degree is far more than the view of one metre
            const box = {upperLeftCoordinate: {x: -1, y: 1}, lowerRightCoordinate: {x: 1, y: -1}};
            answerDates(steps, {...FOOTPRINTS, steps: [{...day(1, TimeStepFootprintStatus.Data), bounds: [box]}]});
            const service = createService();
            service.datesNeeded.set(true);
            await settle();
            expect(boxesRequests()).toBe(0);
            expect(service.viewCoverage()).toBeUndefined();

            service.overlayVisible.set(true);
            await settle();
            expect(service.viewCoverage()).toBe('full');

            // the answer stays while the one of a moved view loads
            getFootprints.mockReturnValue(new Promise(() => undefined));
            viewport.next({extent: [0, 0, 2, 2], resolution: 1});
            await settle();
            expect(service.overlayLoading()).toBe(true);
            expect(service.viewCoverage()).toBe('full');
        });
    });
});
