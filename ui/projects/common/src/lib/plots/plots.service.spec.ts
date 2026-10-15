import {TestBed} from '@angular/core/testing';
import {PlotsApi} from '@geoengine/api-client';
import {NEVER, Observable} from 'rxjs';
import {describe, expect, it, vi} from 'vitest';
import {UserService} from '../user/user.service';
import {Time} from '../time/time.model';
import {SpatialReference} from '../spatial-references/spatial-reference.model';
import {PlotsService} from './plots.service';

const BBOX = {lowerLeftCoordinate: {x: 0, y: 0}, upperRightCoordinate: {x: 10, y: 10}};
const TIME = new Time(Date.UTC(2014, 0, 1));
const CRS = SpatialReference.fromSrsString('EPSG:4326');

function setUp(): {service: PlotsService; getPlotHandler: ReturnType<typeof vi.fn>} {
    TestBed.configureTestingModule({
        providers: [{provide: UserService, useValue: {getSessionStream: (): Observable<never> => NEVER}}],
    });

    const service = TestBed.inject(PlotsService);

    const getPlotHandler = vi.fn().mockResolvedValue({});
    service.plotApi.next({getPlotHandler} as unknown as PlotsApi);

    return {service, getPlotHandler};
}

describe('PlotsService', () => {
    it('requests a plot at the native resolution if no resolution is given', async () => {
        const {service, getPlotHandler} = setUp();

        await service.getPlot('id', BBOX, TIME, undefined, CRS);

        expect(getPlotHandler).toHaveBeenCalledWith(
            {
                bbox: '0,0,10,10',
                time: '2014-01-01T00:00:00.000Z',
                spatialResolution: undefined,
                id: 'id',
                crs: 'EPSG:4326',
            },
            {signal: undefined},
        );
    });

    it('passes the resolution and the abort signal', async () => {
        const {service, getPlotHandler} = setUp();
        const abortController = new AbortController();

        await service.getPlot('id', BBOX, TIME, {x: 0.5, y: 0.25}, CRS, abortController.signal);

        expect(getPlotHandler).toHaveBeenCalledWith(expect.objectContaining({spatialResolution: '0.5,0.25'}), {
            signal: abortController.signal,
        });
    });
});
