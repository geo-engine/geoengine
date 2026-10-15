import {Injectable, inject} from '@angular/core';
import {BoundingBox2D, PlotsApi, WrappedPlotOutput} from '@geoengine/api-client';
import {ReplaySubject, firstValueFrom} from 'rxjs';
import {UserService, apiConfigurationWithAccessKey} from '../user/user.service';
import {Time} from '../time/time.model';
import {UUID} from '../datasets/dataset.model';
import {bboxDictToExtent} from '../util/conversions';
import {SpatialReference} from '../spatial-references/spatial-reference.model';

@Injectable({
    providedIn: 'root',
})
export class PlotsService {
    private sessionService = inject(UserService);

    plotApi = new ReplaySubject<PlotsApi>(1);

    constructor() {
        this.sessionService.getSessionStream().subscribe({
            next: (session) => this.plotApi.next(new PlotsApi(apiConfigurationWithAccessKey(session.sessionToken))),
        });
    }

    /**
     * Computes a plot.
     *
     * If `spatialResolution` is set, raster inputs are read at a coarser overview level, which is faster but less exact.
     * Use this only where an approximation suffices, e.g., for deriving a colorizer.
     * Otherwise, raster inputs are read at their native resolution.
     *
     * Aborting `signal` cancels the request, which also stops the computation in the backend.
     */
    async getPlot(
        id: UUID,
        bbox: BoundingBox2D,
        time: Time,
        spatialResolution?: {x: number; y: number},
        crs?: SpatialReference,
        signal?: AbortSignal,
    ): Promise<WrappedPlotOutput> {
        const plotApi = await firstValueFrom(this.plotApi);

        return plotApi.getPlotHandler(
            {
                bbox: bboxDictToExtent(bbox).join(','),
                time: time.asRequestString(),
                spatialResolution: spatialResolution ? `${spatialResolution.x},${spatialResolution.y}` : undefined,
                id,
                crs: crs?.srsString,
            },
            {signal},
        );
    }
}
