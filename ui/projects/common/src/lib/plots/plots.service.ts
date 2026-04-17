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

    async getPlot(
        id: UUID,
        bbox: BoundingBox2D,
        time: Time,
        spatialResolution: {x: number; y: number},
        crs?: SpatialReference,
    ): Promise<WrappedPlotOutput> {
        const plotApi = await firstValueFrom(this.plotApi);

        return plotApi.getPlotHandler({
            bbox: bboxDictToExtent(bbox).join(','),
            time: time.asRequestString(),
            spatialResolution: `${spatialResolution.x},${spatialResolution.y}`,
            id,
            crs: crs?.srsString,
        });
    }
}
