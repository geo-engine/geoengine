import {Injectable, inject} from '@angular/core';
import moment from 'moment';
import {mergeMap, Observable} from 'rxjs';
import {DatasetService, BackendService, ProjectService} from '@geoengine/core';
import {DataSelectionService} from './data-selection.service';
import {Dataset, RandomColorService, RasterLayer, RasterSymbology, Time, UserService} from '@geoengine/common';

@Injectable()
export class AppDatasetService extends DatasetService {
    protected readonly dataSelectionService = inject(DataSelectionService);

    constructor() {
        const backend = inject(BackendService);
        const userService = inject(UserService);
        const projectService = inject(ProjectService);
        const randomColorService = inject(RandomColorService);

        super();

        this.backend = backend;
        this.userService = userService;
        this.projectService = projectService;
        this.randomColorService = randomColorService;
    }

    override addDatasetToMap(dataset: Dataset): Observable<void> {
        const workflow = dataset.createSourceWorkflow();

        return this.projectService.registerWorkflow(workflow).pipe(
            mergeMap((workflowId) => {
                if (dataset.resultDescriptor.getTypeString() === 'Raster') {
                    const symbology = dataset.symbology as RasterSymbology;

                    const rasterLayer = new RasterLayer({
                        workflowId,
                        name: dataset.name,
                        symbology: symbology
                            ? symbology
                            : RasterSymbology.fromRasterSymbologyDict({
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
                        isLegendVisible: false,
                        isVisible: true,
                    });

                    // TODO: get from metadata
                    let time: Time;
                    if (dataset.name.includes('Pre-Industrial')) {
                        time = new Time(moment.utc('1750-01-01 00:00:00'));
                    } else if (dataset.name.includes('Early Holocene')) {
                        time = new Time(moment.utc('0001-01-01 00:00:00'));
                    } else {
                        time = new Time(moment.utc('2000-01-01 00:00:00'));
                    }

                    return this.dataSelectionService.setRasterLayer(
                        rasterLayer,
                        [time, time],
                        // TODO: get from metadata
                        {
                            min: -10,
                            max: 25,
                        },
                    );
                } else {
                    // TODO: other vector layers?
                    return new Observable<void>(function subscribe(subscriber) {
                        subscriber.error('Vector data is currently unsupported');
                        subscriber.complete();
                    });
                }
            }),
        );
    }
}
