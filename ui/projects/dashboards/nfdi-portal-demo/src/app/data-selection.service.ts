import {Injectable, inject} from '@angular/core';
import {ProjectService, LoadingState} from '@geoengine/core';
import {first, map, mergeMap, tap} from 'rxjs/operators';
import {BehaviorSubject, combineLatest, Observable, of} from 'rxjs';
import moment from 'moment';
import {Layer, RasterLayer, Time, VectorLayer} from '@geoengine/common';

export interface DataRange {
    min: number;
    max: number;
}

@Injectable({
    providedIn: 'root',
})
export class DataSelectionService {
    private readonly projectService = inject(ProjectService);

    readonly layers: Observable<Array<Layer>>;

    readonly rasterLayer = new BehaviorSubject<RasterLayer | undefined>(undefined);
    readonly intensityLayer = new BehaviorSubject<RasterLayer | undefined>(undefined);
    readonly speciesLayer1 = new BehaviorSubject<VectorLayer | undefined>(undefined);
    readonly speciesLayer2 = new BehaviorSubject<VectorLayer | undefined>(undefined);

    readonly species1LoadingState$: Observable<'query' | 'determinate'>;
    readonly species2LoadingState$: Observable<'query' | 'determinate'>;

    readonly timeSteps = new BehaviorSubject<Array<Time>>([new Time(moment.utc())]);
    readonly timeFormat = new BehaviorSubject<string>('YYYY');

    readonly dataRange = new BehaviorSubject<DataRange>({min: 0, max: 1});

    constructor() {
        this.layers = combineLatest([this.rasterLayer, this.intensityLayer, this.speciesLayer2, this.speciesLayer1]).pipe(
            map(([rasterLayer, intensityLayer, speciesLayer2, speciesLayer1]) => {
                const layers = [];
                if (rasterLayer) {
                    layers.push(rasterLayer);
                }
                if (intensityLayer) {
                    layers.push(intensityLayer);
                }
                if (speciesLayer2) {
                    layers.push(speciesLayer2);
                }
                if (speciesLayer1) {
                    layers.push(speciesLayer1);
                }
                return layers;
            }),
        );

        this.species1LoadingState$ = this.speciesLayer1.pipe(
            mergeMap((layer) => {
                if (layer) {
                    return this.projectService.getLayerStatusStream(layer);
                } else {
                    return of(LoadingState.OK);
                }
            }),
            map((status) => (status === LoadingState.LOADING ? 'query' : 'determinate')),
        );

        this.species2LoadingState$ = this.speciesLayer2.pipe(
            mergeMap((layer) => {
                if (layer) {
                    return this.projectService.getLayerStatusStream(layer);
                } else {
                    return of(LoadingState.OK);
                }
            }),
            map((status) => (status === LoadingState.LOADING ? 'query' : 'determinate')),
        );
    }

    async setTimeSteps(
        timeSteps: Array<Time>,
        preselectComparator?: (currentTime: Time, timeStep: Time) => boolean,
        defaultTime?: Time,
    ): Promise<void> {
        if (!timeSteps.length) {
            throw Error('`timeSteps` must not be empty');
        }

        const currentTime = await this.projectService.getTimeOnce();

        let newTime = timeSteps[0];

        if (defaultTime) {
            newTime = defaultTime;
        } else if (!preselectComparator) {
            newTime = timeSteps[0];
        } else {
            for (const timeStep of timeSteps) {
                if (preselectComparator(currentTime, timeStep)) {
                    newTime = timeStep;
                }
            }
        }

        await this.projectService.setTime(newTime);

        this.timeSteps.next(timeSteps);
    }

    setRasterLayer(layer: RasterLayer, dataRange: DataRange): Observable<void> {
        return this.rasterLayer.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this.rasterLayer.next(undefined)),
            mergeMap(() => this.projectService.addLayer(layer)),
            tap(() => {
                this.rasterLayer.next(layer);
                this.dataRange.next(dataRange);
            }),
        );
    }

    resetRasterLayer(): Observable<void> {
        return this.rasterLayer.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this.rasterLayer.next(undefined)),
        );
    }

    setIntensityLayer(layer?: RasterLayer): Observable<void> {
        const removeOperation = this.intensityLayer.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this.intensityLayer.next(undefined)),
        );

        if (!layer) {
            return removeOperation;
        }

        return removeOperation.pipe(
            mergeMap(() => this.projectService.addLayer(layer)),
            tap(() => {
                this.intensityLayer.next(layer);
            }),
        );
    }

    setSpecies1Layer(layer: VectorLayer): Observable<void> {
        return this.speciesLayer1.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this.speciesLayer1.next(undefined)),
            mergeMap(() => this.projectService.addLayer(layer)),
            tap(() => this.speciesLayer1.next(layer)),
        );
    }

    resetSpecies1Layer(): Observable<void> {
        return this.speciesLayer1.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this.speciesLayer1.next(undefined)),
        );
    }

    setSpecies2Layer(layer: VectorLayer): Observable<void> {
        return this.speciesLayer2.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this.speciesLayer2.next(undefined)),
            mergeMap(() => this.projectService.addLayer(layer)),
            tap(() => this.speciesLayer2.next(layer)),
        );
    }

    resetSpecies2Layer(): Observable<void> {
        return this.speciesLayer2.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this.speciesLayer2.next(undefined)),
        );
    }
}
