import {Injectable, inject} from '@angular/core';
import {LoadingState, ProjectService} from '@geoengine/core';
import {first, map, mergeMap, tap} from 'rxjs/operators';
import {BehaviorSubject, combineLatest, Observable, of} from 'rxjs';
import moment from 'moment';
import {estimateTimeFormat, Layer, RasterLayer, Time, VectorLayer} from '@geoengine/common';

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

    readonly rasterLayer: Observable<RasterLayer | undefined>;
    readonly polygonLayer: Observable<VectorLayer | undefined>;

    readonly rasterLayerLoading: Observable<boolean>;

    readonly timeSteps = new BehaviorSubject<Array<Time>>([new Time(moment.utc())]);
    readonly timeFormat = new BehaviorSubject<string>('YYYY');

    readonly dataRange = new BehaviorSubject<DataRange>({min: 0, max: 1});

    protected readonly _rasterLayer = new BehaviorSubject<RasterLayer | undefined>(undefined);
    protected readonly _polygonLayer = new BehaviorSubject<VectorLayer | undefined>(undefined);

    constructor() {
        const projectService = this.projectService;

        this.rasterLayer = this._rasterLayer.pipe(
            mergeMap((rasterLayer) =>
                rasterLayer ? (projectService.getLayerChangesStream(rasterLayer) as Observable<RasterLayer>) : of(undefined),
            ),
        );
        this.polygonLayer = this._polygonLayer.pipe(
            mergeMap((polygonLayer) =>
                polygonLayer ? (projectService.getLayerChangesStream(polygonLayer) as Observable<VectorLayer>) : of(undefined),
            ),
        );

        this.layers = combineLatest([this.rasterLayer, this.polygonLayer]).pipe(
            map(([rasterLayer, polygonLayer]) => {
                const layers = [];
                if (rasterLayer) {
                    layers.push(rasterLayer);
                }
                if (polygonLayer) {
                    layers.push(polygonLayer);
                }

                return layers;
            }),
        );

        this.rasterLayerLoading = this._rasterLayer.pipe(
            mergeMap((layer) => {
                if (!layer) {
                    return of(LoadingState.OK);
                }

                return projectService.getLayerStatusStream(layer);
            }),
            map((loadingState) => loadingState === LoadingState.LOADING),
        );
    }

    setRasterLayer(layer: RasterLayer, timeSteps: Array<Time>, dataRange: DataRange): Observable<void> {
        if (!timeSteps.length) {
            throw Error('`timeSteps` are required when setting a raster');
        }

        return this._rasterLayer.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this._rasterLayer.next(undefined)),
            mergeMap(() => this.projectService.addLayer(layer)),
            tap(() => {
                this._rasterLayer.next(layer);
                this.timeSteps.next(timeSteps);
                this.timeFormat.next(estimateTimeFormat(timeSteps));
                this.projectService.setTime(timeSteps[0]);
                this.dataRange.next(dataRange);
            }),
        );
    }

    clearPolygonLayer(): Observable<void> {
        return this._polygonLayer.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
        );
    }

    setPolygonLayer(layer: VectorLayer): Observable<void> {
        return this._polygonLayer.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this._polygonLayer.next(undefined)),
            mergeMap(() => this.projectService.addLayer(layer)),
            tap(() => this._polygonLayer.next(layer)),
        );
    }
}
