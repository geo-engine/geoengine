import {Injectable, inject} from '@angular/core';
import {LoadingState, ProjectService} from '@geoengine/core';
import {first, map, mergeMap, tap} from 'rxjs/operators';
import {BehaviorSubject, combineLatest, forkJoin, Observable, of} from 'rxjs';
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

    readonly rasterLayer: Observable<RasterLayer | undefined>;
    readonly polygonLayer: Observable<VectorLayer | undefined>;

    readonly vectorLayer: Observable<VectorLayer | undefined>;

    readonly rasterLayerLoading: Observable<boolean>;
    readonly vectorLayerLoading: Observable<boolean>;

    readonly timeSteps = new BehaviorSubject<Array<Time>>([new Time(moment.utc())]);
    readonly timeFormat = new BehaviorSubject<string>('y N'); // TODO: make configurable

    readonly dataRange = new BehaviorSubject<DataRange>({min: 0, max: 1});

    protected readonly _rasterLayer = new BehaviorSubject<RasterLayer | undefined>(undefined);
    protected readonly _polygonLayer = new BehaviorSubject<VectorLayer | undefined>(undefined);

    protected readonly _vectorLayer = new BehaviorSubject<VectorLayer | undefined>(undefined);

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

        this.vectorLayer = this._vectorLayer.pipe(
            mergeMap((vectorLayer) =>
                vectorLayer ? (projectService.getLayerChangesStream(vectorLayer) as Observable<VectorLayer>) : of(undefined),
            ),
        );

        this.layers = combineLatest([this.rasterLayer, this.polygonLayer, this.vectorLayer]).pipe(
            map(([rasterLayer, polygonLayer, vectorLayer]) => {
                const layers = [];
                if (rasterLayer) {
                    layers.push(rasterLayer);
                }
                if (vectorLayer) {
                    layers.push(vectorLayer);
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

        this.vectorLayerLoading = this._vectorLayer.pipe(
            mergeMap((layer) => {
                if (!layer) {
                    return of(LoadingState.OK);
                }

                return projectService.getLayerStatusStream(layer);
            }),
            map((loadingState) => loadingState === LoadingState.LOADING),
        );
    }

    get hasSelectedLayer(): Observable<boolean> {
        return combineLatest([this.rasterLayer, this.vectorLayer]).pipe(
            map(([rasterLayer, vectorLayer]) => !!rasterLayer || !!vectorLayer),
        );
    }

    setRasterLayer(layer: RasterLayer, timeSteps: Array<Time>, dataRange: DataRange): Observable<void> {
        if (!timeSteps.length) {
            throw Error('`timeSteps` are required when setting a raster');
        }

        return forkJoin([this.unsetVectorLayer(), this.unsetRasterLayer()]).pipe(
            mergeMap(() => this.projectService.addLayer(layer)),
            tap(() => {
                this._rasterLayer.next(layer);
                this.timeSteps.next(timeSteps);
                this.projectService.setTime(timeSteps[0]);
                this.dataRange.next(dataRange);
            }),
        );
    }

    unsetRasterLayer(): Observable<void> {
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
        );
    }

    setVectorLayer(layer: VectorLayer, timeSteps: Array<Time>): Observable<void> {
        if (!timeSteps.length) {
            throw Error('`timeSteps` are required when setting a vector');
        }

        return forkJoin([this.unsetVectorLayer(), this.unsetRasterLayer(), this.unsetPolygonLayer()]).pipe(
            mergeMap(() => this.projectService.addLayer(layer)),
            tap(() => {
                this._vectorLayer.next(layer);
                this.timeSteps.next(timeSteps);
                this.projectService.setTime(timeSteps[0]);
            }),
        );
    }

    unsetVectorLayer(): Observable<void> {
        return this._vectorLayer.pipe(
            first(),
            mergeMap((currentLayer) => {
                if (currentLayer) {
                    return this.projectService.removeLayer(currentLayer);
                } else {
                    return of(undefined);
                }
            }),
            tap(() => this._vectorLayer.next(undefined)),
        );
    }

    setPolygonLayer(layer: VectorLayer): Observable<void> {
        return this.unsetPolygonLayer().pipe(
            mergeMap(() => this.projectService.addLayer(layer)),
            tap(() => this._polygonLayer.next(layer)),
        );
    }

    unsetPolygonLayer(): Observable<void> {
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
        );
    }
}
