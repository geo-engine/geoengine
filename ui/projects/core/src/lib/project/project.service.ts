import {
    BehaviorSubject,
    combineLatest,
    firstValueFrom,
    merge,
    Observable,
    Observer,
    of,
    ReplaySubject,
    Subject,
    Subscription,
    zip,
} from 'rxjs';
import {debounceTime, distinctUntilChanged, filter, first, map, mergeMap, skip, switchMap, take, tap} from 'rxjs/operators';

import {Injectable, OnDestroy, inject} from '@angular/core';

import {Project} from './project.model';
import {CoreConfig} from '../config.service';
import {LoadingState} from './loading-state.model';
import {HttpErrorResponse} from '@angular/common/http';
import {BackendService} from '../backend/backend.service';
import {BBoxDict, PlotDict, ProvenanceEntryDict, ToDict, UUID} from '../backend/backend.model';
import {Extent, MapService, ViewportSize} from '../map/map.service';
import {Session} from '../users/session.model';
import OlFeature from 'ol/Feature';
import OlGeometry from 'ol/geom/Geometry';
import {intersects as olIntersects} from 'ol/extent';
import {getProjectionTarget} from '../util/spatial_reference';
import {SpatialReferenceService} from '../spatial-references/spatial-reference.service';
import {
    apiConfigurationWithAccessKey,
    bboxDictToExtent,
    ClusteredPointSymbology,
    extentToBboxDict,
    GeoEngineError,
    HasLayerId,
    HasLayerType,
    HasPlotId,
    isDefined,
    Layer,
    LayerData,
    LayerMetadata,
    LayersService,
    LineSimplificationDict,
    LineSymbology,
    NotificationService,
    Plot,
    PointSymbology,
    PolygonSymbology,
    RasterData,
    RasterLayer,
    RasterLayerMetadata,
    SpatialReference,
    SpatialReferenceSpecification,
    subscribeAndProvide,
    Symbology,
    SymbologyType,
    Time,
    TimeStepDuration,
    timeStepDurationToTimeStepDict,
    unixTimestampToIsoString,
    UserService,
    VectorColumnDataTypes,
    VectorData,
    VectorLayer,
    VectorLayerMetadata,
    WorkflowsService,
} from '@geoengine/common';
import {
    CollectionItem,
    GeoJson,
    OGCWFSApi,
    ProjectLayer as ProjectLayerDict,
    ProviderLayerId,
    LegacyTypedOperatorOperator,
    TypedResultDescriptor,
    Workflow as WorkflowDict,
} from '@geoengine/api-client';

export type FeatureId = string | number;

export interface FeatureSelection {
    feature?: FeatureId;
}

/***
 * The ProjectService is the main housekeeping component of geoengine.
 * All layers, plots, and provenance are registered with the ProjectService.
 */
@Injectable()
export class ProjectService implements OnDestroy {
    protected config = inject(CoreConfig);
    protected notificationService = inject(NotificationService);
    protected mapService = inject(MapService);
    protected backend = inject(BackendService);
    protected userService = inject(UserService);
    protected spatialReferenceService = inject(SpatialReferenceService);
    protected layersService = inject(LayersService);
    protected readonly workflowsService = inject(WorkflowsService);

    private project$ = new ReplaySubject<Project | undefined>(1);

    private readonly layers = new Map<number, ReplaySubject<Layer>>();
    private readonly layerState$ = new Map<number, Observable<LoadingState>>();

    private readonly layerMetadata$ = new Map<number, ReplaySubject<LayerMetadata>>();
    private readonly layerMetadataState$ = new Map<number, ReplaySubject<LoadingState>>();

    private readonly layerData$ = new Map<number, ReplaySubject<LayerData | undefined>>();
    private readonly layerDataState$ = new Map<number, ReplaySubject<LoadingState>>();
    private readonly layerDataSubscriptions = new Map<number, Subscription>();

    private readonly newLayer$ = new Subject<void>();

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    private readonly plotData$ = new Map<number, ReplaySubject<any>>();
    private readonly plotError$ = new Map<number, ReplaySubject<GeoEngineError | undefined>>();
    private readonly plotDataState$ = new Map<number, ReplaySubject<LoadingState>>();
    private readonly plotDataSubscriptions = new Map<number, Subscription>();
    private readonly newPlot$ = new Subject<void>();

    private readonly selectedFeature$ = new BehaviorSubject<FeatureSelection>({feature: undefined});

    private readonly ogcWfsApi = new ReplaySubject<OGCWFSApi>(1);

    constructor() {
        const config = this.config;

        // TODO: currently the ProjectService also handles layer data.
        //       The temporary projects are a workaround to make dashboards work.
        //       Instead, the ProjectService should be refactored to only handle
        //       projects and extract the map layer handing.

        // set the starting project upon login
        this.userService
            .getSessionStream()
            .pipe(
                tap(() => this.project$.next(undefined)),
                mergeMap((session) =>
                    config.PROJECT.CREATE_TEMPORARY_PROJECT_AT_STARTUP
                        ? this.createTemporaryProject(session.sessionToken)
                        : firstValueFrom(this.loadMostRecentProject(session)),
                ),
            )
            .subscribe((project) => this.setProject(project));

        this.userService.getSessionTokenStream().subscribe({
            next: (sessionToken) => this.ogcWfsApi.next(new OGCWFSApi(apiConfigurationWithAccessKey(sessionToken))),
        });
    }

    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    async ngOnDestroy(): Promise<void> {
        if (this.config.PROJECT.CREATE_TEMPORARY_PROJECT_AT_STARTUP) {
            const session = await firstValueFrom(this.userService.getSessionTokenForRequest());
            await this.deleteCurrentProject(session);
        }
    }

    /**
     * Generate a default Project with values from the config file.
     */
    createDefaultProject(projectName?: string): Observable<Project> {
        const name = projectName ?? this.config.DEFAULTS.PROJECT.NAME;
        const timeConfig = this.config.DEFAULTS.PROJECT.TIME;
        let time: Time;
        if (typeof timeConfig === 'string') {
            time = new Time(timeConfig);

            if (!time.start.isValid()) {
                throw new Error(
                    // eslint-disable-next-line @typescript-eslint/restrict-plus-operands, @typescript-eslint/no-base-to-string
                    "Couldn't create default project because the configured start time is invalid:" + this.config.DEFAULTS.PROJECT.TIME,
                );
            }
        } else {
            const timeConfigDict = timeConfig;
            const isRange = this.config.TIME.ALLOW_RANGES && timeConfigDict.end;
            time = isRange ? new Time(timeConfigDict.start, timeConfigDict.end) : new Time(timeConfigDict.start, timeConfigDict.start);

            if (!time.start.isValid() || (isRange && (!time.end.isValid() || time.end.isBefore(time.start)))) {
                throw new Error(
                    // eslint-disable-next-line @typescript-eslint/restrict-plus-operands, @typescript-eslint/no-base-to-string
                    "Couldn't create default project because the configured time range is invalid:" + this.config.DEFAULTS.PROJECT.TIME,
                );
            }
        }

        const timeStepDuration = this.getDefaultTimeStep();

        // TODO: solidify default project creation

        return this.spatialReferenceService.getSpatialReferenceSpecification(this.config.DEFAULTS.PROJECT.PROJECTION).pipe(
            mergeMap((spec: SpatialReferenceSpecification) =>
                this.createProject({
                    name,
                    description: 'Default project',
                    spatialReference: spec.spatialReference,
                    bounds: extentToBboxDict(spec.extent),
                    time,
                    timeStepDuration,
                }),
            ),
        );
    }

    /**
     * Generate a default Project with values from the config file.
     */
    createProject(config: {
        name: string;
        description: string;
        spatialReference: SpatialReference;
        bounds: BBoxDict;
        time: Time;
        timeStepDuration: TimeStepDuration;
    }): Observable<Project> {
        return this.userService.getSessionTokenForRequest().pipe(
            mergeMap((sessionToken) =>
                this.backend.createProject(
                    {
                        name: config.name,
                        description: config.description,
                        bounds: {
                            boundingBox: config.bounds,
                            spatialReference: config.spatialReference.srsString,
                            timeInterval: config.time.toDict(),
                        },
                        timeStep: timeStepDurationToTimeStepDict(config.timeStepDuration),
                    },
                    sessionToken,
                ),
            ),
            map(
                ({id}) =>
                    new Project({
                        id,
                        name: config.name,
                        description: config.description,
                        spatialReference: config.spatialReference,
                        bbox: config.bounds,
                        layers: [],
                        plots: [],
                        time: config.time,
                        timeStepDuration: config.timeStepDuration,
                        version: {
                            // TODO
                            changed: new Date(),
                            id: '0',
                        },
                    }),
            ),
        );
    }

    cloneProject(newName: string): Observable<Project> {
        return this.getProjectOnce().pipe(
            mergeMap((project) =>
                combineLatest([
                    of(project),
                    this.createProject({
                        name: newName,
                        description: project.description,
                        spatialReference: project.spatialReference,
                        bounds: project._bbox,
                        time: project.time,
                        timeStepDuration: project.timeStepDuration,
                    }),
                ]),
            ),
            mergeMap(([oldProject, newPartialProject]) =>
                combineLatest([
                    of(
                        newPartialProject.updateFields({
                            layers: oldProject.layers,
                            plots: oldProject.plots,
                        }),
                    ),
                    this.userService.getSessionTokenForRequest(),
                ]),
            ),
            mergeMap(([project, sessionToken]) =>
                combineLatest([
                    of(project),
                    this.backend.updateProject(
                        {
                            id: project.id,
                            layers: project.layers.map((layer) => layer.toDict()),
                            // TODO: plots
                        },
                        sessionToken,
                    ),
                ]),
            ),
            map(([project, _]) => project),
        );
    }

    /**
     * Get a stream of Projects. This way compments can react to new Projects.
     */
    getProjectStream(): Observable<Project> {
        return this.project$.pipe(filter(isDefined));
    }

    /**
     * Get the current project and no further updates, e.g. for requests.
     */
    getProjectOnce(): Observable<Project> {
        return this.getProjectStream().pipe(first());
    }

    /**
     * Set a new Project. The ProjectService will clear all layer, plots, and provenance.
     * Does *not* store the project.
     */
    setProject(project: Project): void {
        // clear all subjects
        for (const subjectMap of [this.layers, this.layerData$, this.layerDataState$] as Array<Map<number, ReplaySubject<unknown>>>) {
            subjectMap.forEach((subject) => subject.complete());
            subjectMap.clear();
        }

        // clear all subscriptions
        for (const subscriptionMap of [this.layerDataSubscriptions]) {
            subscriptionMap.forEach((subscription) => subscription.unsubscribe());
            subscriptionMap.clear();
        }

        // add layer streams
        for (const layer of project.layers) {
            this.createLayerChangesStream(layer);
            this.createLayerMetadataStreams(layer);
            this.createLayerDataStreams(layer);
            this.createCombinedLoadingState(layer);
        }

        // add plot streams
        for (const plot of project.plots) {
            this.createPlotDataStreams(plot);
        }

        // propagate new project
        this.project$.next(project);

        // store current project in session
        this.userService.getSessionTokenForRequest().subscribe((sessionToken) => this.backend.setSessionProject(project.id, sessionToken));
    }

    /**
     * Set the time of the current project.
     */
    setTime(time: Time): Promise<void> {
        const result = this.getProjectOnce().pipe(
            map((project) => project.time),
            mergeMap((oldTime) => {
                if (time && time.isValid() && !time.isSame(oldTime)) {
                    return this.changeProjectConfig({time});
                } else {
                    return of(undefined);
                }
            }),
        );

        return firstValueFrom(result);
    }

    /**
     * Set a time duration for the current project.
     */
    setTimeStepDuration(timeStepDuration: TimeStepDuration): void {
        this.changeProjectConfig({timeStepDuration});
    }

    /**
     * Set the name of the current Project.
     */
    setName(name: string): Observable<void> {
        return this.changeProjectConfig({name});
    }

    /**
     * Set the projection used by the current project.
     */
    setSpatialReference(spatialReference: SpatialReference): Observable<void> {
        const result = this.getProjectOnce().pipe(
            mergeMap((project) => {
                let bbox = project._bbox;
                if (project.spatialReference.srsString !== spatialReference.srsString) {
                    bbox = this.spatialReferenceService.reprojectBbox(project._bbox, project.spatialReference, spatialReference);
                }
                return this.changeProjectConfig({spatialReference, bbox});
            }),
        );

        return subscribeAndProvide(result);
    }

    /**
     * Get a stream of the projects projection.
     */
    getSpatialReferenceStream(): Observable<SpatialReference> {
        return this.getProjectStream().pipe(
            map((project: Project) => project.spatialReference),
            distinctUntilChanged((x, y) => x.srsString === y.srsString),
        );
    }

    getSpatialReferenceOnce(): Observable<SpatialReference> {
        return this.getProjectStream().pipe(
            first(),
            map((project: Project) => project.spatialReference),
        );
    }

    /**
     * Get a stream of the projects time.
     */
    getTimeStream(): Observable<Time> {
        return this.getProjectStream().pipe(
            map((project) => project.time),
            distinctUntilChanged((t1, t2) => t1.isSame(t2)),
        );
    }

    getTimeOnce(): Promise<Time> {
        return firstValueFrom(
            this.getProjectStream().pipe(
                first(),
                map((project) => project.time),
            ),
        );
    }

    /**
     * Get a stream of the projects time step size.
     */
    getTimeStepDurationStream(): Observable<TimeStepDuration> {
        return this.getProjectStream().pipe(
            map((project) => project.timeStepDuration),
            distinctUntilChanged(),
        );
    }

    registerWorkflow(workflow: WorkflowDict): Observable<UUID> {
        return this.userService.getSessionTokenForRequest().pipe(
            mergeMap((sessionToken) => this.backend.registerWorkflow(workflow, sessionToken)),
            map((response) => response.id),
        );
    }

    getWorkflow(workflowId: UUID): Observable<WorkflowDict> {
        return this.userService
            .getSessionTokenForRequest()
            .pipe(mergeMap((sessionToken) => this.backend.getWorkflow(workflowId, sessionToken)));
    }

    getWorkflowMetaData(workflowId: UUID): Observable<TypedResultDescriptor> {
        return this.userService
            .getSessionTokenForRequest()
            .pipe(mergeMap((sessionToken) => this.backend.getWorkflowMetadata(workflowId, sessionToken)));
    }

    getWorkflowProvenance(workflowId: UUID): Observable<Array<ProvenanceEntryDict>> {
        return this.userService
            .getSessionTokenForRequest()
            .pipe(mergeMap((sessionToken) => this.backend.getWorkflowProvenance(workflowId, sessionToken)));
    }

    /**
     * Determines a common projection for all layers and return their operator with an added a propjection if necessary
     */
    getAutomaticallyProjectedOperatorsFromLayers(layers: Array<Layer>): Observable<Array<LegacyTypedOperatorOperator>> {
        const meta: Array<Observable<TypedResultDescriptor>> = layers.map((l) => this.getWorkflowMetaData(l.workflowId));

        return combineLatest(meta).pipe(
            mergeMap((descriptors: Array<TypedResultDescriptor>) => {
                const srefs = descriptors.map((l) => SpatialReference.fromSrsString(l.spatialReference));
                const targetSref = getProjectionTarget(srefs);

                const workflowsObservable = layers.map((l) => this.getWorkflow(l.workflowId));

                return combineLatest(workflowsObservable).pipe(
                    map((workflows: Array<WorkflowDict>) => {
                        const projectedOperators: Array<LegacyTypedOperatorOperator> = [];

                        for (let i = 0; i < workflows.length; i++) {
                            const sref: SpatialReference = srefs[i];
                            const workflow = workflows[i];
                            const operator: LegacyTypedOperatorOperator = workflow.operator;
                            if (sref.srsString === targetSref.srsString) {
                                projectedOperators.push(operator);
                            } else {
                                projectedOperators.push({
                                    type: 'Reprojection',
                                    params: {
                                        targetSpatialReference: targetSref.srsString,
                                    },
                                    sources: {
                                        source: operator,
                                    },
                                });
                            }
                        }

                        return projectedOperators;
                    }),
                );
            }),
        );
    }

    /**
     * Add a new layer to the project.
     */
    addLayer(layer: Layer, notify = true): Observable<void> {
        return this.addLayers([layer], notify);
    }

    /**
     * Add a set of new layers to the project.
     */
    addLayers(layers: Array<Layer>, notify = true): Observable<void> {
        layers.forEach((layer) => {
            this.createLayerChangesStream(layer);
            this.createLayerMetadataStreams(layer);
            this.createLayerDataStreams(layer);
            this.createCombinedLoadingState(layer);
        });

        const result = this.getProjectOnce().pipe(
            mergeMap((project) =>
                this.changeProjectConfig({
                    layers: [...layers].reverse().concat(project.layers),
                }),
            ),
            tap(() => {
                if (notify) {
                    this.newLayer$.next();
                }
            }),
        );

        return subscribeAndProvide(result);
    }

    /**
     * Add a plot to the project.
     */
    addPlot(plot: Plot, notify = true): Observable<void> {
        this.createPlotDataStreams(plot);

        const result = this.getProjectOnce().pipe(
            mergeMap((project) =>
                this.changeProjectConfig({
                    plots: [plot, ...project.plots],
                }),
            ),
            tap(() => {
                if (notify) {
                    this.newPlot$.next();
                }
            }),
        );

        return subscribeAndProvide(result);
    }

    /**
     * Reload the data of a layer.
     */
    reloadLayerData(layer: Layer): void {
        const layerData$ = this.layerData$.get(layer.id);
        const layerDataState$ = this.layerDataState$.get(layer.id);

        if (!layerData$ || !layerDataState$) {
            return;
        }

        layerData$.next(undefined); // send empty data

        if (this.layerDataSubscriptions.has(layer.id)) {
            this.layerDataSubscriptions.get(layer.id)?.unsubscribe();
            this.layerDataSubscriptions.delete(layer.id);
        }

        switch (layer.layerType) {
            case 'raster': {
                this.layerDataSubscriptions.set(
                    layer.id,
                    this.createRasterLayerDataSubscription(layer as RasterLayer, layerData$, layerDataState$),
                );
                break;
            }
            case 'vector': {
                this.layerDataSubscriptions.set(
                    layer.id,
                    this.createVectorLayerDataSubscription(layer as VectorLayer, layerData$, layerDataState$),
                );
                break;
            }
        }
    }

    /**
     * Reload everything for the layer manually (e.g. on error).
     */
    reloadLayer(layer: Layer): void {
        const layerMetadata$ = this.layerMetadata$.get(layer.id);
        const layerMetadataState$ = this.layerMetadataState$.get(layer.id);

        if (!layerMetadata$ || !layerMetadataState$) {
            return;
        }

        this.reloadLayerData(layer);
        this.retrieveLayerMetadata(layer, layerMetadata$, layerMetadataState$);
    }

    /**
     * Reload the data for the plot manually (e.g. on error).
     */
    reloadPlot(plot: Plot): void {
        const plotData$ = this.plotData$.get(plot.id);
        const plotError$ = this.plotError$.get(plot.id);
        const loadingState$ = this.plotDataState$.get(plot.id);

        if (!plotData$ || !plotError$ || !loadingState$) {
            return;
        }

        plotData$.next(undefined); // send empty data

        this.plotDataSubscriptions.get(plot.id)?.unsubscribe();
        this.plotDataSubscriptions.delete(plot.id);

        const subscription = this.createPlotSubscription(plot, plotData$, plotError$, loadingState$);

        this.plotDataSubscriptions.set(plot.id, subscription);
    }

    /**
     * Remove a plot from the project.
     */
    removePlot(plot: HasPlotId): Observable<void> {
        const result = this.getProjectOnce().pipe(
            mergeMap((project) => {
                const plots = [...project.plots];
                const plotIndex = plots.findIndex((p) => p.id === plot.id);
                if (plotIndex >= 0) {
                    plots.splice(plotIndex, 1);
                    return this.changeProjectConfig({
                        plots,
                    });
                } else {
                    // avoid request if there is nothing to do
                    return of<void>();
                }
            }),
            tap(() => this.removePlotSubscriptions(plot)),
        );

        return subscribeAndProvide(result);
    }

    /**
     * Retrieve the layer models array as a stream.
     */
    getLayerStream(): Observable<Array<Layer>> {
        return this.getProjectStream().pipe(
            map((project) => project.layers),
            distinctUntilChanged(),
        );
    }

    /**
     * Retrieve the plot models array as a stream.
     */
    getPlotStream(): Observable<Array<Plot>> {
        return this.getProjectStream().pipe(
            map((project) => project.plots),
            distinctUntilChanged(),
        );
    }

    /**
     * Notification stream of newly added plots
     */
    getNewPlotStream(): Observable<void> {
        return this.newPlot$;
    }

    /**
     * Notification stream of newly added layers
     */
    getNewLayerStream(): Observable<void> {
        return this.newLayer$;
    }

    getLayerMetadata(layer: Layer): Observable<LayerMetadata> {
        const metaData = this.layerMetadata$.get(layer.id);

        if (!metaData) {
            throw Error(`layer metadata for layer with id ${layer.id} is undefined`);
        }

        return metaData;
    }

    getVectorLayerMetadata(layer: VectorLayer): Observable<VectorLayerMetadata> {
        return this.getLayerMetadata(layer).pipe(map((metadata) => metadata as VectorLayerMetadata));
    }

    getRasterLayerMetadata(layer: RasterLayer): Observable<RasterLayerMetadata> {
        return this.getLayerMetadata(layer).pipe(map((metadata) => metadata as RasterLayerMetadata));
    }

    /**
     * Retrieve the data of the layer as a stream.
     */
    getLayerDataStream(layer: HasLayerId): Observable<LayerData | undefined> {
        // TODO: the plot type needs to be defined
        const data = this.layerData$.get(layer.id);

        if (!data) {
            throw Error(`layer data for layer with id ${layer.id} is undefined`);
        }

        return data;
    }

    /**
     * Retrieve the data of the plot as a stream.
     */
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    getPlotDataStream(plot: HasPlotId): Observable<any> {
        const data = this.plotData$.get(plot.id);

        if (!data) {
            throw Error(`plot data for plot with id ${plot.id} is undefined`);
        }

        return data;
    }

    /**
     * Retrieve the data of the plot as a stream.
     */
    getPlotErrorStream(plot: HasPlotId): Observable<GeoEngineError | undefined> {
        const error = this.plotError$.get(plot.id);

        if (!error) {
            throw Error(`plot data for plot with id ${plot.id} is undefined`);
        }

        return error;
    }

    /**
     * Retrieve the layer status as a stream.
     */
    getLayerStatusStream(layer: HasLayerId): Observable<LoadingState> {
        const status = this.layerState$.get(layer.id);

        if (!status) {
            throw Error(`status for id ${layer.id} is undefined`);
        }

        return status;
    }

    /**
     * Retrieve the layer data status as a stream.
     */
    getLayerDataStatusStream(layer: HasLayerId): Observable<LoadingState> {
        const status = this.layerDataState$.get(layer.id);

        if (!status) {
            throw Error(`status for id ${layer.id} is undefined`);
        }

        return status;
    }

    /**
     * Retrieve the plot data status as a stream.
     */
    getPlotDataStatusStream(plot: HasPlotId): Observable<LoadingState> {
        const status = this.plotDataState$.get(plot.id);

        if (!status) {
            throw Error(`status for id ${plot.id} is undefined`);
        }

        return status;
    }

    /**
     * Change the loading state of a raster layer
     */
    changeRasterLayerDataStatus(layer: HasLayerId & HasLayerType, state: LoadingState): void {
        if (layer.layerType === 'raster') {
            this.layerDataState$.get(layer.id)?.next(state);
        } else {
            throw Error('It is only allowed to change the state of a raster layer');
        }
    }

    /**
     * Removes a layer from the current project.
     */
    removeLayer(layer: Layer): Observable<void> {
        // TODO: un-select selected layer

        const result = this.getProjectOnce().pipe(
            mergeMap((project) => {
                const layers = project.layers.filter((l) => l.id !== layer.id);

                if (project.layers.length === layers.length) {
                    // nothing filtered, so no request
                    return of(undefined);
                }

                return this.changeProjectConfig({layers});
            }),
            tap(() => {
                this.removeLayerSubscriptions(layer);
                this.removeMetadataObservables(layer);
                this.layerState$.delete(layer.id);
            }),
        );

        return subscribeAndProvide(result);
    }

    /**
     * Remove all layers from the current project.
     */
    clearLayers(): Observable<void> {
        const result = this.getProjectOnce().pipe(
            mergeMap((project) => {
                const removedLayers: Array<Layer> = project.layers;

                return this.changeProjectConfig({
                    layers: [],
                }).pipe(map(() => removedLayers));
            }),
            map((removedLayers) => {
                removedLayers.forEach((layer) => {
                    this.removeLayerSubscriptions(layer);
                    this.removeMetadataObservables(layer);
                    this.layerState$.delete(layer.id);
                });

                return undefined;
            }),
        );

        return subscribeAndProvide(result);
    }

    /**
     * Remove all plots from the current project.
     */
    clearPlots(): Observable<void> {
        let removedPlots: Array<Plot>;

        const result = this.getProjectOnce().pipe(
            mergeMap((project) => {
                removedPlots = project.plots;

                return this.changeProjectConfig({
                    plots: [],
                });
            }),
            tap(() => removedPlots.forEach((plot) => this.removePlotSubscriptions(plot))),
        );

        return subscribeAndProvide(result);
    }

    /**
     * Sets the layers
     */
    setLayers(layers: Array<Layer>): void {
        this.getProjectStream()
            .pipe(first())
            .subscribe((project) => {
                if (project.layers !== layers) {
                    this.changeProjectConfig({layers});
                }
            });
    }

    changeLayer(
        layer: Layer,
        changes: {
            name?: string;
            workflowId?: UUID;
            symbology?: Symbology;
            isVisible?: boolean;
            isLegendVisible?: boolean;
        },
    ): Observable<void> {
        if (Object.keys(changes).length === 0) {
            return subscribeAndProvide(of(undefined));
        }

        layer = layer.updateFields(changes);

        const result = this.getProjectOnce().pipe(
            map((project) => project.layers.map((l) => (l.id === layer.id ? layer : l))),
            mergeMap((layers) => this.changeProjectConfig({layers})),
            tap(() => {
                // propagate layer changes
                this.layers.get(layer.id)?.next(layer);
            }),
        );

        return subscribeAndProvide(result);
    }

    /**
     * Get a stream of LayerChanges for a specified layer.
     */
    getLayerChangesStream(layer: Layer): Observable<Layer> {
        const changes = this.layers.get(layer.id);

        if (!changes) {
            throw new Error(`changes for id ${layer.id} are is undefined`);
        }

        return changes;
    }

    /**
     * Toggle the layer's legend visibility.
     */
    toggleLegend(layer: Layer): Observable<void> {
        return this.changeLayer(layer, {isLegendVisible: !layer.isLegendVisible});
    }

    // private static isNoRasterForGivenTimeException(response: HttpErrorResponse): boolean {
    //     if (!response.error || !response.error.nested_exception) {
    //         return false;
    //     }
    //     const nested_exception: { message: string, type: string } = response.error.nested_exception;
    //     return nested_exception.message.indexOf('NoRasterForGivenTimeException') >= 0;
    // }

    loadAndSetProject(projectId: UUID): Observable<Project> {
        const result = this.userService.getSessionTokenForRequest().pipe(
            mergeMap((sessionToken) => this.backend.loadProject(projectId, sessionToken)),
            map(Project.fromDict),
            tap((project) => this.setProject(project)),
        );

        return subscribeAndProvide(result);
    }

    /**
     * @returns The currently selected feature as stream.
     */
    getSelectedFeatureStream(): Observable<FeatureSelection> {
        return this.selectedFeature$.asObservable();
    }

    setSelectedFeature(feature?: OlFeature<OlGeometry>): void {
        this.selectedFeature$.next({feature: feature?.getId()});
    }

    getSelectedFeature(): FeatureSelection {
        return this.selectedFeature$.value;
    }

    /**
     * Create a stream that signals whether a running query should be aborted because the results are no longer needed.
     * It takes the layerId, current zoomLevel and extent of a tile at the time of querying as a parameter in order to
     * determine whether a change in the layer list or on the map view makes the results obsolete.
     */
    createQueryAbortStream(layerId: number, tileZoomLevel: number, tileExtent: Extent): Observable<void> {
        const tileResolution = this.mapService.getView().getResolutionForZoom(tileZoomLevel);

        // create an observable that emits when the layer is removed
        const layerStream = this.layers.get(layerId);
        if (!layerStream) {
            throw Error(`No layer stream found for layer id ${layerId}`);
        }
        const layerRemovedSubject = new BehaviorSubject<boolean>(false);
        const layerStreamSub = layerStream.subscribe({
            complete: () => {
                layerRemovedSubject.next(true);
                layerRemovedSubject.complete();
            },
        });

        const observables: [
            Observable<Time>,
            Observable<ViewportSize>,
            Observable<string>,
            Observable<SpatialReference>,
            Observable<boolean>,
        ] = [
            this.getTimeStream(),
            this.mapService.getViewportSizeStream(),
            this.userService.getSessionTokenForRequest(),
            this.getSpatialReferenceStream(),
            layerRemovedSubject,
        ];

        let initialTime: Time | undefined;
        let initialSref: SpatialReference | undefined;
        let initialSession: string | undefined;

        return combineLatest(observables).pipe(
            tap(([time, _viewportSize, session, sref, _layerRemoved]) => {
                // capture the initial values at the start of the query
                // s.t. we can detect a change later
                initialTime ??= time;
                initialSref ??= sref;
                initialSession ??= session;
            }),
            skip(1),
            filter(
                ([time, viewportSize, session, sref, layerRemoved]) =>
                    !time.isSame(initialTime!) ||
                    viewportSize.resolution !== tileResolution ||
                    !olIntersects(tileExtent, viewportSize.extent) ||
                    session !== initialSession ||
                    sref !== initialSref ||
                    layerRemoved,
            ),
            tap((_) => layerStreamSub.unsubscribe()),
            take(1),
            map(() => undefined),
        );
    }

    /**
     * Creates a projected operator if the layer has not the target spatial reference.
     */
    createProjectedOperator(
        inputOperator: LegacyTypedOperatorOperator,
        metadata: LayerMetadata,
        targetSpatialReference: SpatialReference,
    ): LegacyTypedOperatorOperator {
        if (metadata.spatialReference.equals(targetSpatialReference)) {
            return inputOperator;
        }

        return {
            type: 'Reprojection',
            params: {
                targetSpatialReference: targetSpatialReference.srsString,
            },
            sources: {
                source: inputOperator,
            },
        };
    }

    protected async createTemporaryProject(sessionToken: string): Promise<Project> {
        await this.deleteCurrentProject(sessionToken);

        return await firstValueFrom(this.createDefaultProject(crypto.randomUUID()));
    }

    private async deleteCurrentProject(sessionToken: string): Promise<void> {
        const oldProject = await firstValueFrom(merge(this.project$, of(undefined)).pipe(first()));

        if (oldProject) {
            try {
                await firstValueFrom(this.backend.deleteProject(oldProject.id, sessionToken));
            } catch (_error) {
                // could not delete the old project, maybe because the user changed
                // TODO: remove temporary projects and do not use the project servive for dashboards
            }
        }
    }

    protected loadMostRecentProject(session: Session): Observable<Project> {
        let projectIdLookup: Observable<UUID | undefined>;

        if (session.lastProjectId) {
            // use the project id from the session
            projectIdLookup = of(session.lastProjectId);
        } else {
            // try to find the least recently used project id
            projectIdLookup = this.backend
                .listProjects(
                    {
                        permissions: ['Owner'],
                        filter: 'None',
                        order: 'DateDesc',
                        offset: 0,
                        limit: 1,
                    },
                    session.sessionToken,
                )
                .pipe(
                    map((listings) => {
                        if (listings.length > 0) {
                            return listings[0].id;
                        } else {
                            return undefined;
                        }
                    }),
                );
        }

        return projectIdLookup.pipe(
            mergeMap((projectId) => {
                if (projectId) {
                    return this.backend.loadProject(projectId, session.sessionToken).pipe(map(Project.fromDict));
                } else {
                    return this.createDefaultProject();
                }
            }),
        );
    }

    protected removeLayerSubscriptions(layer: HasLayerId): void {
        // subjects
        for (const subjectMap of [this.layers, this.layerData$, this.layerDataState$]) {
            subjectMap.get(layer.id)?.complete();
            subjectMap.delete(layer.id);
        }

        // subscriptions
        for (const subscriptionMap of [this.layerDataSubscriptions]) {
            subscriptionMap.get(layer.id)?.unsubscribe();
            subscriptionMap.delete(layer.id);
        }
    }

    protected removeMetadataObservables(layer: HasLayerId): void {
        this.layerMetadata$.get(layer.id)?.complete();
        this.layerMetadata$.delete(layer.id);

        this.layerMetadataState$.get(layer.id)?.complete();
        this.layerMetadataState$.delete(layer.id);
    }

    protected removePlotSubscriptions(plot: HasPlotId): void {
        // subjects
        for (const subjectMap of [this.plotData$, this.plotError$, this.plotDataState$]) {
            subjectMap.get(plot.id)?.complete();
            subjectMap.delete(plot.id);
        }

        // subscriptions
        for (const subscriptionMap of [this.plotDataSubscriptions]) {
            subscriptionMap.get(plot.id)?.unsubscribe();
            subscriptionMap.delete(plot.id);
        }
    }

    protected static optimizeVecUpdates<Content extends ToDict<ContentDict> & {equals(other: Content): boolean}, ContentDict>(
        oldLayers: Array<Content>,
        newLayers: Array<Content>,
    ): Array<ContentDict | 'none' | 'delete'> {
        return newLayers.map((layer, i) => (layer.equals(oldLayers[i]) ? 'none' : layer.toDict()));

        // TODO: optimize deletions, etc.
    }

    protected changeProjectConfig(changes: {
        id?: UUID;
        name?: string;
        spatialReference?: SpatialReference;
        bbox?: BBoxDict;
        time?: Time;
        plots?: Array<Plot>;
        layers?: Array<Layer>;
        timeStepDuration?: TimeStepDuration;
    }): Observable<void> {
        // don't request the server if there are no changes
        if (Object.keys(changes).length === 0) {
            return subscribeAndProvide(of(undefined));
        }

        const result = combineLatest([this.getProjectOnce(), this.userService.getSessionTokenForRequest()]).pipe(
            mergeMap(([oldProject, sessionToken]) => {
                const project: Project = oldProject.updateFields(changes);

                return this.backend
                    .updateProject(
                        {
                            id: project.id,
                            name: changes.name,
                            layers: changes.layers
                                ? ProjectService.optimizeVecUpdates<Layer, ProjectLayerDict>(oldProject.layers, project.layers)
                                : undefined,
                            plots: changes.plots
                                ? ProjectService.optimizeVecUpdates<Plot, PlotDict>(oldProject.plots, project.plots)
                                : undefined,
                            bounds: changes.time || changes.spatialReference || changes.bbox ? project.toBoundsDict() : undefined,
                            // TODO: description: changes.description,
                            timeStep: changes.timeStepDuration ? timeStepDurationToTimeStepDict(changes.timeStepDuration) : undefined,
                        },
                        sessionToken,
                    )
                    .pipe(map(() => project));
            }),
            map((project) => {
                this.project$.next(project);

                return undefined;
            }),
        );

        return subscribeAndProvide(result);
    }

    private createCombinedLoadingState(layer: HasLayerId): void {
        const layerMetadataState$ = this.layerMetadataState$.get(layer.id);
        const layerDataState$ = this.layerDataState$.get(layer.id);

        if (!layerMetadataState$ || !layerDataState$) {
            throw Error(`undefined states for layer ${layer.id}`);
        }

        const loadingState$ = combineLatest([layerMetadataState$, layerDataState$]).pipe(
            map((loadingStates) => {
                if (loadingStates.includes(LoadingState.LOADING)) {
                    return LoadingState.LOADING;
                }

                if (loadingStates.includes(LoadingState.ERROR)) {
                    return LoadingState.ERROR;
                }

                if (loadingStates.includes(LoadingState.NODATAFORGIVENTIME)) {
                    return LoadingState.NODATAFORGIVENTIME;
                }

                return LoadingState.OK;
            }),
        );
        this.layerState$.set(layer.id, loadingState$);
    }

    private createLayerDataStreams(layer: Layer): void {
        // each layer has data. The type depends on the layer type
        const layerDataLoadingState$ = new ReplaySubject<LoadingState>(1);
        const layerData$ = new ReplaySubject<LayerData | undefined>(1);
        let layerDataSub: Subscription;
        switch (layer.layerType) {
            case 'raster':
                layerDataSub = this.createRasterLayerDataSubscription(layer as RasterLayer, layerData$, layerDataLoadingState$);
                break;
            case 'vector':
                layerDataSub = this.createVectorLayerDataSubscription(layer as VectorLayer, layerData$, layerDataLoadingState$);
                break;
        }
        this.layerDataSubscriptions.set(layer.id, layerDataSub);
        this.layerDataState$.set(layer.id, layerDataLoadingState$);
        this.layerData$.set(layer.id, layerData$);
    }

    private createLayerMetadataStreams(layer: Layer): void {
        const layerMetadataLoadingState$ = new ReplaySubject<LoadingState>(1);
        const layerMetadata$ = new ReplaySubject<LayerMetadata>(1);

        this.retrieveLayerMetadata(layer, layerMetadata$, layerMetadataLoadingState$);

        this.layerMetadata$.set(layer.id, layerMetadata$);
        this.layerMetadataState$.set(layer.id, layerMetadataLoadingState$);
    }

    /**
     * Create a subscription for layer data, symbology and provenance with loading state checks and error handling
     */
    private createRasterLayerDataSubscription(
        layer: RasterLayer,
        data$: Observer<RasterData>,
        loadingState$: Observer<LoadingState>,
    ): Subscription {
        return combineLatest([this.getTimeStream(), this.getSpatialReferenceStream()])
            .pipe(
                tap(() => loadingState$.next(LoadingState.LOADING)),
                map(
                    ([time, projection]) =>
                        new RasterData(
                            time,
                            projection,
                            // this.mappingQueryService.getWMSQueryUrl({
                            //     operator: layer.operator,
                            //     time,
                            //     projection,
                            // })
                            this.backend.wmsBaseUrl,
                        ),
                ),
                tap({
                    next: () => loadingState$.next(LoadingState.OK),
                    error: (reason: HttpErrorResponse) => {
                        // if (ProjectService.isNoRasterForGivenTimeException(reason)) {
                        //     this.notificationService.error(`${layer.name}: No Raster for the given Time`);
                        //     loadingState$.next(LoadingState.NODATAFORGIVENTIME);
                        // } else {
                        this.notificationService.error(`${layer.name}: ${reason.status} ${reason.statusText}`);
                        loadingState$.next(LoadingState.ERROR);
                        // }
                    },
                }),
            )
            .subscribe({
                next: (data) => data$.next(data),
                // eslint-disable-next-line @typescript-eslint/no-unsafe-return
                error: (error) => error, // ignore error
            });
    }

    /**
     * Retrieve metadata for layer data
     */
    private retrieveLayerMetadata(layer: Layer, metadata$: Observer<LayerMetadata>, loadingState$: Observer<LoadingState>): void {
        this.userService
            .getSessionTokenForRequest()
            .pipe(
                tap(() => loadingState$.next(LoadingState.LOADING)),
                mergeMap((sessionToken) => this.backend.getWorkflowMetadata(layer.workflowId, sessionToken)),
                map((workflowMetadataDict) => LayerMetadata.fromDict(workflowMetadataDict)),
                tap({
                    next: () => loadingState$.next(LoadingState.OK),
                    error: (reason: Response) => {
                        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-base-to-string
                        this.notificationService.error(`${layer.name}: ${reason}`);
                        loadingState$.next(LoadingState.ERROR);
                    },
                }),
            )
            .subscribe({
                next: (metadata) => metadata$.next(metadata),
                error: (error) => error, // ignore error
            });
    }

    /**
     * Create a subscription for layer data, symbology and provenance with loading state checks and error handling
     */
    private createVectorLayerDataSubscription(
        layer: VectorLayer,
        data$: Observer<VectorData>,
        loadingState$: Observer<LoadingState>,
    ): Subscription {
        const workflowIdCache = new Map<[SpatialReference, number /* resolution */], UUID>();

        const isClusteredOrSimplified$ = this.getLayerChangesStream(layer).pipe(
            map((changedLayer) => {
                if (changedLayer.symbology instanceof ClusteredPointSymbology) {
                    return true;
                }
                if (changedLayer.symbology instanceof LineSymbology || changedLayer.symbology instanceof PolygonSymbology) {
                    return changedLayer.symbology.autoSimplified;
                }
                return false;
            }),
            distinctUntilChanged(),
        );

        return combineLatest({
            time: this.getTimeStream(),
            spatialReference: this.getSpatialReferenceStream(),
            viewport: this.mapService.getViewportSizeStream(),
            isClusteredOrSimplified: isClusteredOrSimplified$,
            workflowMetadata: this.getVectorLayerMetadata(layer),
            originalWorkflow: this.workflowsService.getWorkflow(layer.workflowId), // TODO: capture possible changes to the layers's workflow?
        })
            .pipe(
                debounceTime(this.config.DELAYS.DEBOUNCE),
                tap(() => loadingState$.next(LoadingState.LOADING)),
                switchMap(async ({time, spatialReference, viewport, isClusteredOrSimplified, workflowMetadata, originalWorkflow}) => {
                    let workflowId: UUID;
                    const cacheEntry = workflowIdCache.get([spatialReference, viewport.resolution]);
                    if (cacheEntry) {
                        workflowId = cacheEntry;
                    } else if (isClusteredOrSimplified) {
                        let workflow;
                        switch (layer.symbology.getSymbologyType()) {
                            case SymbologyType.POINT: {
                                workflow = createClusteredPointLayerQueryWorkflow(
                                    originalWorkflow,
                                    workflowMetadata,
                                    spatialReference,
                                    viewport.resolution,
                                );
                                break;
                            }
                            case SymbologyType.LINE:
                            case SymbologyType.POLYGON: {
                                workflow = createSimplifiedLinesOrPolygonsLayerQueryWorkflow(
                                    originalWorkflow,
                                    workflowMetadata,
                                    spatialReference,
                                    viewport.resolution,
                                );
                                break;
                            }
                            default:
                                throw new Error(`Unsupported symbology type for simplification: ${layer.symbology.getSymbologyType()}`);
                        }

                        workflowId = await this.workflowsService.registerWorkflow(workflow);

                        workflowIdCache.set([spatialReference, viewport.resolution], workflowId);
                    } else {
                        // no need to change the workflow
                        workflowId = layer.workflowId;
                    }

                    const ogcWfsApi = await firstValueFrom(this.ogcWfsApi);

                    const wfsResponse = await ogcWfsApi.wfsHandler({
                        service: 'WFS',
                        version: '2.0.0',
                        request: 'GetFeature',
                        workflow: workflowId,
                        typeNames: workflowId,
                        bbox: bboxDictToExtent(extentToBboxDict(viewport.extent)).join(','),
                        time: `${unixTimestampToIsoString(time.toDict().start)}/${unixTimestampToIsoString(time.toDict().end)}`,
                        srsName: spatialReference.srsString,
                    });
                    addTimeToProperties(wfsResponse);

                    const requestExtent: [number, number, number, number] = [0, 0, 0, 0]; // TODO: why is this empty?
                    return VectorData.olParse(time, spatialReference, requestExtent, wfsResponse);
                }),
                tap({
                    next: () => loadingState$.next(LoadingState.OK),
                    error: (reason: Response) => {
                        this.notificationService.error(`${layer.name}: ${reason.statusText}`);
                        loadingState$.next(LoadingState.ERROR);
                    },
                }),
            )
            .subscribe({
                next: (data) => data$.next(data),
                error: (_error) => /* ignore error */ undefined,
            });
    }

    private createLayerChangesStream(layer: Layer): void {
        if (this.layers.get(layer.id)) {
            throw new Error('Layer changes stream already registered');
        }

        this.layers.set(layer.id, new ReplaySubject<Layer>(1));

        // emit first change
        this.layers.get(layer.id)?.next(layer);
    }

    private getDefaultTimeStep(): TimeStepDuration {
        switch (this.config.DEFAULTS.PROJECT.TIMESTEP) {
            case '15 minutes':
                return {durationAmount: 15, durationUnit: 'minutes'};
            case '1 hour':
                return {durationAmount: 1, durationUnit: 'hour'};
            case '1 day':
                return {durationAmount: 1, durationUnit: 'day'};
            case '1 month':
                return {durationAmount: 1, durationUnit: 'month'};
            case '6 months':
                return {durationAmount: 6, durationUnit: 'months'};
            case '1 year':
                return {durationAmount: 1, durationUnit: 'year'};
            default:
                return {durationAmount: 1, durationUnit: 'month'};
        }
    }

    private createPlotDataStreams(plot: Plot): void {
        const loadingState$ = new ReplaySubject<LoadingState>(1);
        const data$ = new ReplaySubject<unknown>(1);
        const error$ = new ReplaySubject<GeoEngineError | undefined>(1);

        const subscription = this.createPlotSubscription(plot, data$, error$, loadingState$);
        this.plotDataSubscriptions.set(plot.id, subscription);

        this.plotDataState$.set(plot.id, loadingState$);
        this.plotData$.set(plot.id, data$);
        this.plotError$.set(plot.id, error$);
    }

    /**
     * Create a subscription for plot data with loading state checks and error handling
     */
    private createPlotSubscription(
        plot: Plot,
        data$: Observer<unknown>,
        error$: Observer<GeoEngineError | undefined>,
        loadingState$: Observer<LoadingState>,
    ): Subscription {
        const observables: [Observable<Time>, Observable<ViewportSize>, Observable<string>, Observable<SpatialReference>] = [
            this.getTimeStream(),
            this.mapService.getViewportSizeStream(),
            this.userService.getSessionTokenForRequest(),
            this.getSpatialReferenceStream(),
        ];

        return combineLatest(observables)
            .pipe(
                debounceTime(this.config.DELAYS.DEBOUNCE),
                tap(() => loadingState$.next(LoadingState.LOADING)),
                switchMap(([time, viewport, sessionToken, sref]) =>
                    // TODO: add image size for png

                    this.backend.getPlot(
                        plot.workflowId,
                        {
                            time: time.toDict(),
                            bbox: extentToBboxDict(viewport.extent),
                            crs: sref.srsString,
                            spatialResolution: [viewport.resolution, viewport.resolution], // TODO: check if resolution needs two numbers
                        },
                        sessionToken,
                    ),
                ),
                tap({
                    next: () => loadingState$.next(LoadingState.OK),
                    error: (errorResponse: HttpErrorResponse) => {
                        const errorDict = errorResponse.error;
                        let error: GeoEngineError | undefined;
                        let errorMessage: string;

                        if (
                            'error' in errorDict &&
                            'message' in errorDict &&
                            typeof errorDict['error'] === 'string' &&
                            typeof errorDict['message'] === 'string'
                        ) {
                            error = GeoEngineError.fromDict(errorDict as {error: string; message: string});
                            errorMessage = `${error.name} ${error.message}`;
                        } else {
                            // fallback error notice
                            errorMessage = `${errorResponse.status} ${errorResponse.statusText}`;
                        }

                        this.notificationService.error(`${plot.name}: ${errorMessage}`);

                        error$.next(error);
                        loadingState$.next(LoadingState.ERROR);
                    },
                }),
            )
            .subscribe({
                next: (data) => data$.next(data),
                error: (error) => error, // ignore error
            });
    }

    /**
     * Add all layers (directly) contained in a layer collection to the current project.
     */
    addCollectionLayersToProject(collectionItems: Array<CollectionItem>): Observable<void> {
        const layersObservable = collectionItems
            .filter((layer) => layer.type === 'layer')
            .map((layer) => layer)
            .map((layer) => this.layersService.resolveLayer(layer.id));

        // TODO: lookup in parallel
        return subscribeAndProvide(zip(layersObservable).pipe(mergeMap((layers) => this.addLayers(layers))));
    }

    /**
     * Add a layer to the current project.
     */
    async addLayerbyId(layerId: ProviderLayerId): Promise<void> {
        const layer = await this.layersService.resolveLayer(layerId);
        this.addLayer(layer);
    }
}

function addTimeToProperties(x: GeoJson): void {
    x['features'].forEach((element: Record<string, Record<string, string>>) => {
        const start: string = element['when']['start'];
        const end: string = element['when']['end'];
        element['properties']['_____table__start'] = start;
        element['properties']['_____table__end'] = end;
    });
}

/**
 * In order to visually cluster points depending on the symbology, we need to create a temporary workflow
 * This puts a new operator on top of the actual workflow.
 */
function createClusteredPointLayerQueryWorkflow(
    workflow: WorkflowDict,
    metadata: VectorLayerMetadata,
    mapSpatialReference: SpatialReference,
    resolution: number,
): WorkflowDict {
    const columnAggregates: Record<
        string,
        {
            columnName: string;
            aggregateType: 'meanNumber' | 'stringSample' | 'null';
        }
    > = {};

    for (const [columnName, dataType] of metadata.dataTypes.entries()) {
        let aggregateType: 'meanNumber' | 'stringSample' | 'null';
        switch (dataType) {
            case VectorColumnDataTypes.Category:
            case VectorColumnDataTypes.Float:
            case VectorColumnDataTypes.Int:
                aggregateType = 'meanNumber';
                break;
            case VectorColumnDataTypes.Text:
            case VectorColumnDataTypes.DateTime:
                aggregateType = 'stringSample';
                break;
            default:
                aggregateType = 'null';
        }

        columnAggregates[columnName] = {
            columnName,
            aggregateType,
        };
    }

    return {
        type: 'Vector',
        operator: {
            type: 'VisualPointClustering',
            params: {
                minRadiusPx: PointSymbology.DEFAULT_POINT_RADIUS,
                deltaPx: ClusteredPointSymbology.DELTA_PX,
                resolution,
                radiusColumn: ClusteredPointSymbology.RADIUS_COLUMN,
                countColumn: ClusteredPointSymbology.COUNT_COLUMN,
                columnAggregates,
            },
            sources: {
                vector: createProjectedOperator(workflow.operator, metadata, mapSpatialReference),
            },
        },
    };
}

/**
 * In order to visualize simplified lines and polygons, we need to create a temporary workflow.
 * This puts a new operator on top of the actual workflow.
 */
// eslint-disable-next-line prefer-arrow/prefer-arrow-functions
function createSimplifiedLinesOrPolygonsLayerQueryWorkflow(
    workflow: WorkflowDict,
    metadata: VectorLayerMetadata,
    mapSpatialReference: SpatialReference,
    resolution: number,
): WorkflowDict {
    return {
        type: 'Vector',
        operator: {
            type: 'LineSimplification',
            params: {
                algorithm: 'douglasPeucker',
                epsilon: resolution, // derived by query resolution
            },
            sources: {
                vector: createProjectedOperator(workflow.operator, metadata, mapSpatialReference),
            },
        } as LineSimplificationDict,
    };
}

/**
 * Creates a projected operator if the layer has not the target spatial reference.
 */
function createProjectedOperator(
    inputOperator: LegacyTypedOperatorOperator,
    metadata: LayerMetadata,
    targetSpatialReference: SpatialReference,
): LegacyTypedOperatorOperator {
    if (metadata.spatialReference.equals(targetSpatialReference)) {
        return inputOperator;
    }

    return {
        type: 'Reprojection',
        params: {
            targetSpatialReference: targetSpatialReference.srsString,
        },
        sources: {
            source: inputOperator,
        },
    };
}
