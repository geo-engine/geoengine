import {
    ChangeDetectionStrategy,
    Component,
    Directive,
    OnChanges,
    OnDestroy,
    OnInit,
    SimpleChange,
    SimpleChanges,
    effect,
    inject,
    input,
    output,
    resource,
} from '@angular/core';
import {Observable, Subject, Subscription} from 'rxjs';

import {Layer as OlLayer, Tile as OlLayerTile, Vector as OlLayerVector} from 'ol/layer';
import {Source as OlSource, TileWMS as OlTileWmsSource, Vector as OlVectorSource, OGCMapTile, TileDebug, ImageTile} from 'ol/source';
import OlImageTile from 'ol/ImageTile';
import {get as olGetProj} from 'ol/proj';
import TileGrid from 'ol/tilegrid/TileGrid';
import type {EventsKey} from 'ol/events';
import {unByKey} from 'ol/Observable';
import {CoreConfig} from '../config.service';
import {NotificationService} from '@geoengine/common';
import {ProjectService} from '../project/project.service';
import {LoadingState} from '../project/loading-state.model';
import {BackendService} from '../backend/backend.service';
import {UUID} from '../backend/backend.model';
import OlFeature from 'ol/Feature';
import {
    RasterColorizer,
    RasterData,
    RasterSymbology,
    SpatialReference,
    Symbology,
    Time,
    VectorData,
    VectorSymbology,
    olExtentToTuple,
} from '@geoengine/common';
import {AbortableTileLayer} from './abortable-tile-layer';
import {
    TileDiagnostic,
    TileImageLike,
    TileInView,
    TileFrame,
    TileLoadState,
    TileLoader,
    currentViewport,
    tileExtentInViewProjection,
} from './tile-loader';

/**
 * The `ol-layer` component represents a single layer object of open layers.
 */
@Directive()
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export abstract class MapLayerComponent<OL extends OlLayer<OS, any>, OS extends OlSource, S extends Symbology> {
    protected projectService = inject(ProjectService);

    readonly layerId = input.required<number>();
    readonly isVisible = input(true);
    readonly workflow = input<UUID>();
    readonly symbology = input<S>();

    /**
     * Event emitter that forces a redraw of the map.
     * Must be connected to the map component.
     */
    readonly mapRedraw = output();

    loadedData$ = new Subject<void>();

    protected config = inject(CoreConfig);

    protected source: OS;
    protected _mapLayer: OL;

    /** The loader whose requests are watched per rendered frame, see {@link watchViewport}. */
    private watchedLoader?: TileLoader;
    private unwatchViewport?: EventsKey | EventsKey[];

    /**
     * Setup of DI
     */
    // eslint-disable-next-line @angular-eslint/prefer-inject
    protected constructor(source: OS, layer: (_: OS) => OL) {
        this.source = source;
        this._mapLayer = layer(source);
    }

    /**
     * Return the open layers layer element that displays our layer type
     */
    get mapLayer(): OL {
        return this._mapLayer;
    }

    /**
     * Writes what a tile request did to the console, if `MAP.DEBUG_TILES` asks for it. Tiles that
     * never show up are hard to see otherwise, because OpenLayers silently turns a cancelled or
     * empty image into a tile it never requests again.
     */
    protected reportTileDiagnostic(diagnostic: TileDiagnostic): void {
        if (!this.config.MAP.DEBUG_TILES) {
            return;
        }

        console.warn(`[tiles:${this.layerId()}]`, diagnostic);
    }

    /**
     * Drops the reprojection that was built from a tile that just turned `ERROR`, so the next
     * frame builds a fresh one instead of reusing a failed or incomplete one.
     */
    protected invalidateTileReprojection(tile: OlImageTile): void {
        if (this._mapLayer instanceof AbortableTileLayer && this._mapLayer.getSource() === this.source) {
            this._mapLayer.invalidateAbortedTile(tile);
        }
    }

    /**
     * Cancels the tile requests of tiles the viewport does not want anymore, once per rendered
     * frame, as long as the given loader has requests in flight. Without a frame to run on there is
     * nothing to watch, so the loader gets its tiles cancelled when the movement ends.
     *
     * OpenLayers dispatches `postrender` after the renderer queued the frame's tiles and after
     * `moveend`, but before `handlePostRender` starts their requests, so an abort here is cheap.
     */
    protected watchViewport(loader: TileLoader, tileGrid: TileGrid): void {
        const map = this._mapLayer.getMapInternal();
        const source = this.source as unknown as TileImageLike; // only the raster components call this, and both use tile sources
        if (!map) {
            return;
        }
        if (this.watchedLoader === loader) {
            return;
        }
        this.unwatchViewportFrames();
        this.watchedLoader = loader;
        this.unwatchViewport = map.on('postrender', (event) => {
            const frame = event.frameState;
            if (!frame) {
                return;
            }
            const {extent, zoom} = currentViewport(source, frame as unknown as TileFrame, tileGrid);
            loader.cancelUnwanted(extent, zoom);
        });
    }

    /** Stops watching rendered frames. Pass the loader to only stop if it is the one being watched. */
    protected unwatchViewportFrames(loader?: TileLoader): void {
        if (loader !== undefined && this.watchedLoader !== loader) {
            return;
        }
        if (this.unwatchViewport) {
            unByKey(this.unwatchViewport);
        }
        this.unwatchViewport = undefined;
        this.watchedLoader = undefined;
    }

    /**
     * Return the extent of the layer in map units
     */
    abstract getExtent(): [number, number, number, number];

    protected extractChange<T>(change: SimpleChange): T | undefined {
        if (!change) {
            return undefined;
        }

        if (!change.isFirstChange() && change.currentValue === change.previousValue) {
            return undefined;
        }

        return change.currentValue;
    }
}

/**
 * This component reflects a vector layer on the map
 */
@Component({
    selector: 'geoengine-ol-vector-layer',
    template: '',
    providers: [{provide: MapLayerComponent, useExisting: OlVectorLayerComponent}],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class OlVectorLayerComponent
    extends MapLayerComponent<OlLayerVector<OlVectorSource<OlFeature>>, OlVectorSource<OlFeature>, VectorSymbology>
    implements OnInit, OnDestroy, OnChanges
{
    override readonly symbology = input<VectorSymbology>();

    protected dataSubscription?: Subscription;

    constructor() {
        super(
            new OlVectorSource({wrapX: false}),
            (source) =>
                new OlLayerVector({
                    source,
                    updateWhileAnimating: true,
                }),
        );
    }

    ngOnInit(): void {
        this.dataSubscription = this.projectService.getLayerDataStream({id: this.layerId()}).subscribe((x) => {
            this.source.clear(); // TODO: check if this is needed always...
            if (!(x === null || x === undefined) && x instanceof VectorData) {
                this.source.addFeatures(x.data);
            }
            this.updateOlLayer({symbology: this.symbology()}); // FIXME: HACK until data is a part of a layer
            this.loadedData$.next();
        });
    }

    ngOnDestroy(): void {
        if (this.dataSubscription) {
            this.dataSubscription.unsubscribe();
        }
    }

    getExtent(): [number, number, number, number] {
        const extent = this.source.getExtent();
        if (!extent) {
            throw Error('Vector source has no extent');
        }
        return olExtentToTuple(extent);
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (Object.keys(changes).length > 0) {
            this.updateOlLayer({
                isVisible: this.extractChange<boolean>(changes.isVisible),
                symbology: this.extractChange<VectorSymbology>(changes.symbology),
                workflow: this.extractChange<UUID>(changes.workflow),
            });
        }
    }

    private updateOlLayer(changes: {isVisible?: boolean; symbology?: VectorSymbology; workflow?: UUID}): void {
        if (changes.isVisible !== undefined) {
            this.mapLayer.setVisible(this.isVisible());
            this.mapRedraw.emit();
        }

        const symbology = this.symbology();
        if (changes.symbology && symbology) {
            this.mapLayer.setStyle(symbology.createStyleFunction());
        }
    }
}

/**
 * This component reflects a raster layer on the map
 */
@Component({
    selector: 'geoengine-ol-raster-layer',
    template: '',
    providers: [{provide: MapLayerComponent, useExisting: OlRasterLayerComponent}],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class OlRasterLayerComponent
    extends MapLayerComponent<OlLayerTile<OlTileWmsSource>, OlTileWmsSource, RasterSymbology>
    implements OnInit, OnDestroy, OnChanges
{
    protected backend = inject(BackendService);
    protected notificationService = inject(NotificationService);

    override readonly symbology = input<RasterSymbology>();

    readonly sessionToken = input<UUID>();

    /** Loads the tiles of the current source. */
    private loader?: TileLoader;

    protected dataSubscription?: Subscription;
    protected layerChangesSubscription?: Subscription;
    protected timeSubscription?: Subscription;

    protected spatialReference?: SpatialReference;
    protected time?: Time;

    constructor() {
        super(
            new OlTileWmsSource({
                // empty for start
                params: {},
            }),
            (source) =>
                new AbortableTileLayer({
                    source,
                    opacity: 1,
                }),
        );
    }

    ngOnInit(): void {
        this.dataSubscription = this.projectService.getLayerDataStream({id: this.layerId()}).subscribe((rasterData) => {
            if (!rasterData || !(rasterData instanceof RasterData)) {
                return;
            }

            this.updateTime(rasterData.time);
            this.updateProjection(rasterData.spatialReference);

            if (!this.source) {
                this.initializeOrReplaceOlSource();
            }

            if (this.config.MAP.REFRESH_LAYERS_ON_CHANGE) {
                this.source.refresh();
            }
        });
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (Object.keys(changes).length > 0) {
            this.updateOlLayer({
                isVisible: this.extractChange<boolean>(changes.isVisible),
                symbology: this.extractChange<RasterSymbology>(changes.symbology),
                workflow: this.extractChange<UUID>(changes.workflow),
            });
        }
    }

    ngOnDestroy(): void {
        if (this.dataSubscription) {
            this.dataSubscription.unsubscribe();
        }
        if (this.layerChangesSubscription) {
            this.layerChangesSubscription.unsubscribe();
        }
        if (this.timeSubscription) {
            this.timeSubscription.unsubscribe();
        }

        // abort all WMS tile requests that are still in flight
        this.unwatchViewportFrames();
        this.loader?.abortAll();
    }

    getExtent(): [number, number, number, number] {
        return olExtentToTuple(this._mapLayer.getExtent() ?? [0, 0, 0, 0]);
    }

    private updateOlLayer(changes: {isVisible?: boolean; symbology?: RasterSymbology; workflow?: UUID; sessionToken?: UUID}): void {
        if (this.source === undefined || this._mapLayer === undefined) {
            return;
        }

        if (changes.isVisible !== undefined) {
            this._mapLayer.setVisible(this.isVisible());
            this.mapRedraw.emit();
        }
        const symbology = this.symbology();
        if (changes.symbology && symbology) {
            this._mapLayer.setOpacity(symbology.opacity);

            this.source.updateParams({
                STYLES: this.stylesFromColorizer(symbology.rasterColorizer),
            });
        }
        if (changes.workflow !== undefined || changes.sessionToken !== undefined) {
            this.initializeOrReplaceOlSource();
        }

        if (this.config.MAP.REFRESH_LAYERS_ON_CHANGE) {
            this.source.refresh();
        }
    }

    private updateProjection(p: SpatialReference): void {
        if (p.srsString !== this.spatialReference?.srsString) {
            this.spatialReference = p;
            this.updateOlLayerProjection();
        }
    }

    private updateOlLayerProjection(): void {
        // there is no way to change the projection of a layer. // TODO: check newer OL versions for this
        this.initializeOrReplaceOlSource();
    }

    private updateOlLayerTime(): void {
        const symbology = this.symbology();
        if (this.source && this.time && symbology) {
            this.source.updateParams({
                time: this.time.asRequestString(),
                STYLES: this.stylesFromColorizer(symbology.rasterColorizer),
            });
        }
    }

    private updateTime(t: Time): void {
        if (this.time === undefined || !t.isSame(this.time)) {
            this.time = t;
            this.updateOlLayerTime();
        }
    }

    private initializeOrReplaceOlSource(): void {
        const symbology = this.symbology();
        const spatialReference = this.spatialReference;
        if (!this.time || !symbology || !spatialReference) {
            return;
        }

        // the tiles of the replaced source are not displayed anymore, so their requests can be dropped
        this.unwatchViewportFrames();
        this.loader?.abortAll();

        const source = new OlTileWmsSource({
            url: `${this.backend.wmsBaseUrl}/${this.workflow()}`,
            params: {
                layers: this.workflow(),
                time: this.time.asRequestString(),
                STYLES: this.stylesFromColorizer(symbology.rasterColorizer),
                EXCEPTIONS: 'application/json',
            },
            projection: spatialReference.srsString,
            wrapX: false,
        });

        // The source is created in the layer's own projection, so its tile coordinates have to be
        // read from that grid and then transformed to the projection the viewport is in.
        const sourceProjection = source.getProjection()!;
        const tileGrid = source.getTileGridForProjection(sourceProjection);

        const loader: TileLoader = new TileLoader({
            authHeaders: (): Record<string, string> => ({Authorization: `Bearer ${this.sessionToken()}`}),
            abortWhen: (): Observable<string> => this.projectService.createQueryAbortStream(this.layerId()),
            onTileError: (tile): void => this.invalidateTileReprojection(tile),
            onStateChange: (state): void => {
                if (state === 'loading') {
                    this.watchViewport(loader, tileGrid);
                } else {
                    this.unwatchViewportFrames(loader);
                }
                this.reportDataStatus(state);
            },
            onError: (message): void => {
                this.notificationService.error(message);
            },
            onDiagnostic: (diagnostic): void => this.reportTileDiagnostic(diagnostic),
            tileExtentInView: (tile): TileInView => ({
                extent: tileExtentInViewProjection(tileGrid, tile, sourceProjection, olGetProj(spatialReference.srsString)!),
                zoom: tile.getTileCoord()[0],
            }),
        });
        this.loader = loader;
        source.setTileLoadFunction(loader.load);

        this.source = source;
        this.initializeOrUpdateOlMapLayer();
    }

    private reportDataStatus(state: TileLoadState): void {
        const loadingState = {idle: LoadingState.OK, loading: LoadingState.LOADING, error: LoadingState.ERROR};
        this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, loadingState[state]);
    }

    private stylesFromColorizer(colorizer: RasterColorizer): string {
        return 'custom:' + JSON.stringify(colorizer.toDict());
    }

    private initializeOrUpdateOlMapLayer(): void {
        const symbology = this.symbology();
        if (this._mapLayer) {
            this._mapLayer.setSource(this.source);
        } else if (symbology) {
            this._mapLayer = new AbortableTileLayer({
                source: this.source,
                opacity: symbology.opacity,
            });
        }
    }
}

/**
 * All TMS ids that are supported by the backend. The TMS id is used to request the correct tile matrix set from the backend.
 */
export type TMSId = 'Custom' | 'CustomWebMercator' | 'WebMercatorQuad';

/**
 * This component renders a raster layer on the map by using the OGC API Map Tiles standard.
 */
@Component({
    selector: 'geoengine-ol-ogc-api-map-tile-layer',
    template: '',
    providers: [{provide: MapLayerComponent, useExisting: OlOgcApiMapTileLayerComponent}],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class OlOgcApiMapTileLayerComponent
    extends MapLayerComponent<OlLayerTile<OGCMapTile | TileDebug>, OGCMapTile | TileDebug | ImageTile, RasterSymbology>
    implements OnDestroy
{
    protected readonly backend = inject(BackendService);

    readonly dataConnectorId = input.required<UUID>();
    readonly dataLayerId = input.required<string>();
    readonly sessionToken = input.required<UUID>();
    readonly spatialReference = input.required<SpatialReference>(); // TODO: do we need this?
    readonly time = input.required<Time>();
    readonly tmsId = input<TMSId>('Custom');

    // Show tile debug info instead of the actual layer. This is useful for debugging tile loading issues.
    readonly debug = input(false);

    /** Emits `true` while any tile in this layer is loading, `false` when all tiles are loaded. */
    readonly loading = output<boolean>();

    /** Loads the tiles of the current source. The resource signal already aborts it, this keeps teardown local. */
    private loader?: TileLoader;

    readonly tileSource = resource({
        params: () => ({
            dataConnectorId: this.dataConnectorId(),
            layerId: this.dataLayerId(),
            sessionToken: this.sessionToken(),
            spatialReference: this.spatialReference(),
            time: this.time(), // no way to just update `context` field in OGCMapTile…
        }),
        loader: async ({abortSignal}): Promise<OGCMapTile> => {
            // `source` is assigned after the await below, so the closures that read it must only
            // run once the source exists. Hoisting avoids a temporal-dead-zone reference.
            // eslint-disable-next-line prefer-const -- assigned after the await; `const` would move it before the closures that need it
            let source: OGCMapTile;

            // The source reports its own projection, which the backend picked when it built the tile
            // matrix set, so its tile coordinates belong to that grid and not to the grid of the
            // projection the viewport is in.
            const sourceGrid = (): TileGrid => source.getTileGridForProjection(source.getProjection()!);

            // the abort signal covers both a changed set of params and the destruction of the layer
            const loader: TileLoader = new TileLoader({
                signal: abortSignal,
                authHeaders: (): Record<string, string> => ({Authorization: `Bearer ${this.sessionToken()}`}),
                abortWhen: (): Observable<string> => this.projectService.createQueryAbortStream(this.layerId()),
                onTileError: (tile): void => this.invalidateTileReprojection(tile),
                onStateChange: (state): void => {
                    if (state === 'loading') {
                        this.watchViewport(loader, sourceGrid());
                    } else {
                        this.unwatchViewportFrames(loader);
                    }
                    this.loading.emit(state === 'loading');
                },
                onDiagnostic: (diagnostic): void => this.reportTileDiagnostic(diagnostic),
                tileExtentInView: (tile): TileInView => ({
                    extent: tileExtentInViewProjection(
                        sourceGrid(),
                        tile,
                        source.getProjection()!,
                        olGetProj(this.spatialReference().srsString)!,
                    ),
                    zoom: tile.getTileCoord()[0],
                }),
            });
            this.loader = loader;

            source = new OGCMapTile({
                url: await this.tmsUrl(loader, abortSignal),
                context: {
                    datetime: this.time().asRequestString(),
                },
                wrapX: false, // wrapping does not work with our implementation
                interpolate: false, // Stops blurry tiles when zooming in.
                tileLoadFunction: loader.load,
            });

            return source;
        },
    });

    constructor() {
        super(
            new ImageTile({}), // use as placeholder until the actual source is loaded
            (_source) => {
                return new AbortableTileLayer();
            },
        );

        effect(() => {
            const source = this.tileSource.value();
            if (!source) return;

            this.source = this.debug() ? new TileDebug({source}) : source;
            this._mapLayer.setSource(this.source);
        });

        effect(() => /* TODO: define in parent class */ {
            const isVisible = this.isVisible();
            this._mapLayer.setVisible(isVisible);
        });

        effect(() => {
            const symbology = this.symbology();
            if (!symbology) return;

            this._mapLayer.setOpacity(symbology.opacity);
        });
    }

    /**
     * The tile matrix set of the requested tile matrix set id, as a URL that the `OGCMapTile`
     * source can read on its own. OpenLayers reads it without authentication headers, so it has
     * to be served as an object URL. The same applies to the tiling scheme it links to.
     */
    private async tmsUrl(loader: TileLoader, signal: AbortSignal): Promise<string> {
        const dataConnectorId = this.dataConnectorId();
        const layerId = this.dataLayerId();
        const tms = this.tmsId();

        const tmsUrl = `${this.backend.ogcApiBaseUrl}/${dataConnectorId}/${layerId}/collections/${layerId}/map/tiles/${tms}`;

        return loader.jsonUrl(tmsUrl, signal, async (metadata) => {
            const links = (metadata as {links?: Array<{rel: string; href: string}>}).links ?? [];
            for (const link of links) {
                if (link.rel === 'http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme') {
                    link.href = await loader.jsonUrl(link.href, signal);
                }
            }
        });
    }

    getExtent(): [number, number, number, number] {
        return olExtentToTuple(this._mapLayer.getExtent() ?? [0, 0, 0, 0]);
    }

    ngOnDestroy(): void {
        // the resource signal aborts the loader as well, but doing it here keeps the teardown of
        // the in-flight tile requests in the component that started them
        this.unwatchViewportFrames();
        this.loader?.abortAll();
    }
}
