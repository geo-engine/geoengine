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
import {Subject, Subscription} from 'rxjs';

import {Layer as OlLayer, Tile as OlLayerTile, Vector as OlLayerVector} from 'ol/layer';
import {ImageTile as OlImageTile} from 'ol';
import {Source as OlSource, TileWMS as OlTileWmsSource, Vector as OlVectorSource, OGCMapTile, TileDebug, ImageTile} from 'ol/source';
import {get as olGetProj} from 'ol/proj';
import {CoreConfig} from '../config.service';
import {ProjectService} from '../project/project.service';
import {LoadingState} from '../project/loading-state.model';
import {BackendService} from '../backend/backend.service';
import {UUID} from '../backend/backend.model';
import OlFeature from 'ol/Feature';
import TileState from 'ol/TileState';
import {Extent} from './map.service';
import {
    NotificationService,
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

    protected source: OS;
    protected _mapLayer: OL;

    /**
     * True while an aborted tile is being reset. `resetAbortedTile` is forced through `ERROR`,
     * which synchronously fires a `tileloaderror`; this flag lets the listeners classify that
     * event as transient. It only needs to hold during the reset: genuine failures always fire
     * from an async callback (XHR/fetch), never inside this window.
     */
    protected resettingAbortedTile = false;

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
     * Return the extent of the layer in map units
     */
    abstract getExtent(): [number, number, number, number];

    /**
     * Reset an aborted tile to `IDLE` so OpenLayers re-requests it (e.g. after panning away and
     * back). OpenLayers only requests IDLE tiles, so an ERROR tile would stay invisible forever.
     *
     * `setState` must not go from `LOADING` to `IDLE` directly ("Tile load sequence violation"),
     * so it is routed through `ERROR` first, which also lets the source fire the matching
     * `tileloaderror` and keep its in-flight tile bookkeeping balanced.
     */
    protected resetAbortedTile(tile: OlImageTile): void {
        const previous = this.resettingAbortedTile;
        this.resettingAbortedTile = true;
        try {
            tile.setState(TileState.ERROR);
            tile.setState(TileState.IDLE);
        } finally {
            this.resettingAbortedTile = previous;
        }
    }

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
    protected config = inject(CoreConfig);
    protected notificationService = inject(NotificationService);

    override readonly symbology = input<RasterSymbology>();

    readonly sessionToken = input<UUID>();

    /** In-flight WMS tile requests that need to be aborted when the layer is destroyed. */
    private readonly tileAbortClients = new Set<XMLHttpRequest>();

    /** True once `ngOnDestroy` ran, so async tile callbacks can stop touching the component. */
    private destroyed = false;

    /** Removes the state listeners from the source that is currently set. */
    private sourceStateUnlisten?: () => void;

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
                new OlLayerTile({
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
        this.destroyed = true;
        this.sourceStateUnlisten?.();

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
        for (const client of this.tileAbortClients) {
            client.abort();
        }
        this.tileAbortClients.clear();
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
        if (!this.time || !symbology || !this.spatialReference) {
            return;
        }

        // detach the old source and abort its in-flight requests so that no stale
        // tile callbacks keep modifying this layer's state
        this.sourceStateUnlisten?.();
        this.sourceStateUnlisten = undefined;
        for (const client of this.tileAbortClients) {
            client.abort();
        }
        this.tileAbortClients.clear();

        this.source = new OlTileWmsSource({
            url: `${this.backend.wmsBaseUrl}/${this.workflow()}`,
            params: {
                layers: this.workflow(),
                time: this.time.asRequestString(),
                STYLES: this.stylesFromColorizer(symbology.rasterColorizer),
                EXCEPTIONS: 'application/json',
            },
            projection: this.spatialReference.srsString,
            wrapX: false,
        });

        const proj = olGetProj(this.spatialReference.srsString)!;
        const source = this.source;
        const tileGrid = source.getTileGridForProjection(proj);

        source.setTileLoadFunction((olTile, src) => {
            const tile = olTile as OlImageTile;
            const tileCoord = tile.getTileCoord();
            const tileExtent = tileGrid.getTileCoordExtent(tileCoord) as Extent;

            const client = new XMLHttpRequest();
            this.tileAbortClients.add(client);

            let aborted = false;

            const cancelSub = this.projectService.createQueryAbortStream(this.layerId(), tileExtent).subscribe(() => {
                aborted = true;
                client.abort();
            });

            client.open('GET', src);
            client.responseType = 'blob';
            client.setRequestHeader('Authorization', `Bearer ${this.sessionToken()}`);
            client.addEventListener('abort', () => {
                aborted = true;
            });
            client.addEventListener('loadend', (_event) => {
                cancelSub.unsubscribe();
                this.tileAbortClients.delete(client);

                if (this.destroyed) {
                    return;
                }

                // a tile evicted from the OpenLayers cache while loading never reports back, so the
                // source's tile events alone cannot balance the loading state; only a tile whose image
                // was disposed is guaranteed to never fire tile events again
                if (!tile.getImage() && this.tileAbortClients.size === 0) {
                    this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, LoadingState.OK);
                }

                const data = client.response;

                if (!data) {
                    if (aborted) {
                        // The tile may be requested again later, so reset it to IDLE
                        // instead of leaving it in ERROR (which OpenLayers never re-fetches).
                        this.resetAbortedTile(tile);
                    } else {
                        tile.setState(TileState.ERROR);
                    }
                } else {
                    if (data.type === 'image/png') {
                        const url = URL.createObjectURL(data);
                        const image = tile.getImage() as HTMLImageElement | null;
                        if (!image) {
                            // the tile was evicted while the request was in flight
                            URL.revokeObjectURL(url);
                            return;
                        }
                        if (image.src.startsWith('blob:')) {
                            URL.revokeObjectURL(image.src);
                        }
                        image.src = url;
                        // once the tile is decoded the URL is no longer needed (and OpenLayers
                        // never revokes it itself), so free it to avoid leaking on cache eviction
                        image.addEventListener('load', () => URL.revokeObjectURL(url), {once: true});
                        image.addEventListener('error', () => URL.revokeObjectURL(url), {once: true});
                    } else {
                        tile.setState(TileState.ERROR);
                        data.text().then((m: string) => {
                            if (!this.destroyed) {
                                this.notificationService.error(JSON.parse(m)['message']);
                            }
                        });
                    }
                }
            });
            client.addEventListener('error', () => {
                this.tileAbortClients.delete(client);
                if (!this.destroyed) {
                    tile.setState(TileState.ERROR);
                }
            });

            // The abort stream may emit before the request is even sent (e.g. during panning),
            // in which case the XHR is aborted without firing `loadend` and the request would
            // still go out otherwise.
            if (aborted) {
                cancelSub.unsubscribe();
                this.tileAbortClients.delete(client);
                this.resetAbortedTile(tile);
            } else {
                client.send();
            }
        });

        this.sourceStateUnlisten = this.addStateListenersToOlSource(this.source);
        this.initializeOrUpdateOlMapLayer();
    }

    private stylesFromColorizer(colorizer: RasterColorizer): string {
        return 'custom:' + JSON.stringify(colorizer.toDict());
    }

    private initializeOrUpdateOlMapLayer(): void {
        const symbology = this.symbology();
        if (this._mapLayer) {
            this._mapLayer.setSource(this.source);
        } else if (symbology) {
            this._mapLayer = new OlLayerTile({
                source: this.source,
                opacity: symbology.opacity,
            });
        }
    }

    private addStateListenersToOlSource(source: OlTileWmsSource): () => void {
        // TILE LOADING STATE
        let tilesPending = 0;

        const onStart = (): void => {
            tilesPending++;
            this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, LoadingState.LOADING);
        };
        const onEnd = (): void => {
            tilesPending--;
            if (tilesPending <= 0) {
                this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, LoadingState.OK);
            }
        };
        const onError = (): void => {
            tilesPending--;

            if (this.resettingAbortedTile) {
                // the abort is transient, the tile will be re-requested
                if (tilesPending <= 0) {
                    this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, LoadingState.OK);
                }
                return;
            }

            this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, LoadingState.ERROR);
        };

        source.on('tileloadstart', onStart);
        source.on('tileloadend', onEnd);
        source.on('tileloaderror', onError);

        return () => {
            source.un('tileloadstart', onStart);
            source.un('tileloadend', onEnd);
            source.un('tileloaderror', onError);
        };
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
    protected readonly config = inject(CoreConfig);
    protected readonly notificationService = inject(NotificationService);

    /** Abort controllers for tile loads that are currently in flight. */
    private readonly tileAbortControllers = new Set<AbortController>();

    /** object URLs of the current source's JSON metadata, revoked when the source is replaced */
    private jsonObjectUrls: string[] = [];

    /** True once `ngOnDestroy` ran, so async tile callbacks can stop touching the component. */
    private destroyed = false;

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

    readonly tileSource = resource({
        params: () => ({
            dataConnectorId: this.dataConnectorId(),
            layerId: this.dataLayerId(),
            sessionToken: this.sessionToken(),
            spatialReference: this.spatialReference(),
            time: this.time(), // no way to just update `context` field in OGCMapTile…
        }),
        loader: async ({abortSignal}): Promise<{source: OGCMapTile; objectUrls: string[]}> => {
            const objectUrls: string[] = [];
            try {
                const source = new OGCMapTile({
                    url: await this.tmsBlobUrl(objectUrls, abortSignal),
                    context: {
                        datetime: this.time().asRequestString(),
                    },
                    wrapX: false, // wrapping does not work with our implementation
                    interpolate: false, // Stops blurry tiles when zooming in.
                    tileLoadFunction: (olTile, src): void => {
                        void (async (): Promise<void> => {
                            const controller = new AbortController();
                            this.tileAbortControllers.add(controller);

                            let cancelSub: Subscription | undefined;
                            try {
                                const tile = olTile as OlImageTile;
                                const tileCoord = tile.getTileCoord();
                                const tileGrid = source.getTileGridForProjection(olGetProj(this.spatialReference().srsString)!);
                                const tileExtent = tileGrid.getTileCoordExtent(tileCoord) as Extent;

                                cancelSub = this.projectService
                                    .createQueryAbortStream(this.layerId(), tileExtent)
                                    .subscribe(() => controller.abort());

                                const url = await this.urlToBlobUrl(src, 'IMAGE', undefined, controller.signal);

                                if (this.destroyed) {
                                    URL.revokeObjectURL(url);
                                    return;
                                }

                                // Successfully assign the object URL to the image element
                                const image = (olTile as unknown as {getImage: () => HTMLImageElement | null}).getImage();
                                if (!image) {
                                    // the tile was evicted while the request was in flight
                                    URL.revokeObjectURL(url);
                                    return;
                                }
                                if (image.src.startsWith('blob:')) {
                                    URL.revokeObjectURL(image.src);
                                }
                                image.src = url;
                                // once the tile is decoded the URL is no longer needed (and OpenLayers
                                // never revokes it itself), so free it to avoid leaking on cache eviction
                                image.addEventListener('load', () => URL.revokeObjectURL(url), {once: true});
                                image.addEventListener('error', () => URL.revokeObjectURL(url), {once: true});
                            } catch (error) {
                                if (this.destroyed) {
                                    return;
                                }

                                const aborted = error instanceof DOMException && error.name === 'AbortError';
                                if (!aborted) {
                                    console.error('Error loading OGC tile:', error);
                                }

                                // CRITICAL: You must explicitly catch errors and notify OpenLayers,
                                // otherwise the map will wait indefinitely for this tile to resolve.
                                if (aborted) {
                                    // The tile may be requested again later, so reset it to IDLE
                                    // instead of leaving it in ERROR (which OpenLayers never re-fetches).
                                    this.resetAbortedTile(olTile as OlImageTile);
                                } else {
                                    olTile.setState(TileState.ERROR);
                                }
                            } finally {
                                cancelSub?.unsubscribe();
                                this.tileAbortControllers.delete(controller);
                                // a tile evicted from the OpenLayers cache while loading never reports
                                // back, so the source's tile events alone cannot balance the loading state
                                this.loading.emit(this.tileAbortControllers.size > 0);
                            }
                        })();
                    },
                });

                return {source, objectUrls};
            } catch (error) {
                for (const url of objectUrls) {
                    URL.revokeObjectURL(url);
                }
                throw error;
            }
        },
    });

    constructor() {
        super(
            new ImageTile({}), // use as placeholder until the actual source is loaded
            (_source) => {
                return new OlLayerTile();
            },
        );

        effect((onCleanup) => {
            const value = this.tileSource.value();
            if (!value) return;
            const source = value.source;
            const sourceObjectUrls = value.objectUrls;
            this.jsonObjectUrls = sourceObjectUrls;

            this.source = source;
            if (this.debug()) {
                this.source = new TileDebug({source: this.source});
            }
            const tileSource = this.source;

            const unlistenState = this.addStateListenersToOlSource(tileSource);
            this._mapLayer.setSource(tileSource);

            // Track tile loading and emit through the loading output
            let tilesPending = 0;
            const onStart = (): void => {
                tilesPending++;
                this.loading.emit(true);
            };
            const onEnd = (): void => {
                tilesPending--;
                if (tilesPending <= 0) {
                    this.loading.emit(false);
                }
            };
            tileSource.on('tileloadstart', onStart);
            tileSource.on('tileloadend', onEnd);
            tileSource.on('tileloaderror', onEnd);

            onCleanup(() => {
                this.loading.emit(false);

                unlistenState();
                tileSource.un('tileloadstart', onStart);
                tileSource.un('tileloadend', onEnd);
                tileSource.un('tileloaderror', onEnd);

                // only when the source generation actually changed (a debug() toggle re-runs
                // this effect with the same generation): abort tile loads of the replaced
                // source still in flight and free its metadata object URLs
                if (this.tileSource.value() !== value) {
                    for (const controller of this.tileAbortControllers) {
                        controller.abort();
                    }
                    this.tileAbortControllers.clear();
                    for (const url of sourceObjectUrls) {
                        URL.revokeObjectURL(url);
                    }
                }
            });
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

    async tmsBlobUrl(objectUrls: string[], signal?: AbortSignal): Promise<string> {
        const dataConnectorId = this.dataConnectorId();
        const layerId = this.dataLayerId();
        const tms = this.tmsId();

        const tmsUrl = `${this.backend.ogcApiBaseUrl}/${dataConnectorId}/${layerId}/collections/${layerId}/map/tiles/${tms}`;

        return await this.urlToBlobUrl(
            tmsUrl,
            'JSON',
            async (metadata) => {
                if (!('links' in metadata)) {
                    return;
                }
                for (const link of metadata.links as Array<{rel: string; href: string; type: string}>) {
                    if (link.rel === 'http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme') {
                        link.href = await this.urlToBlobUrl(link.href, 'JSON', undefined, signal, objectUrls);
                    }
                }
            },
            signal,
            objectUrls,
        );
    }

    async urlToBlobUrl(
        url: string,
        type: 'JSON' | 'IMAGE',
        interceptor?: (metadata: JSON) => Promise<void>,
        signal?: AbortSignal,
        objectUrls?: string[],
    ): Promise<string> {
        const sessionToken = this.sessionToken();

        const response = await fetch(url, {
            headers: {
                Authorization: `Bearer ${sessionToken}`,
            },
            signal,
        });

        if (!response.ok) {
            throw new Error(`Fetch failed with status: ${response.status}`);
        }

        switch (type) {
            case 'JSON': {
                const metadata = await response.json();

                if (interceptor) {
                    await interceptor(metadata);
                }

                if (signal?.aborted) {
                    throw new DOMException('Aborted', 'AbortError');
                }

                const objectUrl = URL.createObjectURL(
                    new Blob([JSON.stringify(metadata)], {
                        type: 'application/json',
                    }),
                );
                objectUrls?.push(objectUrl);
                return objectUrl;
            }
            case 'IMAGE': {
                // the caller is responsible for revoking the returned blob URL
                return URL.createObjectURL(await response.blob());
            }
        }
    }

    getExtent(): [number, number, number, number] {
        return olExtentToTuple(this._mapLayer.getExtent() ?? [0, 0, 0, 0]);
    }

    ngOnDestroy(): void {
        this.destroyed = true;

        this.loading.emit(false);

        // abort all tile requests that are still in flight
        for (const controller of this.tileAbortControllers) {
            controller.abort();
        }
        this.tileAbortControllers.clear();

        for (const url of this.jsonObjectUrls) {
            URL.revokeObjectURL(url);
        }
        this.jsonObjectUrls = [];
    }

    private addStateListenersToOlSource(source: OGCMapTile | TileDebug | ImageTile): () => void {
        // TILE LOADING STATE
        let tilesPending = 0;

        const onStart = (): void => {
            tilesPending++;
            this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, LoadingState.LOADING);
        };
        const onEnd = (): void => {
            tilesPending--;
            if (tilesPending <= 0) {
                this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, LoadingState.OK);
            }
        };
        const onError = (): void => {
            tilesPending--;

            if (this.resettingAbortedTile) {
                // the abort is transient, the tile will be re-requested
                if (tilesPending <= 0) {
                    this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, LoadingState.OK);
                }
                return;
            }

            this.projectService.changeRasterLayerDataStatus({id: this.layerId(), layerType: 'raster'}, LoadingState.ERROR);
        };

        source.on('tileloadstart', onStart);
        source.on('tileloadend', onEnd);
        source.on('tileloaderror', onError);

        return () => {
            source.un('tileloadstart', onStart);
            source.un('tileloadend', onEnd);
            source.un('tileloaderror', onError);
        };
    }
}
