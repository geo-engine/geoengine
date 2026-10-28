import {ComponentFixture, TestBed} from '@angular/core/testing';
import {fromLonLat, get as getProjection} from 'ol/proj';
import ReprojTile from 'ol/reproj/Tile';
import TileState from 'ol/TileState';
import {NEVER} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {SpatialReference, Time} from '@geoengine/common';
import {BackendService} from '../backend/backend.service';
import {CoreConfig, DEFAULT_CORE_CONFIG} from '../config.service';
import {OlOgcApiMapTileLayerComponent} from './map-layer.component';
import {ProjectService} from '../project/project.service';

describe('OGC map tile projection switching', () => {
    let fixture: ComponentFixture<OlOgcApiMapTileLayerComponent>;
    let nativeEpsg: number;
    let revokeObjectUrl: ReturnType<typeof vi.spyOn>;
    let fetchMock: ReturnType<typeof vi.fn>;
    const baseUrl = 'https://example.com/ogc/data/layer';

    beforeEach(() => {
        nativeEpsg = 4326;
        const documents = new Map<string, Blob>();
        vi.spyOn(URL, 'createObjectURL').mockImplementation((blob) => {
            const url = `blob:metadata-${documents.size}`;
            documents.set(url, blob as Blob);
            return url;
        });
        revokeObjectUrl = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => undefined);
        // OpenLayers reads the authenticated object URLs with XMLHttpRequest, separately from fetch.
        vi.stubGlobal(
            'XMLHttpRequest',
            class extends EventTarget {
                readonly status = 200;
                responseText = '';
                private url = '';
                open(_method: string, url: string): void {
                    this.url = url;
                }
                setRequestHeader(): void {
                    /* Object URL reads need no headers. */
                }
                send(): void {
                    void documents
                        .get(this.url)!
                        .text()
                        .then((text) => {
                            this.responseText = text;
                            this.dispatchEvent(new Event('load'));
                        });
                }
            },
        );
        fetchMock = vi.fn((url: string) => {
            const tms = url.endsWith('/CustomWebMercator') ? 'CustomWebMercator' : 'Custom';
            const epsg = tms === 'CustomWebMercator' ? 3857 : nativeEpsg;
            const crs = `http://www.opengis.net/def/crs/EPSG/0/${epsg}`;
            const metadata = url.includes('/tileMatrixSets/')
                ? {
                      id: tms,
                      crs,
                      orderedAxes: epsg === 4326 ? ['Lon', 'Lat'] : ['X', 'Y'],
                      tileMatrices: [
                          {
                              id: '0',
                              cellSize: epsg === 4326 ? 360 / 256 : epsg === 3857 ? 40075016.68557849 / 256 : 10000,
                              pointOfOrigin:
                                  epsg === 4326
                                      ? [-180, 90]
                                      : epsg === 3857
                                        ? [-20037508.342789244, 20037508.342789244]
                                        : [-1000000, 11000000],
                              tileWidth: 256,
                              tileHeight: 256,
                              matrixWidth: 4,
                              matrixHeight: 4,
                          },
                      ],
                  }
                : {
                      dataType: 'map',
                      crs,
                      links: [
                          {rel: 'http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme', href: `${baseUrl}/tileMatrixSets/${tms}`},
                          {
                              rel: 'item',
                              type: 'image/png',
                              href: `${baseUrl}/collections/layer/map/tiles/${tms}/{tileMatrix}/{tileRow}/{tileCol}?datetime={datetime}`,
                          },
                      ],
                  };
            return Promise.resolve({ok: true, json: () => Promise.resolve(metadata)});
        });
        vi.stubGlobal('fetch', fetchMock);
        TestBed.configureTestingModule({
            imports: [OlOgcApiMapTileLayerComponent],
            providers: [
                {provide: BackendService, useValue: {ogcApiBaseUrl: 'https://example.com/ogc'}},
                {provide: CoreConfig, useValue: DEFAULT_CORE_CONFIG},
                {provide: ProjectService, useValue: {createQueryAbortStream: vi.fn().mockReturnValue(NEVER)}},
            ],
        });
        fixture = TestBed.createComponent(OlOgcApiMapTileLayerComponent);
        fixture.componentRef.setInput('layerId', 0);
        fixture.componentRef.setInput('dataConnectorId', 'data');
        fixture.componentRef.setInput('dataLayerId', 'layer');
        fixture.componentRef.setInput('sessionToken', 'session');
        fixture.componentRef.setInput('spatialReference', new SpatialReference('EPSG:3857'));
        fixture.componentRef.setInput('time', new Time(new Date('2026-04-01T00:00:00Z')));
        fixture.componentRef.setInput('tmsId', 'CustomWebMercator');
    });

    afterEach(() => {
        fixture.destroy();
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
    });

    async function readySource(): Promise<NonNullable<ReturnType<OlOgcApiMapTileLayerComponent['tileSource']['value']>>> {
        fixture.detectChanges();
        await vi.waitFor(() => {
            fixture.detectChanges();
            expect(fixture.componentInstance.tileSource.value()?.getState()).toBe('ready');
        });
        return fixture.componentInstance.tileSource.value()!;
    }

    it.each([4326, 32632, 32633, 32755])('reloads native EPSG:%s tiles for OpenLayers reprojection and switches back', async (epsg) => {
        nativeEpsg = epsg;
        const backendSource = await readySource();
        expect(backendSource.getProjection()?.getCode()).toContain('3857');
        const metadataRequest = fetchMock.mock.calls[0] as [string, RequestInit];
        expect(metadataRequest[0]).toBe(`${baseUrl}/collections/layer/map/tiles/CustomWebMercator`);
        expect(metadataRequest[1].headers).toEqual({Authorization: 'Bearer session'});
        const previousSignal = metadataRequest[1].signal!;

        fixture.componentRef.setInput('tmsId', 'Custom');
        const nativeSource = await readySource();
        expect(nativeSource).not.toBe(backendSource);
        expect(previousSignal.aborted).toBe(true);
        expect(revokeObjectUrl).toHaveBeenCalled();
        expect(nativeSource.getProjection()?.getCode()).toContain(String(epsg));
        const url = nativeSource.getTileUrlFunction()([0, 0, 0], 1, nativeSource.getProjection()!);
        expect(url).toContain('/map/tiles/Custom/0/0/0?datetime=2026-04-01');
        const viewProjection = getProjection('EPSG:3857')!;
        const center = fromLonLat(epsg === 32755 ? [147, -30] : [9, 50]);
        const [z, x, y] = nativeSource.getTileGridForProjection(viewProjection).getTileCoordForCoordAndZ(center, 5);
        const displayTile = nativeSource.getTile(z, x, y, 1, viewProjection);
        expect(displayTile).toBeInstanceOf(ReprojTile);
        expect(displayTile.getState()).not.toBe(TileState.EMPTY);

        fixture.componentRef.setInput('tmsId', 'CustomWebMercator');
        const restoredSource = await readySource();
        expect(restoredSource).not.toBe(nativeSource);
        expect(restoredSource.getProjection()?.getCode()).toContain('3857');
    });
});
