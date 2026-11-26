import {TestBed} from '@angular/core/testing';
import View from 'ol/View';
import {Subscription} from 'rxjs';
import {afterEach, expect, it, vi} from 'vitest';
import {CoreConfig, DEFAULT_CORE_CONFIG} from '../config.service';
import type {MapContainerComponent} from './map-container/map-container.component';
import {MapService, ViewportSize} from './map.service';

let subscriptions: Subscription;

function createMap(delay = DEFAULT_CORE_CONFIG.DELAYS.ZOOM_DEBOUNCE): {map: MapService; view: View} {
    vi.useFakeTimers();
    subscriptions = new Subscription();
    TestBed.configureTestingModule({
        providers: [
            MapService,
            {provide: CoreConfig, useValue: {...DEFAULT_CORE_CONFIG, DELAYS: {...DEFAULT_CORE_CONFIG.DELAYS, ZOOM_DEBOUNCE: delay}}},
        ],
    });
    const map = TestBed.inject(MapService);
    const view = new View({center: [0, 0], zoom: 5});
    map.setView(view);
    map.setViewportSize({extent: [0, 0, 1, 1], resolution: view.getResolution()!});
    TestBed.tick();
    return {map, view};
}

afterEach(() => {
    subscriptions.unsubscribe();
    TestBed.resetTestingModule();
    vi.useRealTimers();
});

it('publishes initial and panned request viewports immediately', () => {
    const {map, view} = createMap();
    const received = vi.fn();
    subscriptions.add(map.getRequestViewportStream().subscribe(received));
    expect(received).toHaveBeenCalledExactlyOnceWith(map.getViewportSize());
    view.beginInteraction();
    map.setViewportSize({extent: [1, 1, 2, 2], resolution: view.getResolution()!});
    expect(received).toHaveBeenLastCalledWith(map.getViewportSize());
    expect(received).toHaveBeenCalledTimes(2);
});

it('cancels synchronously and publishes the final request viewport on the configured deadline', async () => {
    const {map, view} = createMap(200);
    const requests = vi.fn();
    const rawViewport = vi.fn();
    subscriptions.add(map.getRequestViewportStream().subscribe(requests));
    subscriptions.add(map.getViewportSizeStream().subscribe(rawViewport));
    const signal = map.getZoomAbortSignal();
    const final: ViewportSize = {extent: [10, 10, 12, 12], resolution: 100};
    map.registerMapComponent({emitViewportSize: (): void => map.setViewportSize(final)} as MapContainerComponent);

    view.setZoom(6);
    expect(signal.aborted).toBe(true);
    expect(requests).toHaveBeenLastCalledWith(undefined);
    const intermediate: ViewportSize = {extent: [0, 0, 2, 2], resolution: 200};
    map.setViewportSize(intermediate);
    expect(rawViewport).toHaveBeenLastCalledWith(intermediate);
    expect(requests).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(199);
    expect(requests).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(requests).toHaveBeenCalledTimes(3);
    expect(requests).toHaveBeenLastCalledWith(final);
    expect(map.getZoomAbortSignal().aborted).toBe(false);
});

it('allows apps to disable zoom debounce through configuration', async () => {
    const {map, view} = createMap(0);
    const requests = vi.fn();
    subscriptions.add(map.getRequestViewportStream().subscribe(requests));
    view.setZoom(6);
    map.setViewportSize({extent: [0, 0, 2, 2], resolution: view.getResolution()!});
    expect(requests).toHaveBeenCalledTimes(2);
    expect(requests).not.toHaveBeenCalledWith(undefined);
    await vi.advanceTimersByTimeAsync(1000);
    expect(requests).toHaveBeenCalledTimes(2);
});
