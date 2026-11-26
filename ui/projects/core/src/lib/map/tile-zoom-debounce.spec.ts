// @vitest-environment jsdom

import View from 'ol/View';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {TileZoomDebounce} from './tile-zoom-debounce';

describe('TileZoomDebounce', () => {
    let view: View;
    let debounce: TileZoomDebounce;
    let onZoom: ReturnType<typeof vi.fn<() => void>>;

    beforeEach(() => {
        vi.useFakeTimers();
        view = new View({center: [0, 0], zoom: 5});
        onZoom = vi.fn();
        debounce = new TileZoomDebounce(view, 400, onZoom);
    });

    afterEach(() => {
        debounce.dispose();
        vi.useRealTimers();
    });

    it('loads immediately initially and during a pan, including while the mouse is held', async () => {
        await debounce.wait(new AbortController().signal);
        view.beginInteraction();
        view.setCenter([100, 100]);
        const ready = vi.fn();
        void debounce.wait(new AbortController().signal).then(ready);
        await vi.advanceTimersByTimeAsync(0);
        expect(ready).toHaveBeenCalledOnce();
        expect(onZoom).not.toHaveBeenCalled();
    });

    it.each([7, 4])('restarts the delay on every zoom change and resumes at zoom %s', async (finalZoom: number) => {
        view.setZoom(6);
        const ready = vi.fn();
        void debounce.wait(new AbortController().signal).then(ready);
        await vi.advanceTimersByTimeAsync(300);
        view.setZoom(finalZoom);
        await vi.advanceTimersByTimeAsync(399);
        expect(ready).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(1);
        expect(ready).toHaveBeenCalledOnce();
        expect(onZoom).toHaveBeenCalledTimes(2);
    });

    it('holds requests through a paused pinch gesture', async () => {
        view.beginInteraction();
        view.setZoom(6);
        const ready = vi.fn();
        void debounce.wait(new AbortController().signal).then(ready);
        await vi.advanceTimersByTimeAsync(1000);
        expect(ready).not.toHaveBeenCalled();
        view.endInteraction(0);
        await vi.advanceTimersByTimeAsync(50);
        expect(ready).toHaveBeenCalledOnce();
    });

    it('waits through a zoom animation and the delay after its final frame', async () => {
        view.animate({zoom: 7, duration: 800});
        await vi.advanceTimersByTimeAsync(100);
        const ready = vi.fn();
        void debounce.wait(new AbortController().signal).then(ready);
        await vi.advanceTimersByTimeAsync(700);
        expect(ready).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(450);
        expect(ready).toHaveBeenCalledOnce();
    });

    it('removes an aborted waiter before zooming finishes', async () => {
        view.setZoom(6);
        const controller = new AbortController();
        const result = expect(debounce.wait(controller.signal)).rejects.toMatchObject({name: 'AbortError'});
        controller.abort();
        await result;
        await vi.advanceTimersByTimeAsync(400);
    });

    it('clears its timer and view listener when disposed', async () => {
        view.setZoom(6);
        const ready = vi.fn();
        void debounce.wait(new AbortController().signal).then(ready);
        debounce.dispose();
        await vi.advanceTimersByTimeAsync(0);
        expect(ready).toHaveBeenCalledOnce();
        expect(vi.getTimerCount()).toBe(0);
        view.setZoom(7);
        expect(onZoom).toHaveBeenCalledOnce();
    });
});
