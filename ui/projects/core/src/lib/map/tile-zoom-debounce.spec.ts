// @vitest-environment jsdom

import View from 'ol/View';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {TileZoomDebounce} from './tile-zoom-debounce';

describe('TileZoomDebounce', () => {
    let view: View;
    let debounce: TileZoomDebounce;
    let onPause: ReturnType<typeof vi.fn<(paused: boolean) => void>>;

    beforeEach(() => {
        vi.useFakeTimers();
        view = new View({center: [0, 0], zoom: 5});
        onPause = vi.fn();
        debounce = new TileZoomDebounce(view, 400, onPause);
    });

    afterEach(() => {
        debounce.dispose();
        vi.useRealTimers();
    });

    it('leaves initial loading and panning immediate, including while the mouse is held', async () => {
        view.beginInteraction();
        view.setCenter([100, 100]);
        await vi.advanceTimersByTimeAsync(1000);
        expect(onPause).not.toHaveBeenCalled();
    });

    it.each([7, 4])('restarts the delay on every zoom change and resumes at zoom %s', async (finalZoom: number) => {
        view.setZoom(6);
        await vi.advanceTimersByTimeAsync(300);
        view.setZoom(finalZoom);
        await vi.advanceTimersByTimeAsync(399);
        expect(onPause.mock.calls).toEqual([[true], [true]]);
        await vi.advanceTimersByTimeAsync(1);
        expect(onPause.mock.calls).toEqual([[true], [true], [false]]);
    });

    it('holds requests through a paused pinch gesture without polling', async () => {
        view.beginInteraction();
        view.setZoom(6);
        await vi.advanceTimersByTimeAsync(1000);
        expect(onPause.mock.calls).toEqual([[true]]);
        expect(vi.getTimerCount()).toBe(0);
        view.endInteraction(0);
        expect(onPause).toHaveBeenLastCalledWith(false);
    });

    it('waits for the delay when the zoom gesture finishes early', async () => {
        view.beginInteraction();
        view.setZoom(6);
        await vi.advanceTimersByTimeAsync(200);
        view.endInteraction(0);
        expect(onPause.mock.calls).toEqual([[true]]);
        await vi.advanceTimersByTimeAsync(200);
        expect(onPause).toHaveBeenLastCalledWith(false);
    });

    it('allows a pan started during the delay after a discrete zoom', async () => {
        view.setZoom(6);
        await vi.advanceTimersByTimeAsync(200);
        view.beginInteraction();
        view.setCenter([100, 100]);
        await vi.advanceTimersByTimeAsync(200);
        expect(onPause).toHaveBeenLastCalledWith(false);
        expect(view.getInteracting()).toBe(true);
    });

    it('allows a new pan after the zoom gesture ends during the delay', async () => {
        view.beginInteraction();
        view.setZoom(6);
        view.endInteraction(0);
        view.beginInteraction();
        view.setCenter([100, 100]);
        await vi.advanceTimersByTimeAsync(400);
        expect(onPause).toHaveBeenLastCalledWith(false);
    });

    it('waits through a zoom animation and the delay after its final frame', async () => {
        view.animate({zoom: 7, duration: 800});
        await vi.advanceTimersByTimeAsync(800);
        expect(onPause).not.toHaveBeenCalledWith(false);
        await vi.advanceTimersByTimeAsync(450);
        expect(onPause).toHaveBeenLastCalledWith(false);
    });

    it('clears its timer and view listeners when disposed', async () => {
        view.setZoom(6);
        debounce.dispose();
        await vi.advanceTimersByTimeAsync(1000);
        expect(vi.getTimerCount()).toBe(0);
        view.setZoom(7);
        view.beginInteraction();
        view.endInteraction(0);
        expect(onPause.mock.calls).toEqual([[true]]);
    });
});
