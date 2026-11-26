import {unByKey} from 'ol/Observable';
import View from 'ol/View';

/** Holds new tile requests until a zoom has settled; center changes alone do not delay loading. */
export class TileZoomDebounce {
    private timer?: ReturnType<typeof setTimeout>;
    private readonly waiting = new Set<() => void>();
    private readonly listener;

    constructor(
        private readonly view: View,
        private readonly delay: number,
        onZoom: () => void,
    ) {
        this.listener = view.on('change:resolution', () => {
            clearTimeout(this.timer);
            this.timer = setTimeout(() => this.resume(), this.delay);
            onZoom();
        });
    }

    /** An aborted queued tile must never start a request when zooming finishes. */
    wait(signal: AbortSignal): Promise<void> {
        signal.throwIfAborted();
        if (this.timer === undefined) return Promise.resolve();

        return new Promise((resolve, reject) => {
            const finish = (): void => {
                signal.removeEventListener('abort', abort);
                this.waiting.delete(finish);
                resolve();
            };
            const abort = (): void => {
                this.waiting.delete(finish);
                reject(new DOMException('Aborted', 'AbortError'));
            };
            this.waiting.add(finish);
            signal.addEventListener('abort', abort, {once: true});
        });
    }

    dispose(): void {
        unByKey(this.listener);
        clearTimeout(this.timer);
        this.timer = undefined;
        for (const finish of this.waiting) finish();
    }

    private resume(): void {
        // A slow pinch or a held wheel gesture can pause without ending the zoom interaction.
        if (this.view.getAnimating() || this.view.getInteracting()) {
            this.timer = setTimeout(() => this.resume(), 50);
            return;
        }
        this.timer = undefined;
        for (const finish of this.waiting) finish();
    }
}
