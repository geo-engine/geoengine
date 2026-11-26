import {unByKey} from 'ol/Observable';
import View from 'ol/View';

/** Pauses viewport requests until the zoom's delay and gesture have both finished. */
export class TileZoomDebounce {
    private timer?: ReturnType<typeof setTimeout>;
    private paused = false;
    private zoomInteraction = false;
    private zoomAnimation = false;
    private resolvingConstraints = false;
    private disposed = false;
    private readonly listeners;

    constructor(
        view: View,
        delay: number,
        private readonly onPause: (paused: boolean) => void,
    ) {
        this.listeners = [
            view.on('change:resolution', () => {
                this.paused = true;
                this.zoomInteraction = view.getInteracting();
                this.zoomAnimation = view.getAnimating();
                clearTimeout(this.timer);
                this.timer = setTimeout(() => {
                    this.timer = undefined;
                    this.resume();
                }, delay);
                onPause(true);
            }),
            // View hints emit change when a gesture or animation ends, even without a new resolution.
            view.on('change', () => {
                const interactionEnded = this.zoomInteraction && !view.getInteracting();
                this.zoomInteraction &&= view.getInteracting();
                this.zoomAnimation &&= view.getAnimating();
                if (interactionEnded) {
                    // endInteraction clears its hint before starting the constrained zoom animation.
                    // Keep the gate closed until that synchronous transition has finished.
                    this.resolvingConstraints = true;
                    queueMicrotask(() => {
                        if (this.disposed) return;
                        this.resolvingConstraints = false;
                        this.zoomAnimation ||= view.getAnimating();
                        this.resume();
                    });
                }
                this.resume();
            }),
        ];
    }

    dispose(): void {
        this.disposed = true;
        unByKey(this.listeners);
        clearTimeout(this.timer);
    }

    private resume(): void {
        if (this.paused && this.timer === undefined && !this.resolvingConstraints && !this.zoomInteraction && !this.zoomAnimation) {
            this.paused = false;
            this.onPause(false);
        }
    }
}
