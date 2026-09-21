import {defineConfig, Plugin} from 'vitest/config';

const MOCK_CANVAS_PLUGIN: Plugin = {
    name: 'mock-canvas',
    // 1. Vite encounters: import("canvas")
    resolveId(id) {
        if (id === 'canvas') {
            // Intercept 'canvas' and re-route it to virtual ID
            return '\0canvas-mock';
        }
        return undefined;
    },
    // 2. Vite sees '\0' -> skips file system lookup -> routes directly to load()
    load(id) {
        if (id === '\0canvas-mock') {
            // Return simulated browser-safe module contents
            return `
                const noop = () => {};
                const context = {
                    canvas: { width: 1, height: 1 },
                    fillStyle: '',
                    strokeStyle: '',
                    lineWidth: 1,
                    font: '',
                    textAlign: 'left',
                    textBaseline: 'alphabetic',
                    beginPath: noop,
                    arc: noop,
                    moveTo: noop,
                    lineTo: noop,
                    stroke: noop,
                    fill: noop,
                    fillRect: noop,
                    clearRect: noop,
                    drawImage: noop,
                    save: noop,
                    restore: noop,
                    scale: noop,
                    translate: noop,
                    setTransform: noop,
                    clip: noop,
                    rect: noop,
                    closePath: noop,
                    measureText: () => ({width: 0}),
                    createLinearGradient: () => ({addColorStop: noop}),
                    setLineDash: noop,
                    getImageData: () => ({data: new Uint8ClampedArray(4)}),
                };
                export default {getContext: () => context};
                export const createCanvas = () => ({getContext: () => context});
            `;
        }
        return undefined;
    },
};

export default defineConfig({
    plugins: [MOCK_CANVAS_PLUGIN],
    test: {
        coverage: {
            provider: 'v8',
        },
    },
});
