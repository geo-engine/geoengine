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
            return 'export default {}; export const createCanvas = () => {};';
        }
        return undefined;
    },
};

export default defineConfig({
    plugins: [MOCK_CANVAS_PLUGIN],
});
