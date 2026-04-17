import {defineConfig} from 'vite';
import path from 'path';

export default defineConfig({
    resolve: {
        alias: {
            // vega-embed loads `NodeCanvas = await import("canvas");` which crashes in browser tests
            canvas: path.resolve(__dirname, 'empty-module.js'),
        },
    },
});
