// @ts-check
import {defineConfig} from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import icon from 'astro-icon';
import starlight from '@astrojs/starlight';
import openApiOperatorsPlugin from './astro-openapi-plugin.ts';
import starlightLinksValidator from 'starlight-links-validator';
import pydocPlugin from './pydoc-plugin.ts';

// https://astro.build/config
export default defineConfig({
    site: 'https://www.geoengine.io',

    vite: {
        plugins: [tailwindcss()],
    },

    integrations: [
        icon(),
        openApiOperatorsPlugin({
            input: '../openapi.json',
            operatorsOutputDir: './src/content/docs/docs/operators',
            plotsOutputDir: './src/content/docs/docs/plots',
        }),
        pydocPlugin({
            outputDir: './src/content/docs/docs/python',
        }),
        starlight({
            plugins: [starlightLinksValidator()],
            title: 'Geo Engine Docs',
            description: 'Documentation for the Geo Engine project.',
            logo: {
                src: './src/images/GeoEngine_Bildmarke.svg',
            },
            favicon: './src/images/GeoEngine_Bildmarke.svg',
            components: {
                SiteTitle: './src/components/TitleOverride.astro',
            },
            expressiveCode: {
                shiki: {
                    langAlias: {
                        'rust,ignore': 'rust',
                    },
                },
            },
            sidebar: [
                {
                    label: 'Welcome to Geo Engine Docs',
                    link: '/docs/',
                },
                {
                    label: 'The Geo Engine',
                    autogenerate: {directory: 'docs/the-geo-engine'},
                    collapsed: false,
                },
                {
                    label: 'API',
                    autogenerate: {directory: 'docs/api'},
                    collapsed: false,
                },
                {
                    label: 'Datatypes',
                    autogenerate: {directory: 'docs/datatypes'},
                    collapsed: false,
                },
                {
                    label: 'Operators',
                    autogenerate: {directory: 'docs/operators'},
                    collapsed: false,
                },
                {
                    label: 'Plots',
                    autogenerate: {directory: 'docs/plots'},
                    collapsed: false,
                },
                {
                    label: 'Python Library',
                    autogenerate: {directory: 'docs/python'},
                    collapsed: true,
                },
            ],
        }),
    ],
});
