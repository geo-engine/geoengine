// @ts-check
import {defineConfig} from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import icon from 'astro-icon';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
    site: 'https://www.geoengine.io',

    vite: {
        plugins: [tailwindcss()],
    },

    integrations: [icon(), starlight({
        title: 'Geo Engine Docs',
        description: 'Documentation for the Geo Engine project.',
        logo: {
            src: './src/images/GeoEngine_Bildmarke.svg',
        },
        favicon:'./src/images/GeoEngine_Bildmarke.svg',
        sidebar: [
            {
          label: 'Welcome to Geo Engine Docs', // Or whatever title you want
          link: '/docs/', // Link to the page you want to show when this item is clicked
        },
        {
          label: 'The Geo Engine',
          autogenerate: { directory: 'docs/the-geo-engine' }, 
          collapsed: false,
        },
        {
          label: 'API',
          autogenerate: { directory: 'docs/api' }, 
          collapsed: false,
        },
        {
          label: 'Datatypes',
          autogenerate: { directory: 'docs/datatypes' }, 
          collapsed: false,
        },
        {
          label: 'Operators',
          // autogenerate: { directory: 'docs/operators' },
          items: [
            {
              label: 'BandFilter',
              link: '/docs/operators/bandfilter/',
            },
          ],
          collapsed: false,
        },
      ],
    })],
});