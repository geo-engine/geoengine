/** @type {import('tailwindcss').Config} */
export default {
    content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}', './node_modules/preline/preline.js'],
    // theme: {
    //   extend: {
    //     fontFamily: {
    //       sans: [
    //         "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica Neue, Arial, Noto Sans, sans-serif, Apple Color Emoji, Segoe UI Emoji, Segoe UI Symbol, Noto Color Emoji",
    //       ],
    //     },
    //   },
    // },
    plugins: [
        // require("@tailwindcss/forms"),
        require('preline/plugin'),
    ],
};
