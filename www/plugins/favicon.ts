import type {Logger, Plugin} from 'vite';
import sharp from 'sharp';
import fs from 'node:fs/promises';
import path from 'node:path';

const SVG_PATH = path.resolve('public/favicon.svg');
const PUBLIC_DIR = path.resolve('public');

/**
 * Defines the target sizes and filenames for the favicons to be generated.
 */
const TARGET_SIZES = [
    {name: 'favicon-16x16.png', size: 16},
    {name: 'favicon-32x32.png', size: 32},
    {name: 'favicon-64x64.png', size: 64},
    {name: 'apple-touch-icon.png', size: 180},
    {name: 'android-chrome-192x192.png', size: 192},
    {name: 'android-chrome-512x512.png', size: 512},
] as const;

export interface FaviconPluginOptions {
    name: string;
    shortName: string;
    description: string;
    themeColor: string;
    backgroundColor: string;
}

/**
 * Generates favicons from a source SVG file and places them in the public directory.
 * The generated favicons (PNG) include various sizes for different platforms and a favicon.ico file.
 * Additionally, it creates a manifest.json file for Android PWA support.
 *
 * @returns A Vite plugin that generates favicons during the build process.
 */
export function generateFavicons(options: FaviconPluginOptions): Plugin {
    let logger: Logger;
    return {
        name: 'vite-plugin-generate-favicons',

        configResolved(config): void {
            logger = config.logger;
        },

        async buildStart(): Promise<void> {
            this.addWatchFile(SVG_PATH);
            await generate(logger, options);
        },
    };
}

/**
 * Generates favicons from the source SVG file and saves them in the public directory.
 * It creates PNG files of various sizes and a favicon.ico file.
 * Additionally, it generates a manifest.json file for Android PWA support.
 *
 * @param logger
 */
async function generate(logger: Logger, options: FaviconPluginOptions): Promise<void> {
    try {
        const svgBuffer = await fs.readFile(SVG_PATH);

        // Copy original SVG to public for modern browsers
        await fs.copyFile(SVG_PATH, path.join(PUBLIC_DIR, 'favicon.svg'));

        // Render PNGs at specified dimensions
        await Promise.all(
            TARGET_SIZES.map(({name, size}) => sharp(svgBuffer).resize(size, size).toFormat('png').toFile(path.join(PUBLIC_DIR, name))),
        );

        // Create favicon.ico from 16x16, 32x32, and 64x64 PNGs
        const icoPngBuffers = await pngsToBuffers(
            TARGET_SIZES.filter(({size}) => [16, 32, 64].includes(size)).map(({name}) => path.join(PUBLIC_DIR, name)),
        );
        const icoBuffer = await createIcoFromPngs(icoPngBuffers);
        await fs.writeFile(path.join(PUBLIC_DIR, 'favicon.ico'), icoBuffer);

        // Create Web App manifest file for PWA support
        const manifestContent = JSON.stringify(webAppManifest(options), null, 4) + '\n';
        await fs.writeFile(path.join(PUBLIC_DIR, 'manifest.json'), manifestContent);

        logger.info(`✓ Favicons successfully generated in ${PUBLIC_DIR}`);
    } catch (error) {
        logger.error('✕ Error generating favicons', {error: error as Error});
    }
}

/**
 * Reads PNG files from the specified paths and returns their buffers.
 * @param pngPaths
 * @returns A promise that resolves to an array of buffers for the PNG files.
 */
async function pngsToBuffers(pngPaths: string[]): Promise<Buffer[]> {
    return Promise.all(pngPaths.map((pngPath) => fs.readFile(pngPath)));
}

/**
 * Creates an ICO file from an array of PNG buffers.
 * @param pngBuffers
 * @returns A promise that resolves to a buffer containing the ICO file data.
 */
async function createIcoFromPngs(pngBuffers: Buffer[]): Promise<Buffer> {
    const numImages = pngBuffers.length;
    const headerSize = 6 + numImages * 16;

    const header = Buffer.alloc(6);
    header.writeUInt16LE(0, 0); // Reserved
    header.writeUInt16LE(1, 2); // Image type (1 = ICO)
    header.writeUInt16LE(numImages, 4); // Number of images

    const entries: Buffer[] = [];
    let offset = headerSize;

    for (const png of pngBuffers) {
        const metadata = await sharp(png).metadata();
        const entry = Buffer.alloc(16);

        entry.writeUInt8(metadata.width! >= 256 ? 0 : metadata.width!, 0);
        entry.writeUInt8(metadata.height! >= 256 ? 0 : metadata.height!, 1);
        entry.writeUInt8(0, 2); // Color palette
        entry.writeUInt8(0, 3); // Reserved
        entry.writeUInt16LE(1, 4); // Color planes
        entry.writeUInt16LE(32, 6); // Bits per pixel
        entry.writeUInt32LE(png.length, 8); // Size of PNG data
        entry.writeUInt32LE(offset, 12); // Offset of PNG data

        entries.push(entry);
        offset += png.length;
    }

    return Buffer.concat([header, ...entries, ...pngBuffers]);
}

interface WebAppManifest {
    name: string;
    short_name: string;
    description: string;
    start_url: string;
    display: 'standalone' | 'fullscreen' | 'minimal-ui' | 'browser';
    background_color: string;
    theme_color: string;
    icons: {
        src: string;
        sizes: string;
        type: string;
    }[];
}

/**
 * Generates a Web application manifest object for PWA support based on the provided options.
 * Cf. https://developer.mozilla.org/en-US/docs/Web/Progressive_web_apps/Manifest
 * @param options The favicon plugin options.
 * @returns A Web application manifest object.
 */
function webAppManifest(options: FaviconPluginOptions): WebAppManifest {
    return {
        name: options.name,
        short_name: options.shortName,
        description: options.description,
        start_url: '/',
        display: 'standalone',
        background_color: options.backgroundColor,
        theme_color: options.themeColor,
        icons: [
            {
                src: '/android-chrome-192x192.png',
                sizes: '192x192',
                type: 'image/png',
            },
            {
                src: '/android-chrome-512x512.png',
                sizes: '512x512',
                type: 'image/png',
            },
        ],
    } as const;
}
