import {getImage} from 'astro:assets';

export interface OptimizedImage {
    src: string;
    width: number;
    mediaQuery?: string;
}

export const NO_IMAGE: OptimizedImage = {
    src: 'none',
    width: 0,
    mediaQuery: undefined,
} as const;

export interface OptimizedImageSet {
    sm: OptimizedImage;
    md: OptimizedImage;
    lg: OptimizedImage;
    xl: OptimizedImage;
    original: OptimizedImage;
}

export const NO_IMAGE_SET: OptimizedImageSet = {
    sm: NO_IMAGE,
    md: NO_IMAGE,
    lg: NO_IMAGE,
    xl: NO_IMAGE,
    original: NO_IMAGE,
} as const;

export async function optimizedBackgroundImage(src: ImageMetadata): Promise<OptimizedImageSet> {
    const mediaBreakpoints = [768, 1024, 1280, 1536]; // from tailwindcss
    const bg = await getImage({src, widths: mediaBreakpoints});

    const optimizedImages: OptimizedImage[] = [];

    for (const srcSet of bg.srcSet.values) {
        if (!srcSet.transform.width) {
            throw new Error('Image width is not defined');
        }
        optimizedImages.push({
            src: `url('${srcSet.url}')`,
            width: srcSet.transform.width,
            mediaQuery: `max-width: ${srcSet.transform.width}px`,
        });
    }

    if (!bg.options.width) {
        throw new Error('Image width is not defined');
    }

    optimizedImages.push({
        src: `url('${bg.src}')`,
        width: bg.options.width,
    });

    // sort ascending by width
    optimizedImages.sort((a, b) => a.width - b.width);

    const optimizedImagesSet = {
        sm: optimizedImages[0],
        md: optimizedImages[1],
        lg: optimizedImages[2],
        xl: optimizedImages[3],
        original: optimizedImages[4],
    };

    return optimizedImagesSet;
}
