/**
 * Wraps the given text to fit within the specified maximum width.
 * Returns an array of lines.
 */
function wrapText(context: CanvasRenderingContext2D, text: string, maxWidth: number): string[] {
    const words = text.split(/\s+/).filter(Boolean);
    if (words.length === 0) {
        return [''];
    }

    const lines: string[] = [];
    let currentLine = words[0];

    for (let index = 1; index < words.length; index++) {
        const candidate = `${currentLine} ${words[index]}`;
        if (context.measureText(candidate).width <= maxWidth) {
            currentLine = candidate;
            continue;
        }

        lines.push(currentLine);
        currentLine = words[index];
    }

    lines.push(currentLine);
    return lines;
}

/** Adds a citation to the given map image. */
export async function addCitationToMapImage(mapImage: string, citation: string): Promise<string> {
    if (!citation) return mapImage;

    const image = await new Promise<HTMLImageElement>((resolve, reject) => {
        const img = new Image();
        img.onload = (): void => resolve(img);
        img.onerror = (): void => reject(new Error('Failed to load map image for export.'));
        img.src = mapImage;
    });

    const width = image.naturalWidth || image.width;
    const height = image.naturalHeight || image.height;

    const canvas = document.createElement('canvas');
    canvas.width = width;
    canvas.height = height;

    const context = canvas.getContext('2d');
    if (!context) return mapImage;

    const MIN_FONT_SIZE = 12;
    const FONT_SCALE_DIVISOR = 60;
    const FONT_WEIGHT = 600;
    const FONT_FAMILY = 'sans-serif';
    const TEXT_IN_IMAGE_MARGIN = 8;

    const fontSize = Math.max(MIN_FONT_SIZE, Math.round(Math.min(width, height) / FONT_SCALE_DIVISOR));
    const lineHeight = fontSize * 1.2;
    const [maxTextWidth] = goldenRatioSides(width);

    context.drawImage(image, 0, 0, width, height);
    context.font = `${FONT_WEIGHT} ${fontSize}px ${FONT_FAMILY}`;
    context.textBaseline = 'middle';
    context.fillStyle = 'rgba(255, 255, 255, 0.9)';

    const lines = wrapText(context, citation, maxTextWidth);

    const lastLineWidth = Math.max(...lines.map((line) => context.measureText(line).width));
    const textHeight = lines.length * lineHeight;
    const x = width - lastLineWidth - TEXT_IN_IMAGE_MARGIN;
    const y = height - textHeight - TEXT_IN_IMAGE_MARGIN;

    lines.forEach((line, index) => {
        const lineY = y + lineHeight * (index + 0.5);
        context.fillText(line, x, lineY, maxTextWidth);
    });

    return canvas.toDataURL('image/png');
}

/** Calculates the `[long side, short side]` of a rectangle based on the golden ratio given the total width. */
export function goldenRatioSides(totalWidth: number): [number, number] {
    const b = totalWidth / (1 + (1 + Math.sqrt(5)) / 2);
    const a = totalWidth - b;
    return [a, b];
}
