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

    const canvas = document.createElement('canvas');
    const width = image.naturalWidth || image.width;
    const height = image.naturalHeight || image.height;
    const context = canvas.getContext('2d');

    if (!context) {
        return mapImage;
    }

    canvas.width = width;
    canvas.height = height;

    context.drawImage(image, 0, 0, width, height);

    const fontSize = Math.max(12, Math.round(Math.min(width, height) / 60));
    const margin = 8;
    const lineHeight = fontSize * 1.2;
    const maxTextWidth = Math.min(width * 0.7, 600);
    const lines = wrapText(context, citation, maxTextWidth);

    context.font = `600 ${fontSize}px sans-serif`;
    context.textBaseline = 'middle';
    context.fillStyle = 'rgba(255, 255, 255, 0.9)';

    const lastLineWidth = Math.max(...lines.map((line) => context.measureText(line).width));
    const textHeight = lines.length * lineHeight;
    const x = width - lastLineWidth - margin;
    const y = height - textHeight - margin;

    lines.forEach((line, index) => {
        const lineY = y + lineHeight * (index + 0.5);
        context.fillText(line, x, lineY, maxTextWidth);
    });

    return canvas.toDataURL('image/png');
}
