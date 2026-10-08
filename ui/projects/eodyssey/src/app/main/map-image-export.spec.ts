import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {addCitationToMapImage, goldenRatioSides} from './map-image-export';

describe('MainComponent', () => {
    beforeEach(() => {
        vi.restoreAllMocks();
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('adds the selected data source citation to the exported PNG', async () => {
        const fillText = vi.fn();
        const drawImage = vi.fn();
        const fillRect = vi.fn();
        const measureText = vi.fn((value: string) => ({width: value.length * 10}));

        const canvas = {
            width: 0,
            height: 0,
            getContext: vi.fn(() => ({
                drawImage,
                fillText,
                fillRect,
                clearRect: vi.fn(),
                measureText,
                font: '',
                fillStyle: '',
                textBaseline: '',
                save: vi.fn(),
                restore: vi.fn(),
                scale: vi.fn(),
                translate: vi.fn(),
                setTransform: vi.fn(),
                clip: vi.fn(),
                beginPath: vi.fn(),
                arc: vi.fn(),
                moveTo: vi.fn(),
                lineTo: vi.fn(),
                rect: vi.fn(),
                closePath: vi.fn(),
                stroke: vi.fn(),
                fill: vi.fn(),
            })),
            toDataURL: vi.fn(() => 'data:image/png;base64,captioned'),
        };

        const originalCreateElement = document.createElement.bind(document);
        vi.spyOn(document, 'createElement').mockImplementation((tagName: string, options?: ElementCreationOptions) => {
            if (tagName === 'canvas') {
                return canvas as unknown as HTMLCanvasElement;
            }

            return originalCreateElement(tagName, options);
        });

        class MockImage {
            public naturalWidth = 800;
            public naturalHeight = 600;
            public onload: (() => void) | null = null;
            private _src = '';

            set src(value: string) {
                this._src = value;
                this.onload?.();
            }

            get src(): string {
                return this._src;
            }
        }

        vi.stubGlobal('Image', MockImage);

        const result = await addCitationToMapImage('data:image/png;base64,raw', 'Copernicus Sentinel data [Year]');

        expect(result).toBe('data:image/png;base64,captioned');
        expect(drawImage).toHaveBeenCalled();
        expect(fillText).toHaveBeenCalledWith(
            'Copernicus Sentinel data [Year]',
            expect.any(Number),
            expect.any(Number),
            expect.any(Number),
        );
    });

    it('calculates golden ratio sides', () => {
        const totalWidth = 1000;
        const [a, b] = goldenRatioSides(totalWidth);
        expect(a + b).toBeCloseTo(totalWidth);
        expect(a).toBeGreaterThan(b);
        expect(a / b).toBeCloseTo((1 + Math.sqrt(5)) / 2, 5);
    });
});
