import {afterEach, describe, expect, it, vi} from 'vitest';
import {AppConfig, DEFAULT_VIEWPORT_LIMITS} from './app-config.service';

describe('EOdyssey viewport configuration', () => {
    afterEach(() => vi.unstubAllGlobals());

    it('uses defaults and reads both numeric limits from assets/config.json', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{}')));
        const config = new AppConfig();
        await config.load();
        expect(config.VIEWPORT_LIMITS).toEqual(DEFAULT_VIEWPORT_LIMITS);
        vi.stubGlobal(
            'fetch',
            vi.fn().mockResolvedValue(new Response(JSON.stringify({VIEWPORT_LIMITS: {MAX_SOURCE_PIXELS: 1234, MAX_STAC_SEARCHES: 12}}))),
        );
        await config.load();
        expect(config.VIEWPORT_LIMITS).toEqual({MAX_SOURCE_PIXELS: 1234, MAX_STAC_SEARCHES: 12});
    });

    it.each([0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, '100'])('rejects invalid loading limits: %s', async (value) => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({VIEWPORT_LIMITS: {MAX_SOURCE_PIXELS: value}}))));
        await expect(new AppConfig().load()).rejects.toThrow('VIEWPORT_LIMITS');
    });
});
