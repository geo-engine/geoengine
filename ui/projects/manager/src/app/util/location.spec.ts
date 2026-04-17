import {oidcRedirectPath} from './location';

const urlToLocation = (url: URL): Location =>
    ({
        origin: url.origin,
        pathname: url.pathname,
        hash: url.hash,
    }) as Location;

describe('oidcRedirectPath', () => {
    it('should generate correct redirect URI without path', async () => {
        const location = urlToLocation(new URL('https://example.com'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route);
        await expect(redirectUri).toBe('https://example.com/#/oidc-popup');
    });

    it('should generate correct redirect URI with path', async () => {
        const location = urlToLocation(new URL('https://example.com/some/path'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route);
        await expect(redirectUri).toBe('https://example.com/some/path#/oidc-popup');
    });

    it('should generate correct redirect URI with path, with trailing slash', async () => {
        const location = urlToLocation(new URL('https://example.com/some/path/'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route);
        await expect(redirectUri).toBe('https://example.com/some/path/#/oidc-popup');
    });

    it('should generate correct redirect URI under /manager subpath', async () => {
        const location = urlToLocation(new URL('https://example.com/#/manager'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route);
        await expect(redirectUri).toBe('https://example.com/#/manager/oidc-popup');
    });
});
