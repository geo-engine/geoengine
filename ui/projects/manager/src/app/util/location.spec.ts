import {oidcRedirectPath} from './location';

const urlToLocation = (url: URL): Location => ({origin: url.origin}) as Location;

describe('oidcRedirectPath', () => {
    it('should generate correct redirect URI at the root', async () => {
        const location = urlToLocation(new URL('https://example.com'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route);
        await expect(redirectUri).toBe('https://example.com/oidc-popup');
    });

    it('should honor the manager mount path', async () => {
        const location = urlToLocation(new URL('https://example.com'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route, '/manager');
        await expect(redirectUri).toBe('https://example.com/manager/oidc-popup');
    });

    it('should honor the deployment base href', async () => {
        const location = urlToLocation(new URL('https://example.com'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route, '/manager', '/gis/');
        await expect(redirectUri).toBe('https://example.com/gis/manager/oidc-popup');
    });

    it('should honor a deployment base href without a trailing slash', async () => {
        const location = urlToLocation(new URL('https://example.com'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route, '/manager', '/gis');
        await expect(redirectUri).toBe('https://example.com/gis/manager/oidc-popup');
    });
});
