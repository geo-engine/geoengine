import {describe, expect, it} from 'vitest';
import {oidcRedirectPath} from './location';

const urlToLocation = (url: URL): Location => ({origin: url.origin, pathname: url.pathname}) as Location;

describe('oidcRedirectPath', () => {
    it('should generate correct redirect URI at the root', () => {
        const location = urlToLocation(new URL('https://example.com/navigation'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route, 'navigation');
        expect(redirectUri).toBe('https://example.com/oidc-popup');
    });

    it('should honor a mounted manager path', () => {
        const location = urlToLocation(new URL('https://example.com/gis/manager/navigation'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route, 'navigation');
        expect(redirectUri).toBe('https://example.com/gis/manager/oidc-popup');
    });

    it('should normalize path separators', () => {
        const location = urlToLocation(new URL('https://example.com/gis/manager/navigation/'));

        const route = '/oidc-popup';
        const redirectUri = oidcRedirectPath(location, route, 'navigation');
        expect(redirectUri).toBe('https://example.com/gis/manager/oidc-popup');
    });

    it('should reject a route that is not in the current URL', () => {
        const location = urlToLocation(new URL('https://example.com/gis/manager/navigation'));

        expect(() => oidcRedirectPath(location, '/oidc-popup', 'signin')).toThrowError(
            '[WildLIVE OIDC] current route does not match pathname',
        );
    });

    it('should use the manager mount when navigation is not in the URL', () => {
        const location = urlToLocation(new URL('https://example.com/gis/manager'));

        const redirectUri = oidcRedirectPath(location, '/oidc-popup', 'navigation');
        expect(redirectUri).toBe('https://example.com/gis/manager/oidc-popup');
    });
});
