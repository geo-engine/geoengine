/**
 * Generates a redirect URI for OIDC authentication flows.
 * @param route The route to append to the redirect URI.
 * @param currentRoute The current Angular route, e.g. `navigation`.
 * @returns The full redirect URI, or `undefined` when the current route cannot be resolved.
 * @example
 * Standalone manager at `https://example.com/navigation`:
 * `oidcRedirectPath(location, '/oidc-popup', 'navigation')` returns
 * `https://example.com/oidc-popup`.
 * @example
 * Manager embedded in GIS at `https://example.com/gis/manager/navigation`:
 * `oidcRedirectPath(location, '/oidc-popup', 'navigation')` returns
 * `https://example.com/gis/manager/oidc-popup`.
 */
export function oidcRedirectPath(location: Location, route: string, currentRoute: string): string | undefined {
    // A trailing slash is equivalent for browser routes, but would prevent the
    // suffix check below from recognizing URLs such as `/manager/navigation/`.
    let pathname = location.pathname;
    while (pathname.endsWith('/')) {
        pathname = pathname.slice(0, -1);
    }
    if (!pathname) {
        pathname = '/';
    }

    const currentPath = currentRoute ? `/${currentRoute}` : '';
    let basePath: string | undefined;
    if (currentPath && pathname.endsWith(currentPath)) {
        // Remove the current Angular route, leaving the deployment and mount path:
        // `/gis/manager/navigation` becomes `/gis/manager`.
        basePath = pathname.slice(0, -currentPath.length);
    } else if (currentRoute === 'navigation') {
        // The navigation component can also be rendered at the application's mount
        // path before Angular has added `/navigation` to the browser URL.
        basePath = pathname === '/' ? '' : pathname;
    } else {
        // Never send an unverified root URI to Keycloak. A wrong but valid-looking
        // redirect URI is harder to diagnose than stopping the authentication flow.
        // eslint-disable-next-line no-console
        console.debug('[WildLIVE OIDC] current route does not match pathname', {
            normalizedPathname: pathname,
            currentPath,
        });
        return undefined;
    }

    // Split on `/` instead of relying on regular expressions so leading and
    // trailing slashes cannot create duplicate separators in the URI.
    const path = [basePath, route]
        .flatMap((part) => part.split('/'))
        .filter(Boolean)
        .join('/');

    return new URL(`/${path}`, location.origin).toString();
}
