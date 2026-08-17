/**
 * Generates a redirect URI for OIDC authentication flows.
 * @param route The route to append to the redirect URI.
 * @param managerBasePath The manager route prefix, e.g. `/manager` when embedded in GIS.
 * @param baseHref The application's deployment base href.
 * @returns The full redirect URI including the specified route.
 * @example
 * Standalone manager at `https://example.com/`:
 * `oidcRedirectPath(location, '/oidc-popup')` returns
 * `https://example.com/oidc-popup`.
 * @example
 * Manager embedded in GIS at `/manager`:
 * `oidcRedirectPath(location, '/oidc-popup', '/manager')` returns
 * `https://example.com/manager/oidc-popup`.
 * @example
 * GIS deployed below `/gis/`:
 * `oidcRedirectPath(location, '/oidc-popup', '/manager', '/gis/')` returns
 * `https://example.com/gis/manager/oidc-popup`.
 */
export function oidcRedirectPath(location: Location, route: string, managerBasePath = '', baseHref = '/'): string {
    // ponytail: use explicit route metadata; the current pathname also contains the active manager route.
    let managerPath = managerBasePath;
    // Remove the leading slash before resolving the path relative to baseHref.
    if (managerPath.startsWith('/')) {
        managerPath = managerPath.substring(1);
    }
    // Avoid creating a double slash between the manager prefix and the route.
    if (managerPath.endsWith('/')) {
        managerPath = managerPath.substring(0, managerPath.length - 1);
    }

    let routePath = route;
    // Normalize the route so it can be appended to the manager prefix.
    if (routePath.startsWith('/')) {
        routePath = routePath.substring(1);
    }

    const path = `${managerPath}/${routePath}`;
    return new URL(path, new URL(baseHref, location.origin)).toString();
}
