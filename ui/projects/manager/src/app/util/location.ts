/**
 * Generates a redirect URI for OIDC authentication flows.
 * @param route The route to append to the redirect URI.
 * @param managerBasePath The manager route prefix, e.g. `/manager` when embedded in GIS.
 * @param baseHref The application's deployment base href.
 * @returns The full redirect URI including the specified route.
 */
export function oidcRedirectPath(location: Location, route: string, managerBasePath = '', baseHref = '/'): string {
    // ponytail: use explicit route metadata; the current pathname also contains the active manager route.
    const path = `${managerBasePath.replace(/^\/+|\/+$/g, '')}/${route.replace(/^\/+/, '')}`;
    return new URL(path, new URL(baseHref, location.origin)).toString();
}
