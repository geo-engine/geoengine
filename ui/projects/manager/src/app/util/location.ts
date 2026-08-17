/**
 * Generates a redirect URI for OIDC authentication flows.
 * @param route The route to append to the redirect URI.
 * @returns The full redirect URI including the specified route.
 */
export function oidcRedirectPath(location: Location, route: string): string {
    let redirectUri = location.origin;
    if (location.pathname) {
        redirectUri += location.pathname;
    }

    redirectUri = redirectUri.replace(/\/+$/, '') + route;

    return redirectUri;
}
