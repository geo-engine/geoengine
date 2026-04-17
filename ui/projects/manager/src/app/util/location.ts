/**
 * Generates a redirect URI for OIDC authentication flows.
 * @param route The route to append to the redirect URI.
 * @returns The full redirect URI including the specified route.
 */
export function oidcRedirectPath(location: Location, route: string): string {
    const IS_HASHTAG_ROUTING = true;
    const IS_UNDER_MANAGER_SUBPATH = location.hash.startsWith('#/manager');

    let redirectUri = location.origin;
    if (location.pathname) {
        redirectUri += location.pathname;
    }
    if (IS_HASHTAG_ROUTING) {
        redirectUri += `#`;
    }
    if (IS_UNDER_MANAGER_SUBPATH) {
        redirectUri += `/manager`;
    }

    redirectUri += route;

    return redirectUri;
}
