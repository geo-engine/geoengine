import {Injectable, inject} from '@angular/core';
import {
    ComputationQuota,
    AuthCodeRequestURL,
    Configuration,
    DefaultConfig,
    OperatorQuota,
    GeneralApi,
    RoleDescription,
    ServerInfo,
    SessionApi,
    UserApi,
    UserSession,
} from '@geoengine/api-client';
import {
    BehaviorSubject,
    Observable,
    ReplaySubject,
    catchError,
    combineLatest,
    filter,
    first,
    firstValueFrom,
    from,
    map,
    mergeMap,
    of,
} from 'rxjs';
import {Location} from '@angular/common';
import {UUID} from '../datasets/dataset.model';
import {isDefined} from '../util/conversions';
import {ActivatedRoute, Router} from '@angular/router';
import {HttpErrorResponse} from '@angular/common/http';
import {Session} from './session.model';
import {BackendStatus, Quota, User} from './user.model';
import {utc} from 'moment';
import {CommonConfig} from '../config.service';
import {NotificationService} from '../notification.service';

/**
 * A service that is responsible for retrieving user information and modifying the current user.
 */
@Injectable({
    providedIn: 'root',
})
export class UserService {
    static readonly OIDC_RESTORE_ROUTE_KEY = 'oidcRestoreRoute';
    static readonly IS_HASH_SUFFIX = '#/';
    static readonly SESSION_KEY_SUFFIX = 'session';

    protected readonly config = inject(CommonConfig);
    protected readonly notificationService = inject(NotificationService);
    protected readonly router = inject(Router);
    protected readonly activatedRoute = inject(ActivatedRoute);
    protected readonly location = inject(Location);

    protected readonly session$ = new ReplaySubject<Session | undefined>(1);
    protected readonly backendStatus$ = new BehaviorSubject<BackendStatus>({available: false, initial: true});
    protected readonly backendInfo$ = new BehaviorSubject<ServerInfo | undefined>(undefined);
    protected readonly sessionQuota$ = new BehaviorSubject<Quota | undefined>(undefined);
    protected readonly refreshSessionQuota$ = new BehaviorSubject<void>(undefined);

    userApi = new ReplaySubject<UserApi>(1);
    sessionApi = new ReplaySubject<SessionApi>(1);
    backendApi = new GeneralApi();

    protected logoutCallback?: () => void;
    protected sessionInitialized = false;

    constructor() {
        // get oidc paramters from url before routing is enabled
        const oidcParams = this.getOidcParametersFromUrl();

        this.session$.subscribe((session) => {
            // storage of the session
            this.saveSessionInBrowser(session);
            if (!session) return;
            this.userApi.next(new UserApi(apiConfigurationWithAccessKey(session.sessionToken)));
            this.sessionApi.next(new SessionApi(apiConfigurationWithAccessKey(session.sessionToken)));
        });

        this.getBackendStatus()
            .pipe(mergeMap((status, _index) => this.setBackendInfo(status).then(() => this.tryLogin(status, oidcParams))))
            .subscribe();

        // update quota when session changes or update is triggered
        this.createSessionQuotaStream();

        // now, trigger an update of the backend status once. While this is an async call, we don't need to await here.
        void this.triggerBackendStatusUpdate();
    }

    async setBackendInfo(status: BackendStatus): Promise<void> {
        if (status.available) {
            const info = await this.backendApi.serverInfoHandler();
            this.backendInfo$.next(info);
        }
    }

    async tryLogin(
        status: BackendStatus,
        oidcParams:
            | {
                  sessionState: string;
                  code: string;
                  state: string;
              }
            | undefined,
    ): Promise<void> {
        // if the backend is not ready, we cannot do anything
        if (status.initial) {
            return;
        }

        if (!status.available) {
            if (this.sessionInitialized) {
                this.notificationService.error('Session close caused by backend shutdown');
            } else {
                this.notificationService.error('Backend is not available');
            }

            this.sessionInitialized = false;
            this.session$.next(undefined);
            return;
        }

        this.sessionInitialized = true;

        const oidcRestoreRoute = sessionStorage.getItem(UserService.OIDC_RESTORE_ROUTE_KEY);
        if (oidcParams && oidcRestoreRoute) {
            const sess = await this.oidcLogin(oidcParams);
            this.session$.next(sess);
            this.router.navigateByUrl(oidcRestoreRoute);
        } else {
            try {
                // restore old session if possible
                const session = await this.sessionFromBrowser(this.config.USER.AUTO_GUEST_LOGIN);
                this.session$.next(session);
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
            } catch (error: any) {
                // only show error if we did not expect it
                if (
                    error instanceof HttpErrorResponse &&
                    error.error &&
                    typeof error.error === 'object' &&
                    'error' in error.error &&
                    (error.error as {error?: string}).error !== 'Unauthorized'
                ) {
                    this.notificationService.error(error.message);
                }
                this.session$.next(undefined);
            }
        }
    }

    async triggerBackendStatusUpdate(): Promise<void> {
        try {
            const _available = await this.backendApi.availableHandler();

            this.backendStatus$.next({available: true});
        } catch (error) {
            this.backendStatus$.next({available: false, httpError: error as HttpErrorResponse});
        }
    }

    getBackendStatus(): Observable<BackendStatus> {
        return this.backendStatus$;
    }

    async sessionFromBrowser(fallbackToGuest: boolean): Promise<Session | undefined> {
        try {
            return await this.restoreSessionFromBrowser();
        } catch {
            if (fallbackToGuest) {
                return this.createGuestUser();
            } else {
                return undefined;
            }
        }
    }

    async createGuestUser(): Promise<Session> {
        const response = await new SessionApi().anonymousHandler();
        return this.sessionFromDict(response);
    }

    /**
     * @returns Retrieve a stream that notifies about the current session.
     */
    getSessionStream(): Observable<Session> {
        return this.session$.pipe(filter(isDefined));
    }

    /**
     * @returns Retrieve a stream that notifies about the current session.
     *          May be undefined if there is no current session.
     */
    getSessionOrUndefinedStream(): Observable<Session | undefined> {
        return this.session$;
    }

    getSessionTokenStream(): Observable<UUID> {
        return this.getSessionStream().pipe(map((session) => session.sessionToken));
    }

    getSessionTokenForRequest(): Observable<UUID> {
        return this.getSessionTokenStream().pipe(first());
    }

    /**
     * Returns a stream that notifies about the current session quota.
     * May be undefined if there is no current session or the backend does not use quotas.
     *
     * @returns Observable<Quota | undefined>
     **/
    getSessionQuotaStream(): Observable<Quota | undefined> {
        this.refreshSessionQuota();
        return this.sessionQuota$;
    }

    /**
     * triggers a refresh of the session quota
     *
     * @returns void
     **/
    refreshSessionQuota(): void {
        this.refreshSessionQuota$.next();
    }

    getBackendInfoStream(): Observable<ServerInfo | undefined> {
        return this.backendInfo$;
    }

    /**
     * This calls Angulars prepareExternalUrl for route "/".
     * @returns the url string
     **/
    getSpaExternalUrl(): string {
        return this.location.prepareExternalUrl('/');
    }

    /**
     * This returns the url incl. base path of the single page application.
     * We use it for routung and restoring after OIDC login.
     * In most cases it is 'https://abc.app.geoengine.io/'.
     * If angulars '--base-href=something' is used, it is 'https://abc.app.geoengine.io/something/'.
     *
     * @returns the url string
     */
    getSpaExternalUrlWithDomain(): string {
        return window.location.origin + this.getSpaBaseHref();
    }

    /**
     * This returns only the APP_BASE_HREF of the application.
     * Since the APP_BASE_HREF is not directly accessable, we get it from the external url.
     * @returns the APP_BASE_HREF
     */
    getSpaBaseHref(): string {
        const baseHrefUrl = this.getSpaExternalUrl();
        if (!baseHrefUrl.startsWith('/')) {
            // Angular allows to set APP_BASE_HREF to start with the origin. If that happens, this must be covered here!
            throw new Error("base_href must start with '/'");
        }
        if (baseHrefUrl.endsWith('#')) {
            return baseHrefUrl.substring(0, baseHrefUrl.length - 2);
        }
        return baseHrefUrl;
    }

    /**
     * For local storage we use the APP_BASE_HREF as part of the key. (To be able to use multiple apps with one domain?)
     * To get a uniform key, we replace '/' and '-' with '_'.
     * @returns APP_BASE_HREF with '/' and '-' replaces by '_'.
     */
    getBaseHrefBasedKey(): string {
        //
        return this.getSpaBaseHref().replace(/\//g, '_').replace(/-/g, '_');
    }

    /**
     * This allows to identify if hash or path routing is used.
     * @returns 'true' if hash based routing is used.
     */
    isHashRouting(): boolean {
        return this.getSpaExternalUrl().endsWith(UserService.IS_HASH_SUFFIX);
    }

    isGuestUserStream(): Observable<boolean> {
        return this.getSessionStream().pipe(map((s) => !s.user?.email || !s.user.realName));
    }

    /**
     * Login using user credentials. If it was successful, set a new user.
     *
     * @param credentials.user The user name.
     * @param credentials.password The user's password.
     * @returns a 'Session' if login completes. Throws otherwise.
     */
    async login(userCredentials: {email: string; password: string}): Promise<Session> {
        const response = await new SessionApi().loginHandler({
            userCredentials,
        });
        const session = this.sessionFromDict(response);

        this.session$.next(session);

        return session;
    }

    async guestLogin(): Promise<Session> {
        const oldSession = await firstValueFrom(this.session$);

        if (oldSession) {
            await new SessionApi(oldSession.apiConfiguration).logoutHandler();
        }

        try {
            const session = await this.createGuestUser();
            this.session$.next(session);

            return session;
        } catch (error) {
            // failing on a guest login means we cannot do it,
            // so we are logged out
            this.session$.next(undefined);
            throw error;
        }
    }

    logout(): void {
        this.session$.next(undefined);
    }

    async computationsQuota(offset: number, limit: number): Promise<ComputationQuota[]> {
        const userApi = await firstValueFrom(this.userApi);

        return userApi.computationsQuotaHandler({
            offset,
            limit,
        });
    }

    async computationQuota(computation: UUID): Promise<OperatorQuota[]> {
        const userApi = await firstValueFrom(this.userApi);

        return userApi.computationQuotaHandler({
            computation,
        });
    }

    async createSessionWithToken(sessionToken: UUID): Promise<Session> {
        const response = await new SessionApi(apiConfigurationWithAccessKey(sessionToken)).sessionHandler();

        const session = this.sessionFromDict(response);

        this.session$.next(session);

        return session;
    }

    getSessionOnce(): Observable<Session> {
        return this.getSessionStream().pipe(first());
    }

    /**
     * Login using user credentials. If it was successful, set a new user.
     *
     * @param session.user The user name.
     * @param session.password The user's password.
     * @returns `true` if the session is valid, `false` otherwise.
     */
    isSessionValid(session: Session): Observable<boolean> {
        return from(
            new SessionApi(session.apiConfiguration)
                .sessionHandler()
                .then((_response) => true)
                .catch((_error) => false),
        );
    }

    async isLoggedIn(): Promise<boolean> {
        return await firstValueFrom(this.session$).then(isDefined);
    }

    /**
     * This callback is called when the user is logged out.
     * This can be used to re-route to login pages.
     */
    setLogoutCallback(callback: () => void): void {
        this.logoutCallback = callback;
    }

    oidcInit(oidcRestoreRoute: string): Promise<AuthCodeRequestURL> {
        sessionStorage.setItem(UserService.OIDC_RESTORE_ROUTE_KEY, oidcRestoreRoute);

        return new SessionApi().oidcInit({
            redirectUri: this.getSpaExternalUrlWithDomain(),
        });
    }

    oidcLogin(request: {sessionState: string; code: string; state: string}): Promise<Session> {
        const sess = new SessionApi()
            .oidcLogin({
                authCodeResponse: request,
                redirectUri: this.getSpaExternalUrlWithDomain(),
            })
            .then((response) => {
                const session = this.sessionFromDict(response);
                sessionStorage.removeItem(UserService.OIDC_RESTORE_ROUTE_KEY);
                return session;
            });

        return sess;
    }

    saveSettingInLocalStorage(keyValue: string, setting: string): void {
        localStorage.setItem(this.getBaseHrefBasedKey() + keyValue, setting);
    }

    getSettingFromLocalStorage(keyValue: string): string | null {
        return localStorage.getItem(this.getBaseHrefBasedKey() + keyValue);
    }

    /**
     * Returns a stream that notifies about the current roles of the user.
     * May be undefined if there is no current session.
     *
     * @returns Observable<Array<RoleDescription> | undefined>
     **/
    getRoleDescriptions(): Observable<Array<RoleDescription> | undefined> {
        return combineLatest([this.userApi, this.getSessionOrUndefinedStream()]).pipe(
            mergeMap(([userApi, session]) => {
                if (!session) return of(undefined);
                return from(userApi.getRoleDescriptions());
            }),
            catchError(() => of(undefined)),
        );
    }

    protected saveSessionInBrowser(session: Session | undefined): void {
        if (session) {
            localStorage.setItem(this.getBaseHrefBasedKey() + UserService.SESSION_KEY_SUFFIX, session.sessionToken);
        } else {
            localStorage.removeItem(this.getBaseHrefBasedKey() + UserService.SESSION_KEY_SUFFIX);
        }
    }

    protected restoreSessionFromBrowser(): Promise<Session> {
        const sessionToken = localStorage.getItem(this.getBaseHrefBasedKey() + UserService.SESSION_KEY_SUFFIX) ?? '';

        return this.createSessionWithToken(sessionToken);
    }

    async getRoleByName(roleName: string): Promise<UUID> {
        const userApi = await firstValueFrom(this.userApi);
        return userApi
            .getRoleByNameHandler({
                name: roleName,
            })
            .then((role) => role.id);
    }

    async registerUser(userRegistration: {email: string; password: string; realName: string}): Promise<string> {
        const sessionApi = await firstValueFrom(this.sessionApi);
        return sessionApi.registerUserHandler({userRegistration});
    }

    protected sessionFromDict(sessionDict: UserSession): Session {
        let user: User | undefined;
        if (sessionDict.user) {
            user = new User({
                id: sessionDict.user.id,
                email: sessionDict.user.email ?? undefined,
                realName: sessionDict.user.realName ?? undefined,
            });
        }

        const session: Session = {
            sessionToken: sessionDict.id,
            user,
            validUntil: utc(sessionDict.validUntil),
            lastProjectId: sessionDict.project ?? undefined,
            lastView: sessionDict.view ?? undefined,
            apiConfiguration: apiConfigurationWithAccessKey(sessionDict.id),
        };

        return session;
    }

    private createSessionQuotaStream(): void {
        combineLatest([this.userApi, this.getSessionOrUndefinedStream(), this.refreshSessionQuota$])
            .pipe(
                mergeMap(([userApi, session, _update]) => {
                    if (!session) return of(undefined);
                    return from(userApi.quotaHandler());
                }),
                catchError(() => of(undefined)),
                map((quota) => (quota ? Quota.fromDict(quota) : undefined)),
            )
            .subscribe((quota) => {
                this.sessionQuota$.next(quota);
            });
    }

    private getOidcParametersFromUrl(): {sessionState: string; code: string; state: string} | undefined {
        const params = new URLSearchParams(window.location.search);
        const sessionState = params.get('session_state');
        const code = params.get('code');
        const state = params.get('state');

        if (sessionState && code && state) {
            return {sessionState, code, state};
        }

        return undefined;
    }
}

export const apiConfigurationWithAccessKey = (accessToken: string): Configuration =>
    new Configuration({
        basePath: DefaultConfig.basePath,
        fetchApi: DefaultConfig.fetchApi,
        middleware: DefaultConfig.middleware,
        queryParamsStringify: DefaultConfig.queryParamsStringify,
        username: DefaultConfig.username,
        password: DefaultConfig.password,
        apiKey: DefaultConfig.apiKey,
        accessToken: accessToken,
        headers: DefaultConfig.headers,
        credentials: DefaultConfig.credentials,
    });
