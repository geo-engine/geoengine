import {Injectable, Injector, Signal, inject} from '@angular/core';
import {toObservable, toSignal} from '@angular/core/rxjs-interop';
import {Observable, concat, defer, distinctUntilChanged, exhaustMap, filter, map, of, share, switchMap, timer} from 'rxjs';
import {ProcessingGraphReadStatus} from '@geoengine/api-client';
import {UserService} from '@geoengine/common';
import {UUID} from '../backend/backend.model';
import {CoreConfig} from '../config.service';

/**
 * Tells whether a loading layer waits for reads that its data source rate limits, e.g. with
 * HTTP 429. The backend retries such reads until they succeed, so the tiles of the layer arrive
 * late, not never, and the layer can say why instead of looking stuck.
 */
@Injectable({
    providedIn: 'root',
})
export class ReadStatusService {
    private readonly userService = inject(UserService);
    private readonly config = inject(CoreConfig);

    /**
     * The rate limited processing graphs of the session. All layers share one poll, which only runs
     * while at least one of them waits for it.
     */
    private readonly statuses$: Observable<ReadonlyArray<ProcessingGraphReadStatus>> = defer(() =>
        timer(0, this.config.DELAYS.READ_STATUS.POLL_INTERVAL),
    ).pipe(
        exhaustMap(() => this.fetchStatuses()),
        share(),
    );

    /**
     * Whether the reads of the processing graph are rate limited. Asks the backend only once
     * `loading` has been `true` for `DELAYS.READ_STATUS.AFTER_LOADING` and as long as it stays so,
     * so a layer that loads fast costs nothing. `false` while the processing graph is unknown.
     *
     * Call it in an injection context, e.g. a field initializer, or pass an `injector`: the signal
     * lives as long as that context.
     */
    throttled(loading: Signal<boolean>, processingGraphId: Signal<UUID | undefined>, injector = inject(Injector)): Signal<boolean> {
        const id$ = toObservable(processingGraphId, {injector}).pipe(filter((id) => id !== undefined));
        return toSignal(this.throttledStream(toObservable(loading, {injector}), id$), {initialValue: false, injector});
    }

    private throttledStream(loading: Observable<boolean>, processingGraphId: Observable<UUID>): Observable<boolean> {
        return loading.pipe(
            distinctUntilChanged(),
            switchMap((isLoading) =>
                isLoading
                    ? concat(
                          of(false),
                          timer(this.config.DELAYS.READ_STATUS.AFTER_LOADING).pipe(
                              switchMap(() => processingGraphId),
                              switchMap((id) =>
                                  this.statuses$.pipe(map((statuses) => statuses.some((status) => status.processingGraphId === id))),
                              ),
                          ),
                      )
                    : of(false),
            ),
            distinctUntilChanged(),
        );
    }

    /** A failed request only means that there is no hint to show, so it counts as no rate limit. */
    private async fetchStatuses(): Promise<ReadonlyArray<ProcessingGraphReadStatus>> {
        try {
            return await this.userService.sessionApi().readStatusHandler();
        } catch {
            return [];
        }
    }
}
