import {BehaviorSubject, Observable, Subject, Subscription} from 'rxjs';
import {map} from 'rxjs/operators';
import {Injectable, ElementRef, OutputEmitterRef} from '@angular/core';
import {SidenavConfig} from '../layout.service';

/**
 * This service provides means to interact with the sidenav container component.
 */
@Injectable()
export class SidenavRef {
    private title$ = new BehaviorSubject<string | undefined>(undefined);
    private backButtonComponent$ = new BehaviorSubject<SidenavConfig | undefined>(undefined);

    private searchElements$ = new BehaviorSubject<readonly ElementRef[] | undefined>(undefined);
    private searchElementsSubscription?: Subscription;
    private searchString$?: OutputEmitterRef<string>;

    private close$ = new Subject<void>();

    /**
     * Set the toolbar title
     */
    setTitle(title: string | undefined): void {
        this.title$.next(title);
    }

    /**
     * Get events of title changes
     */
    getTitleStream(): Observable<string> {
        return this.title$.pipe(map((title) => title ?? ''));
    }

    /**
     * Set the component that should be loaded upon clicking "back"
     */
    setBackButtonComponent(component: SidenavConfig | undefined): void {
        this.backButtonComponent$.next(component);
    }

    /**
     * Get events upon back button component changes
     */
    getBackButtonComponentStream(): Observable<SidenavConfig | undefined> {
        return this.backButtonComponent$;
    }

    /**
     * Retrieve the current back button component
     */
    getBackButtonComponent(): SidenavConfig | undefined {
        return this.backButtonComponent$.getValue();
    }

    /**
     * Setup the search via the `SidenavSearchComponent`
     *
     * @param contentChildren provides a reference to a `QueryList`
     * @param searchString$ this emits search inputs to upon changes to the query list
     */
    setSearch(contentChildren: readonly ElementRef[], searchString$: OutputEmitterRef<string>): void {
        this.removeSearch();

        this.searchElements$.next(contentChildren);
        this.searchString$ = searchString$;
    }

    /**
     * Unset the search setup
     */
    removeSearch(): void {
        if (this.searchElementsSubscription) {
            this.searchElementsSubscription.unsubscribe();
        }
        if (this.searchString$) {
            this.searchString$ = undefined;
        }

        this.searchElements$.next(undefined);
    }

    /**
     * Retrieve the current `SidenavSearchComponent` as a stream
     */
    getSearchComponentStream(): Observable<readonly ElementRef[] | undefined> {
        return this.searchElements$;
    }

    /**
     * Retrieve the existence of a `SidenavSearchComponent` as a stream of indicators
     */
    hasSearchComponentStream(): Observable<boolean> {
        return this.searchElements$.pipe(map((elements) => elements !== undefined));
    }

    /**
     * Function that safely emits the search term via `searchString$` if it is specified
     */
    searchTerm(term: string): void {
        this.searchString$?.emit(term);
    }

    /**
     * Close the sidenav
     */
    close(): void {
        this.close$.next();
    }

    /**
     * Get a stream to react on sidenav close actions.
     */
    getCloseStream(): Observable<void> {
        return this.close$;
    }
}
