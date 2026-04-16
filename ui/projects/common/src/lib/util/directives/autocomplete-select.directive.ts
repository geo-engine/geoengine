import {Directive, OnChanges, OnDestroy, SimpleChanges, input} from '@angular/core';
import {UntypedFormControl} from '@angular/forms';
import {Observable, ReplaySubject, Subscription} from 'rxjs';

@Directive({
    selector: '[geoengineAutocompleteSelect]',
    exportAs: 'geoengineAutocompleteSelect',
})
export class AutocompleteSelectDirective<T> implements OnChanges, OnDestroy {
    readonly allValues = input<Array<T>>([], {alias: 'geoengineAutocompleteSelectAllValues'});
    readonly searchPredicate = input<AutocompleteSelectPredicateFunction<T>>(DEFAULT_PREDICATE_FN, {
        alias: 'geoengineAutocompleteSelectSearchPredicate',
    });

    readonly filterFormControl = new UntypedFormControl();
    readonly _filteredValues = new ReplaySubject<Array<T>>();

    private filterFormSubscription: Subscription;

    constructor() {
        this.filterFormSubscription = this.filterFormControl.valueChanges.subscribe((_value: string) => {
            this.applyFilter();
        });
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (changes.allValues || changes.compareWith || changes.searchPredicate) {
            this.applyFilter();
        }
    }

    ngOnDestroy(): void {
        this.filterFormSubscription?.unsubscribe();
    }

    get filteredValues(): Observable<Array<T>> {
        return this._filteredValues;
    }

    protected applyFilter(): void {
        let searchTerm: string = this.filterFormControl.value;

        let filteredValues = this.allValues();

        if (searchTerm) {
            searchTerm = searchTerm.toLowerCase();
            filteredValues = filteredValues.filter((value: T) => this.searchPredicate()(searchTerm, value));
        }

        this._filteredValues.next(filteredValues);
    }
}

export type AutocompleteSelectPredicateFunction<T> = (filter: string, element: T) => boolean;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const DEFAULT_PREDICATE_FN: AutocompleteSelectPredicateFunction<any> = (filter: string, element: any): boolean =>
    JSON.stringify(element).toLowerCase().includes(filter);
