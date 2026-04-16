import {Component, OnInit, ChangeDetectionStrategy, AfterViewInit, OnDestroy, inject, viewChild} from '@angular/core';
import {UntypedFormControl, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ReplaySubject, Subject} from 'rxjs';
import {MatSelect} from '@angular/material/select';
import {takeUntil, take} from 'rxjs/operators';
import {CountryProviderService, Country} from '../country-provider.service';
import {MatFormField} from '@angular/material/input';
import {MatOption} from '@angular/material/autocomplete';
import {MatSelectSearchComponent} from 'ngx-mat-select-search';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-ebv-country-selector',
    templateUrl: './country-selector.component.html',
    styleUrls: ['./country-selector.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatFormField, MatSelect, FormsModule, ReactiveFormsModule, MatOption, MatSelectSearchComponent, AsyncPipe],
})
export class CountrySelectorComponent implements OnInit, AfterViewInit, OnDestroy {
    private readonly countryProviderService = inject(CountryProviderService);

    public readonly countryDataList: Array<Country>;

    public countryCtrl: UntypedFormControl = new UntypedFormControl();

    public countryFilterCtrl: UntypedFormControl = new UntypedFormControl();

    public filteredCountries: ReplaySubject<Array<Country>> = new ReplaySubject<Array<Country>>(1);

    readonly countrySelect = viewChild.required<MatSelect>('countrySelect');

    protected _onDestroy = new Subject<void>();

    constructor() {
        this.countryDataList = this.countryProviderService.availabeCountries;
    }

    ngOnInit(): void {
        this.countryCtrl.valueChanges.pipe(takeUntil(this._onDestroy)).subscribe((value) => {
            this.countryProviderService.setSelectedCountry(value);
        });

        this.countryProviderService
            .getSelectedCountryStream()
            .pipe(takeUntil(this._onDestroy))
            .subscribe((country) => {
                this.countryCtrl.setValue(country, {emitEvent: false, emitModelToViewChange: true});
            });

        this.filteredCountries.next(this.countryDataList.slice());

        this.countryFilterCtrl.valueChanges.pipe(takeUntil(this._onDestroy)).subscribe(() => {
            this.filterCountries();
        });
    }

    ngAfterViewInit(): void {
        this.setInitialValue();
    }

    ngOnDestroy(): void {
        this._onDestroy.next();
        this._onDestroy.complete();
    }

    protected setInitialValue(): void {
        this.filteredCountries.pipe(take(1), takeUntil(this._onDestroy)).subscribe(() => {
            this.countrySelect().compareWith = (a: Country, b: Country): boolean => a?.name === b?.name;
        });
    }

    protected filterCountries(): void {
        if (!this.countryDataList) {
            return;
        }
        // get the search keyword
        let search = this.countryFilterCtrl.value;
        if (!search) {
            this.filteredCountries.next(this.countryDataList.slice());
            return;
        } else {
            search = search.toLowerCase();
        }
        this.filteredCountries.next(this.countryDataList.filter((country) => country.name.toLowerCase().includes(search)));
    }
}
