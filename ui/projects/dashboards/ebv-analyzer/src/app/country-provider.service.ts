import {Injectable, inject} from '@angular/core';
import {BehaviorSubject, mergeMap, Observable} from 'rxjs';
import {COUNTRY_LIST} from './country-selector/country-selector-data.model';
import {ProjectService} from '@geoengine/core';
import {DataSelectionService} from './data-selection.service';
import {countryDatasetName} from './country-selector/country-data.model';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {PolygonSymbology, VectorLayer} from '@geoengine/common';

export interface Country {
    name: string;
    minx: number;
    maxx: number;
    miny: number;
    maxy: number;
    tifChannelId: number;
}

@Injectable({
    providedIn: 'root',
})
export class CountryProviderService {
    private readonly projectService = inject(ProjectService);
    private readonly dataSelectionService = inject(DataSelectionService);

    public readonly selectedCountry$ = new BehaviorSubject<Country | undefined>(undefined);
    public readonly availabeCountries: Array<Country>;

    constructor() {
        this.availabeCountries = COUNTRY_LIST.map((r) => {
            const [name, maxx, maxy, minx, miny, tifChannelId] = r;
            return {
                name,
                minx,
                maxx,
                miny,
                maxy,
                tifChannelId,
            };
        }).sort((a, b) => a.name.localeCompare(b.name));
    }

    public setSelectedCountry(country: Country): void {
        this.selectedCountry$.next(country);

        const workflow: WorkflowDict = {
            type: 'Vector',
            operator: {
                type: 'OgrSource',
                params: {
                    data: 'polygon_country_' + countryDatasetName(country.name),
                },
            },
        };

        this.projectService
            .registerWorkflow(workflow)
            .pipe(
                mergeMap((workflowId) =>
                    this.dataSelectionService.setPolygonLayer(
                        new VectorLayer({
                            workflowId,
                            name: country.name,
                            symbology: PolygonSymbology.fromPolygonSymbologyDict({
                                type: 'polygon',
                                stroke: {
                                    width: {
                                        type: 'static',
                                        value: 2,
                                    },
                                    color: {
                                        type: 'static',
                                        color: [255, 0, 0, 255],
                                    },
                                },
                                fillColor: {
                                    type: 'static',
                                    color: [0, 0, 0, 0],
                                },
                                autoSimplified: true,
                            }),
                            isLegendVisible: false,
                            isVisible: true,
                        }),
                    ),
                ),
            )
            .subscribe(() => {
                // success
            });
    }

    public clearSelectedCountry(): void {
        this.selectedCountry$.next(undefined);
    }

    public getSelectedCountryStream(): Observable<Country | undefined> {
        return this.selectedCountry$;
    }
}
