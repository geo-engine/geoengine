import {ChangeDetectionStrategy, Component, inject, viewChild} from '@angular/core';
import {
    FormControl,
    FormGroup,
    Validators,
    AbstractControl,
    ValidatorFn,
    ValidationErrors,
    FormsModule,
    ReactiveFormsModule,
} from '@angular/forms';
import {ProjectService} from '../../../project/project.service';
import {mergeMap} from 'rxjs/operators';
import {BehaviorSubject, Observable, combineLatest, of} from 'rxjs';
import {
    RasterDataTypes,
    RasterLayer,
    ResultTypes,
    geoengineValidators,
    BandNeighborhoodAggregate,
    UUID,
    RasterSymbology,
    SingleBandRasterColorizer,
    BandNeighborhoodAggregateDict,
    GeoEngineError,
} from '@geoengine/common';
import {SymbologyCreatorComponent} from '../../../layers/symbology/symbology-creator/symbology-creator.component';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatButtonToggleGroup, MatButtonToggle} from '@angular/material/button-toggle';
import {MatFormField, MatLabel, MatInput, MatError, MatHint} from '@angular/material/input';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

interface BandNeighborhoodForm {
    rasterLayer: FormControl<RasterLayer | undefined>;
    name: FormControl<string>;
    neighborhoodAggregate: FormControl<NeighborhoodAggregate>;
    windowSize: FormControl<number>;
    bandDistance: FormControl<number>;
}

enum NeighborhoodAggregate {
    Average,
    FirstDerivative,
}

@Component({
    selector: 'geoengine-band-neighborhood-aggregate',
    templateUrl: './band-neighborhood-aggregate.component.html',
    styleUrls: ['./band-neighborhood-aggregate.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        MatButtonToggleGroup,
        MatButtonToggle,
        MatFormField,
        MatLabel,
        MatInput,
        MatError,
        OperatorOutputNameComponent,
        MatHint,
        SymbologyCreatorComponent,
        MatButton,
        AsyncPipe,
    ],
})
export class BandNeighborhoodAggregateComponent {
    private readonly projectService = inject(ProjectService);

    readonly RASTER_TYPE = [ResultTypes.RASTER];
    readonly rasterDataTypes = RasterDataTypes.ALL_DATATYPES;

    readonly NeighborhoodAggregate = NeighborhoodAggregate;

    readonly lastError$ = new BehaviorSubject<string | undefined>(undefined);
    readonly loading$ = new BehaviorSubject<boolean>(false);

    readonly form: FormGroup<BandNeighborhoodForm>;

    readonly symbologyCreator = viewChild.required(SymbologyCreatorComponent);

    constructor() {
        this.form = new FormGroup<BandNeighborhoodForm>({
            rasterLayer: new FormControl<RasterLayer | undefined>(undefined, {
                nonNullable: true,
                validators: [Validators.required], // TODO: check that the input has at least 2 bands
            }),
            name: new FormControl('Neighborhood Aggregate', {
                nonNullable: true,
                validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
            }),
            neighborhoodAggregate: new FormControl(NeighborhoodAggregate.Average, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            windowSize: new FormControl(3, {
                nonNullable: true,
                validators: [Validators.required, geoengineValidators.largerThan(2.0), oddNumberValidator()],
            }),
            bandDistance: new FormControl(1, {
                nonNullable: true,
                validators: [],
            }),
        });
    }

    updateNeighborhoodAggregate(): void {
        switch (this.form.controls.neighborhoodAggregate.value) {
            case NeighborhoodAggregate.Average:
                this.form.controls.windowSize.setValidators([
                    Validators.required,
                    geoengineValidators.largerThan(2.0),
                    oddNumberValidator(),
                ]);
                this.form.controls.windowSize.updateValueAndValidity();
                this.form.controls.bandDistance.clearValidators();
                this.form.controls.bandDistance.updateValueAndValidity();
                break;
            case NeighborhoodAggregate.FirstDerivative:
                this.form.controls.windowSize.clearValidators();
                this.form.controls.windowSize.updateValueAndValidity();
                this.form.controls.bandDistance.setValidators([Validators.required, geoengineValidators.largerThan(0.0)]);
                this.form.controls.bandDistance.updateValueAndValidity();
                break;
        }
    }

    add(): void {
        if (this.loading$.value) {
            return; // don't add while loading
        }

        const name: string = this.form.controls['name'].value;
        const rasterLayer: RasterLayer | undefined = this.form.controls['rasterLayer'].value;

        if (!rasterLayer) {
            return; // checked by form validator
        }

        const aggregate = this.getAggregate();

        this.projectService
            .getWorkflow(rasterLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow) => {
                    const workflow: WorkflowDict = {
                        type: 'Raster',
                        operator: {
                            type: 'BandNeighborhoodAggregate',
                            params: {
                                aggregate,
                            },
                            sources: {
                                raster: inputWorkflow.operator,
                            },
                        } as BandNeighborhoodAggregateDict,
                    } as WorkflowDict;

                    return this.projectService.registerWorkflow(workflow);
                }),
                mergeMap((workflowId: UUID) => {
                    const symbology$: Observable<RasterSymbology> = this.symbologyCreator().symbologyForRasterLayer(
                        workflowId,
                        rasterLayer,
                    );
                    return combineLatest([of(workflowId), symbology$]);
                }),
                mergeMap(([workflowId, symbology]: [UUID, RasterSymbology]) => {
                    if (symbology.rasterColorizer instanceof SingleBandRasterColorizer) {
                        const outSymbology = new RasterSymbology(symbology.opacity, symbology.rasterColorizer.replaceBand(0));
                        return this.projectService.addLayer(
                            new RasterLayer({
                                workflowId,
                                name,
                                symbology: outSymbology,
                                isLegendVisible: false,
                                isVisible: true,
                            }),
                        );
                    } else {
                        throw new GeoEngineError('SymbologyError', 'The input Symbology must be a single band colorizer.');
                    }
                }),
            )
            .subscribe({
                next: () => {
                    // everything worked well
                    this.lastError$.next(undefined);
                    this.loading$.next(false);
                },
                error: (error) => {
                    const errorMsg = error;
                    this.lastError$.next(errorMsg);
                    this.loading$.next(false);
                },
            });
    }

    private getAggregate(): BandNeighborhoodAggregate {
        switch (this.form.controls.neighborhoodAggregate.value) {
            case NeighborhoodAggregate.Average:
                return {
                    type: 'average',
                    windowSize: this.form.controls.windowSize.value,
                };
            case NeighborhoodAggregate.FirstDerivative:
                return {
                    type: 'firstDerivative',
                    bandDistance: {
                        type: 'equallySpaced',
                        distance: this.form.controls.bandDistance.value,
                    },
                };
        }
    }
}

const oddNumberValidator =
    (): ValidatorFn =>
    (control: AbstractControl): ValidationErrors | null => {
        if (control.value === null || control.value === '') {
            return null; // don't validate empty values to allow optional controls
        }
        const isOdd = control.value % 2 !== 0;
        return isOdd ? null : {notOdd: true};
    };
