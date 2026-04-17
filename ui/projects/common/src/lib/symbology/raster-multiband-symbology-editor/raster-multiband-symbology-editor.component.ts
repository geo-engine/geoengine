import {Subscription} from 'rxjs';
import {ChangeDetectionStrategy, Component, computed, effect, inject, input, OnDestroy, output, signal} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {geoengineValidators} from '../../util/form.validators';
import {SymbologyQueryParams, MultiBandRasterColorizer} from '../symbology.model';
import {Color, TRANSPARENT} from '../../colors/color';
import {WorkflowsService} from '../../workflows/workflows.service';
import {ExpressionDict, StatisticsDict, StatisticsParams} from '../../operators/operator.model';
import {PlotsService} from '../../plots/plots.service';
import {UUID} from '../../datasets/dataset.model';
import {MatCard, MatCardHeader, MatCardTitleGroup, MatCardTitle, MatCardSubtitle, MatCardContent} from '@angular/material/card';
import {MatIcon} from '@angular/material/icon';
import {MatFormField, MatLabel, MatInput, MatError} from '@angular/material/input';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {MatButton} from '@angular/material/button';
import {TitleCasePipe} from '@angular/common';

interface RgbSettingsForm {
    red: FormGroup<{
        min: FormControl<number>;
        max: FormControl<number>;
        scale: FormControl<number>;
    }>;
    green: FormGroup<{
        min: FormControl<number>;
        max: FormControl<number>;
        scale: FormControl<number>;
    }>;
    blue: FormGroup<{
        min: FormControl<number>;
        max: FormControl<number>;
        scale: FormControl<number>;
    }>;
    noDataColor: FormControl<Color>;
}

type RgbColorName = 'red' | 'green' | 'blue';

/**
 * This dialog allows calculations on (one or more) raster layers.
 */
@Component({
    selector: 'geoengine-raster-multiband-symbology-editor',
    templateUrl: './raster-multiband-symbology-editor.component.html',
    styleUrls: ['./raster-multiband-symbology-editor.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatCard,
        MatCardHeader,
        MatCardTitleGroup,
        MatCardTitle,
        MatCardSubtitle,
        MatIcon,
        MatCardContent,
        FormsModule,
        ReactiveFormsModule,
        MatFormField,
        MatLabel,
        MatInput,
        MatError,
        MatProgressSpinner,
        MatButton,
        TitleCasePipe,
    ],
})
export class RasterMultibandSymbologyEditorComponent implements OnDestroy {
    private readonly formBuilder = inject(FormBuilder);
    private readonly workflowsService = inject(WorkflowsService);
    private readonly plotsService = inject(PlotsService);

    readonly workflowId = input.required<UUID>();
    readonly queryParams = input<SymbologyQueryParams>();
    readonly band1 = input.required<{
        name: string;
        index: number;
    }>();
    readonly band2 = input.required<{
        name: string;
        index: number;
    }>();
    readonly band3 = input.required<{
        name: string;
        index: number;
    }>();
    readonly colorizer = input.required<MultiBandRasterColorizer>();
    readonly colorizerChange = output<MultiBandRasterColorizer>();

    readonly channels = computed<Array<{color: RgbColorName; label: string}>>(() => {
        const band1 = this.band1();
        const band2 = this.band2();
        const band3 = this.band3();

        return [
            {color: 'red', label: band1.name},
            {color: 'green', label: band2.name},
            {color: 'blue', label: band3.name},
        ];
    });

    readonly form: FormGroup<RgbSettingsForm>;
    readonly formSubscription: Subscription;

    readonly isLoadingRasterStats = signal(false);

    /**
     * Set up the form and disable it if the raster stats are loading
     */
    constructor() {
        const formBuilder = this.formBuilder.nonNullable;
        this.form = new FormGroup<RgbSettingsForm>({
            red: formBuilder.group(
                {
                    min: formBuilder.control(0, Validators.required),
                    max: formBuilder.control(255, Validators.required),
                    scale: formBuilder.control(1, [Validators.required, Validators.min(0), Validators.max(1)]),
                },
                {
                    validators: geoengineValidators.minAndMax('min', 'max', {
                        checkBothExist: true,
                        mustNotEqual: true,
                    }),
                },
            ),
            green: formBuilder.group(
                {
                    min: formBuilder.control(0, Validators.required),
                    max: formBuilder.control(255, Validators.required),
                    scale: formBuilder.control(1, [Validators.required, Validators.min(0), Validators.max(1)]),
                },
                {
                    validators: geoengineValidators.minAndMax('min', 'max', {
                        checkBothExist: true,
                        mustNotEqual: true,
                    }),
                },
            ),
            blue: formBuilder.group(
                {
                    min: formBuilder.control(0, Validators.required),
                    max: formBuilder.control(255, Validators.required),
                    scale: formBuilder.control(1, [Validators.required, Validators.min(0), Validators.max(1)]),
                },
                {
                    validators: geoengineValidators.minAndMax('min', 'max', {
                        checkBothExist: true,
                        mustNotEqual: true,
                    }),
                },
            ),
            noDataColor: formBuilder.control<Color>(TRANSPARENT, Validators.required),
        });

        this.formSubscription = this.form.valueChanges.subscribe({
            next: (_value) => {
                if (this.form.invalid) {
                    return;
                }

                const value = this.form.getRawValue();
                const colorizer = this.colorizer().withParams(
                    value.red.min,
                    value.red.max,
                    value.red.scale,
                    value.green.min,
                    value.green.max,
                    value.green.scale,
                    value.blue.min,
                    value.blue.max,
                    value.blue.scale,
                    value.noDataColor,
                );

                if (!this.colorizer().equals(colorizer)) {
                    this.colorizerChange.emit(colorizer);
                }
            },
        });

        effect(() => {
            const colorizer = this.colorizer();

            this.form.setValue({
                red: {
                    min: colorizer.redMin,
                    max: colorizer.redMax,
                    scale: colorizer.redScale,
                },
                green: {
                    min: colorizer.greenMin,
                    max: colorizer.greenMax,
                    scale: colorizer.greenScale,
                },
                blue: {
                    min: colorizer.blueMin,
                    max: colorizer.blueMax,
                    scale: colorizer.blueScale,
                },
                noDataColor: colorizer.noDataColor,
            });
        });

        effect(() => {
            if (this.isLoadingRasterStats()) {
                this.form.disable();
            } else {
                this.form.enable();
            }
        });
    }

    ngOnDestroy(): void {
        this.formSubscription.unsubscribe();
    }

    async calculateRasterStats(): Promise<void> {
        if (this.isLoadingRasterStats()) {
            return; // checked by form validator
        }
        const queryParams = this.queryParams();
        if (!queryParams) {
            return;
        }

        this.isLoadingRasterStats.set(true);

        const workflow = await this.workflowsService.getWorkflow(this.workflowId());

        // TODO: remove expressions when Statistics Operator supports multiple bands
        const subExpression = ({name, index}: {name: string; index: number}): ExpressionDict => {
            return {
                type: 'Expression',
                params: {
                    expression: String.fromCharCode('A'.charCodeAt(0) + index),
                    outputType: 'F64',
                    outputBand: {
                        name,
                        measurement: {type: 'unitless'},
                    },
                    mapNoData: false,
                },
                sources: {
                    raster: workflow.operator,
                },
            } as ExpressionDict;
        };

        const statsWorkflowId = await this.workflowsService.registerWorkflow({
            type: 'Plot',
            operator: {
                type: 'Statistics',
                params: {columnNames: ['red', 'green', 'blue']} as StatisticsParams,
                sources: {
                    source: [subExpression(this.band1()), subExpression(this.band2()), subExpression(this.band3())],
                },
            } as StatisticsDict,
        });

        const plot = await this.plotsService.getPlot(
            statsWorkflowId,
            queryParams.bbox,
            queryParams.time,
            queryParams.resolution,
            queryParams.spatialReference,
        );

        const plotData = plot.data as {
            red: {
                min: number;
                max: number;
            };
            green: {
                min: number;
                max: number;
            };
            blue: {
                min: number;
                max: number;
            };
        };

        const colors: Array<RgbColorName> = ['red', 'green', 'blue'];
        for (const color of colors) {
            this.form.controls[color].controls.min.setValue(plotData[color].min);
            this.form.controls[color].controls.max.setValue(plotData[color].max);
        }

        this.form.updateValueAndValidity();

        this.isLoadingRasterStats.set(false);
    }
}
