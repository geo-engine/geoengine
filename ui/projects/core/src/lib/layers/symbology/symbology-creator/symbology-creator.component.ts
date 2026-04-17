import {ChangeDetectionStrategy, Component, forwardRef, HostListener, OnDestroy, OnInit, inject, input} from '@angular/core';
import {
    ControlValueAccessor,
    FormControl,
    FormGroup,
    NG_VALUE_ACCESSOR,
    Validators,
    FormsModule,
    ReactiveFormsModule,
} from '@angular/forms';
import {combineLatest, first, map, mergeMap, Observable, of, Subject, takeUntil} from 'rxjs';
import {BBoxDict, RasterResultDescriptorDict, SrsString, TimeIntervalDict, UUID} from '../../../backend/backend.model';
import {BackendService} from '../../../backend/backend.service';
import {ProjectService} from '../../../project/project.service';
import {SpatialReferenceService} from '../../../spatial-references/spatial-reference.service';
import {
    Colorizer,
    ColorMapSelectorComponent,
    LinearGradient,
    ALL_COLORMAPS,
    RasterLayer,
    RasterResultDescriptor,
    RasterSymbology,
    SingleBandRasterColorizer,
    StatisticsDict,
    StatisticsParams,
    Time,
    TRANSPARENT,
    UserService,
    FxFlexDirective,
} from '@geoengine/common';
import {MatFormField, MatLabel, MatHint, MatInput} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {NgClass} from '@angular/common';

const COLORMAPS = ALL_COLORMAPS;

export enum SymbologyCreationType {
    AS_INPUT = 'AS_INPUT',
    COMPUTE_LINEAR_GRADIENT = 'COMPUTE_LINEAR_GRADIENT',
    LINEAR_GRADIENT_FROM_MIN_MAX = 'LINEAR_GRADIENT_FROM_MIN_MAX',
}

@Component({
    selector: 'geoengine-symbology-creator',
    templateUrl: './symbology-creator.component.html',
    styleUrls: ['./symbology-creator.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => SymbologyCreatorComponent),
            multi: true,
        },
    ],
    imports: [MatFormField, MatLabel, MatSelect, FormsModule, ReactiveFormsModule, MatOption, MatHint, FxFlexDirective, MatInput, NgClass],
})
export class SymbologyCreatorComponent implements OnInit, OnDestroy, ControlValueAccessor {
    protected readonly projectService = inject(ProjectService);
    protected readonly userService = inject(UserService);
    protected readonly backend = inject(BackendService);
    protected readonly spatialReferenceService = inject(SpatialReferenceService);

    AS_INPUT = SymbologyCreationType.AS_INPUT;
    COMPUTE_LINEAR_GRADIENT = SymbologyCreationType.COMPUTE_LINEAR_GRADIENT;
    LINEAR_GRADIENT_FROM_MIN_MAX = SymbologyCreationType.LINEAR_GRADIENT_FROM_MIN_MAX;

    readonly enableCopyInputSymbology = input(true);
    readonly colorMapName = input('viridis');
    readonly opacity = input(1.0);

    min = new FormControl(0, {validators: [Validators.required], nonNullable: true});
    max = new FormControl(255, {validators: [Validators.required], nonNullable: true});

    value = new FormControl<SymbologyCreationType>(SymbologyCreationType.AS_INPUT, {
        nonNullable: false,
        validators: [Validators.required],
    });

    form = new FormGroup([this.min, this.max, this.value], (_) => {
        if (
            this.value.getRawValue() !== SymbologyCreationType.LINEAR_GRADIENT_FROM_MIN_MAX ||
            (this.min.getRawValue() !== null && this.max.getRawValue() !== null && this.max.getRawValue() > this.min.getRawValue())
        ) {
            return null;
        } else return {valid: false};
    });

    private colorMap = COLORMAPS.VIRIDIS;

    private _onChange?: (value: SymbologyCreationType | null) => void;
    private _onTouched?: () => void;

    // Can be used to complete subscriptions `OnDestroy` – need to call `next` and `complete` once.
    private unsubscribe$ = new Subject<void>();

    constructor() {
        this.value.valueChanges.pipe(takeUntil(this.unsubscribe$)).subscribe((value) => {
            if (!this._onChange) {
                return;
            }

            this._onChange(value);
        });
    }

    ngOnInit(): void {
        if (!this.enableCopyInputSymbology()) {
            this.value.setValue(this.COMPUTE_LINEAR_GRADIENT);
        }

        const colorMapName = this.colorMapName();
        if (colorMapName.toUpperCase() in COLORMAPS) {
            this.colorMap = COLORMAPS[colorMapName.toUpperCase()];
        } else {
            throw new Error('Unsupported color map name ' + colorMapName);
        }

        if (this.opacity() < 0 || this.opacity() > 1) {
            throw new Error('The opacity needs to be in [0, 1]');
        }
    }

    ngOnDestroy(): void {
        this.unsubscribe$.next();
        this.unsubscribe$.complete();
    }

    writeValue(obj: SymbologyCreationType | null): void {
        if (obj === null) {
            this.value.setValue(null);
            return;
        }

        if (!(obj in SymbologyCreationType)) {
            throw new Error('Value must be of type `SymbologyCreationType`.');
        }

        this.value.setValue(obj);
    }

    registerOnChange(fn: (value: SymbologyCreationType | null) => void): void {
        if (typeof fn !== 'function') {
            throw new Error('Expected a function.');
        }
        this._onChange = fn;
    }

    registerOnTouched(fn: () => void): void {
        if (typeof fn !== 'function') {
            throw new Error('Expected a function.');
        }
        this._onTouched = fn;
    }

    @HostListener('blur', ['$event']) onBlur(_event: FocusEvent): void {
        if (!this._onTouched) {
            return;
        }
        this._onTouched();
    }

    setDisabledState?(isDisabled: boolean): void {
        if (isDisabled) {
            this.value.disable();
        } else {
            this.value.enable();
        }
    }

    symbologyForRasterLayer(workflowId: UUID, inputLayer?: RasterLayer): Observable<RasterSymbology> {
        const value = this.value.getRawValue();

        if (!value) {
            throw new Error('Cannot call `symbologyForRasterLayer` on empty selection.');
        }

        switch (value) {
            case SymbologyCreationType.AS_INPUT: {
                if (inputLayer) {
                    return of(inputLayer.symbology);
                } else throw new Error('Input raster layer parameter is missing');
            }
            case SymbologyCreationType.COMPUTE_LINEAR_GRADIENT: {
                return this.computeSymbologyForRasterLayer(workflowId);
            }
            case SymbologyCreationType.LINEAR_GRADIENT_FROM_MIN_MAX: {
                return of(
                    new RasterSymbology(
                        this.opacity(),
                        new SingleBandRasterColorizer(0, this.colorizerForMinMax(this.min.getRawValue(), this.max.getRawValue())),
                    ),
                );
            }
        }
    }

    protected computeSymbologyForRasterLayer(workflowId: UUID): Observable<RasterSymbology> {
        const rasterName = 'raster';

        const statisticsWorkflow$ = this.projectService.getWorkflow(workflowId).pipe(
            mergeMap((workflow) =>
                this.projectService.registerWorkflow({
                    type: 'Plot',
                    operator: {
                        type: 'Statistics',
                        params: {
                            columnNames: [rasterName],
                        } as StatisticsParams,
                        sources: {
                            source: [workflow['operator']],
                        },
                    } as StatisticsDict,
                }),
            ),
        );

        const queryParams$: Observable<{
            bbox: BBoxDict;
            crs: SrsString;
            time: TimeIntervalDict;
            spatialResolution: [number, number];
        }> = combineLatest([
            this.projectService
                .getWorkflowMetaData(workflowId)
                .pipe(map((resultDescriptor) => RasterResultDescriptor.fromDict(resultDescriptor as RasterResultDescriptorDict))),
            this.projectService.getTimeOnce(),
        ]).pipe(
            mergeMap(([resultDescriptor, time]) =>
                combineLatest([
                    // if we don't know the bbox of the dataset, we use the projection's whole bbox for guessing the symbology
                    // TODO: better use the screen extent?
                    of(resultDescriptor.spatialGrid.bbox()),
                    of(resultDescriptor.spatialReference),
                    of(time),
                ]),
            ),
            map(([bbox, crs, time]: [BBoxDict, SrsString, Time]) => {
                // for sampling, we choose a reasonable resolution
                // FIXME: changing the reslution does not imply that we do less work!
                const NUM_PIXELS = 1024;
                const xResolution = (bbox.upperRightCoordinate.x - bbox.lowerLeftCoordinate.x) / NUM_PIXELS;
                const yResolution = (bbox.upperRightCoordinate.y - bbox.lowerLeftCoordinate.y) / NUM_PIXELS;
                return {
                    bbox,
                    crs,
                    time: time.toDict(),
                    spatialResolution: [xResolution, yResolution],
                };
            }),
        );

        return combineLatest([statisticsWorkflow$, queryParams$, this.userService.getSessionOnce()]).pipe(
            first(),
            mergeMap(([statisticsWorkflow, queryParams, session]) =>
                this.backend.getPlot(statisticsWorkflow, queryParams, session.sessionToken),
            ),
            map((plot) => {
                if (plot.plotType !== 'Statistics') {
                    throw new Error('Expected `Statistics` plot.');
                }

                return plot.data as Record<
                    string,
                    {
                        valueCount: number;
                        validCount: number;
                        min: number;
                        max: number;
                        mean: number;
                        stddev: number;
                    }
                >;
            }),
            map((statistics) => {
                const min = statistics[rasterName].min;
                const max = statistics[rasterName].max;

                if (min === null || min === undefined || max === null || max === undefined) {
                    throw new Error('Sample statistics do not have valid min/max values.');
                }

                return new RasterSymbology(this.opacity(), new SingleBandRasterColorizer(0, this.colorizerForMinMax(min, max)));
            }),
        );
    }

    private colorizerForMinMax(min: number, max: number): Colorizer {
        const NUMBER_OF_COLOR_STEPS = 16;
        const REVERSE_COLORS = false;

        const breakpoints = ColorMapSelectorComponent.createLinearBreakpoints(this.colorMap, NUMBER_OF_COLOR_STEPS, REVERSE_COLORS, {
            min,
            max,
        });

        return new LinearGradient(breakpoints, TRANSPARENT, breakpoints[breakpoints.length - 1].color, breakpoints[0].color);
    }
}
