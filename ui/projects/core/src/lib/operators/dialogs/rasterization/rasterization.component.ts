import {ChangeDetectionStrategy, Component, OnDestroy, inject, viewChild} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ProjectService} from '../../../project/project.service';
import {mergeMap} from 'rxjs/operators';
import {UUID} from '../../../backend/backend.model';
import {BehaviorSubject, combineLatest, Observable, of, Subscription} from 'rxjs';
import {
    DensityRasterizationDict,
    GridRasterizationDict,
    Layer,
    NotificationService,
    RasterLayer,
    RasterSymbology,
    RasterizationDict,
    ResultTypes,
    geoengineValidators,
    AsyncValueDefault,
} from '@geoengine/common';
import {SymbologyCreatorComponent} from '../../../layers/symbology/symbology-creator/symbology-creator.component';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatTabGroup, MatTab} from '@angular/material/tabs';
import {NgTemplateOutlet, AsyncPipe} from '@angular/common';
import {MatRadioGroup, MatRadioButton} from '@angular/material/radio';
import {MatFormField, MatLabel, MatInput, MatHint} from '@angular/material/input';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';

interface RasterizationForm {
    name: FormControl<string>;
    layer: FormControl<Layer | undefined>;
    rasterization: FormGroup<DensityForm> | FormGroup<GridForm>;
}

interface GridForm {
    gridOrDensity: FormControl<number>;
    gridSizeMode: FormControl<'fixed' | 'relative'>;
    resolution: FormGroup<{
        resX: FormControl<number>;
        resY: FormControl<number>;
    }>;
    origin: FormGroup<{
        originX: FormControl<number>;
        originY: FormControl<number>;
    }>;
}
interface DensityForm {
    gridOrDensity: FormControl<number>;
    cutoff: FormControl<number>;
    stddev: FormControl<number>;
}
/**
 * This component allows creating the rasterization operator.
 */
@Component({
    selector: 'geoengine-rasterization',
    templateUrl: './rasterization.component.html',
    styleUrls: ['./rasterization.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        MatTabGroup,
        MatTab,
        NgTemplateOutlet,
        MatRadioGroup,
        MatRadioButton,
        MatFormField,
        MatLabel,
        MatInput,
        MatHint,
        OperatorOutputNameComponent,
        SymbologyCreatorComponent,
        MatButton,
        AsyncPipe,
        AsyncValueDefault,
    ],
})
export class RasterizationComponent implements OnDestroy {
    private projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private formBuilder = inject(FormBuilder);

    selected = new FormControl(0, {validators: [Validators.required], nonNullable: true});

    readonly inputTypes = [ResultTypes.POINTS];

    readonly form: FormGroup<RasterizationForm>;
    readonly subscriptions: Array<Subscription> = [];

    readonly loading$ = new BehaviorSubject<boolean>(false);

    readonly symbologyCreator = viewChild.required(SymbologyCreatorComponent);

    constructor() {
        const layerControl = new FormControl<Layer | undefined>(undefined, {
            nonNullable: true,
            validators: [Validators.required],
        });
        this.form = new FormGroup<RasterizationForm>({
            name: this.formBuilder.nonNullable.control<string>('Rasterized', [Validators.required, geoengineValidators.notOnlyWhitespace]),
            layer: layerControl,
            rasterization: this.initialGrid(),
        });
    }

    ngOnDestroy(): void {
        for (const subscription of this.subscriptions) {
            subscription.unsubscribe();
        }
    }

    initialGrid(): FormGroup<GridForm> {
        const fb = this.formBuilder.nonNullable;
        const gridMode = fb.control<'fixed' | 'relative'>('fixed');
        return fb.group<GridForm>({
            gridOrDensity: this.createRasterizationType(0),
            gridSizeMode: gridMode,
            resolution: this.formBuilder.group({
                resX: fb.control<number>(10.0, {validators: [Validators.required, geoengineValidators.largerThan(0)]}),
                resY: fb.control<number>(10.0, {validators: [Validators.required, geoengineValidators.largerThan(0)]}),
            }),
            origin: this.formBuilder.group({
                originX: fb.control<number>(0.0, Validators.required),
                originY: fb.control<number>(0.0, Validators.required),
            }),
        });
    }

    initialDensity(): FormGroup<DensityForm> {
        return this.formBuilder.nonNullable.group<DensityForm>({
            gridOrDensity: this.createRasterizationType(1),
            cutoff: this.formBuilder.nonNullable.control<number>(0.01, {
                validators: [Validators.required, geoengineValidators.inRange(0, 1, true, false)],
            }),
            stddev: this.formBuilder.nonNullable.control<number>(1.0, {
                validators: [Validators.required, geoengineValidators.largerThan(0)],
            }),
        });
    }

    add(): void {
        if (this.loading$.value) {
            return; // don't add while loading
        }

        const pointsLayer = this.form.controls['layer'].value!;
        const layerName: string = this.form.controls['name'].value;
        const params = this.rasterizationParams();

        if (!params) {
            return;
        }

        this.loading$.next(true);

        this.projectService
            .getAutomaticallyProjectedOperatorsFromLayers([pointsLayer])
            .pipe(
                mergeMap(([points]) => {
                    const workflow: WorkflowDict = {
                        type: 'Raster',
                        operator: {
                            type: 'Rasterization',
                            params,
                            sources: {
                                vector: points,
                            },
                        } as RasterizationDict,
                    };
                    return this.projectService.registerWorkflow(workflow);
                }),
                mergeMap((workflowId: UUID) => {
                    const symbology$: Observable<RasterSymbology> = this.symbologyCreator().symbologyForRasterLayer(workflowId);
                    return combineLatest([of(workflowId), symbology$]);
                }),
                mergeMap(([workflowId, symbology]: [UUID, RasterSymbology]) =>
                    this.projectService.addLayer(
                        new RasterLayer({
                            workflowId,
                            name: layerName,
                            symbology,
                            isLegendVisible: false,
                            isVisible: true,
                        }),
                    ),
                ),
            )
            .subscribe({
                next: () => {
                    // success
                    this.loading$.next(false);
                },
                error: (error) => {
                    this.notificationService.error(error);
                    this.loading$.next(false);
                },
            });
    }

    protected createRasterizationType(selectedIndex: number): FormControl<number> {
        this.selected = this.formBuilder.nonNullable.control<number>(selectedIndex);
        const rasterType = this.selected;
        this.subscriptions.push(rasterType.valueChanges.subscribe((value) => this.changeRasterization(value)));
        return rasterType;
    }

    protected changeRasterization(rasterization: number): void {
        if (rasterization === 0) {
            this.form.setControl('rasterization', this.initialGrid());
        } else if (rasterization === 1) {
            this.form.setControl('rasterization', this.initialDensity());
        }
    }

    private rasterizationParams(): GridRasterizationDict | DensityRasterizationDict | null {
        let params: GridRasterizationDict | DensityRasterizationDict | null = null;
        let rasterization = this.form.controls.rasterization;

        if (this.selected.value === 0) {
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion -- needed for TS to narrow the union type
            rasterization = this.form.controls.rasterization as FormGroup<GridForm>;

            return (params = {
                type: 'grid',
                spatialResolution: {
                    x: rasterization?.value.resolution?.resX ?? 10,
                    y: rasterization?.value.resolution?.resY ?? 10,
                },
                gridSizeMode: rasterization?.value.gridSizeMode ?? 'fixed',
                originCoordinate: {
                    x: rasterization?.value.origin?.originX ?? 0,
                    y: rasterization?.value.origin?.originY ?? 0,
                },
            });
        }

        if (this.selected.value === 1) {
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion -- needed for TS to narrow the union type
            rasterization = this.form.controls.rasterization as FormGroup<DensityForm>;

            return (params = {
                type: 'density',
                cutoff: rasterization?.value.cutoff ?? 10,
                stddev: rasterization?.value.stddev ?? 10,
            });
        }
        return params;
    }
}
