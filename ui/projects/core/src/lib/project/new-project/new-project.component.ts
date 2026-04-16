import {BehaviorSubject, zip} from 'rxjs';
import {first, mergeMap} from 'rxjs/operators';
import {Component, ChangeDetectionStrategy, AfterViewInit, inject} from '@angular/core';
import {UntypedFormGroup, UntypedFormBuilder, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ProjectService} from '../project.service';
import {SpatialReferenceService, WEB_MERCATOR} from '../../spatial-references/spatial-reference.service';
import {
    NamedSpatialReference,
    NotificationService,
    SpatialReferenceSpecification,
    Time,
    extentToBboxDict,
    FxLayoutDirective,
    FxFlexDirective,
} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatFormField, MatInput, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatButton} from '@angular/material/button';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-new-project',
    templateUrl: './new-project.component.html',
    styleUrls: ['./new-project.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        FxLayoutDirective,
        MatFormField,
        FxFlexDirective,
        MatInput,
        MatHint,
        MatSelect,
        MatOption,
        MatButton,
        AsyncPipe,
    ],
})
export class NewProjectComponent implements AfterViewInit {
    protected formBuilder = inject(UntypedFormBuilder);
    protected projectService = inject(ProjectService);
    protected notificationService = inject(NotificationService);
    protected spatialReferenceService = inject(SpatialReferenceService);

    spatialReferenceOptions: Array<NamedSpatialReference>;

    form: UntypedFormGroup;

    created$ = new BehaviorSubject(false);

    constructor() {
        this.spatialReferenceOptions = this.spatialReferenceService.getSpatialReferences();
        this.form = this.formBuilder.group({
            name: [
                '',
                Validators.required,
                // TODO: check for uniqueness
                // geoengineValidators.uniqueProjectName(this.storageService),
            ],
            spatialReference: [WEB_MERCATOR, Validators.required],
        });
        this.projectService
            .getSpatialReferenceStream()
            .pipe(first())
            .subscribe((spatialReference) => {
                this.form.controls['spatialReference'].setValue(spatialReference);
            });
    }

    ngAfterViewInit(): void {
        setTimeout(() => this.form.updateValueAndValidity());
    }

    /**
     * Create a new project and switch to it.
     */
    create(): void {
        const spatialReference = this.form.controls['spatialReference'].value;

        zip(this.projectService.getTimeStream(), this.spatialReferenceService.getSpatialReferenceSpecification(spatialReference.srsString))
            .pipe(
                first(),
                mergeMap(([time, spec]: [Time, SpatialReferenceSpecification]) => {
                    const projectName: string = this.form.controls['name'].value;

                    return this.projectService.createProject({
                        name: projectName,
                        description: projectName, // TODO: add description
                        spatialReference: spec.spatialReference,
                        bounds: extentToBboxDict(spec.extent),
                        time,
                        timeStepDuration: {durationAmount: 1, durationUnit: 'months'},
                    });
                }),
            )
            .subscribe((project) => {
                this.projectService.setProject(project);
                this.created$.next(true);
                this.notificationService.info(`Created and switched to new project »${project.name}«`);
            });
    }
}
