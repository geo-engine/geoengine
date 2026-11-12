import {AsyncPipe} from '@angular/common';
import {Component, ChangeDetectionStrategy, AfterViewInit, inject} from '@angular/core';
import {
    FormControl,
    FormGroup,
    NonNullableFormBuilder,
    Validators,
    AbstractControl,
    ValidatorFn,
    ValidationErrors,
    FormsModule,
    ReactiveFormsModule,
} from '@angular/forms';
import {MatButton} from '@angular/material/button';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {MatRadioGroup, MatRadioButton} from '@angular/material/radio';
import {BehaviorSubject, ReplaySubject} from 'rxjs';
import {map, mergeMap} from 'rxjs/operators';
import {ProjectsApi} from '@geoengine/api-client';
import {NotificationService, UserService, FxLayoutDirective, FxFlexDirective, apiConfigurationWithAccessKey} from '@geoengine/common';
import {UUID} from '../../backend/backend.model';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {ProjectService} from '../project.service';

const notCurrentProject =
    (currentProjectId: () => string): ValidatorFn =>
    (control: AbstractControl): ValidationErrors | null => {
        const errors: {
            currentProject?: boolean;
        } = {};

        if (currentProjectId() === control.value) {
            errors.currentProject = true;
        }

        return Object.keys(errors).length > 0 ? errors : null;
    };

interface ProjectListing {
    id: UUID;
    name: string;
    description: string;
}

@Component({
    selector: 'geoengine-load-project',
    templateUrl: './load-project.component.html',
    styleUrls: ['./load-project.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        FxLayoutDirective,
        ReactiveFormsModule,
        MatRadioGroup,
        FxFlexDirective,
        MatRadioButton,
        MatButton,
        MatProgressSpinner,
        AsyncPipe,
    ],
})
export class LoadProjectComponent implements AfterViewInit {
    protected projectService = inject(ProjectService);
    protected userService = inject(UserService);
    protected notificationService = inject(NotificationService);
    protected formBuilder = inject(NonNullableFormBuilder);

    form: FormGroup<{projectId: FormControl<UUID>}>;

    projects$ = new ReplaySubject<Array<ProjectListing>>(1);
    loading$ = new BehaviorSubject<boolean>(true);

    currentProjectId: UUID = '';

    constructor() {
        this.projectService.getProjectOnce().subscribe((project) => {
            this.currentProjectId = project.id;
        });

        this.form = this.formBuilder.group({
            projectId: this.formBuilder.control(
                this.currentProjectId,
                Validators.compose([Validators.required, notCurrentProject(() => this.currentProjectId)]),
            ),
        });

        this.userService
            .getSessionTokenForRequest()
            .pipe(
                mergeMap((sessionToken) =>
                    new ProjectsApi(apiConfigurationWithAccessKey(sessionToken)).listProjectsHandler({
                        order: 'DateAsc', // TODO: provide options
                        offset: 0,
                        limit: 20, // TODO: paginate
                    }),
                ),
                map((projectListingDicts) =>
                    projectListingDicts.map((projectListingDict) => ({
                        id: projectListingDict.id,
                        name: projectListingDict.name,
                        description: projectListingDict.description,
                    })),
                ),
            )
            .subscribe((projectListings) => {
                this.projects$.next(projectListings);
                this.loading$.next(false);
            });
    }

    ngAfterViewInit(): void {
        setTimeout(() => this.form.updateValueAndValidity());
    }

    load(): void {
        const newProjectId = this.form.controls.projectId.value;
        this.projectService.loadAndSetProject(newProjectId).subscribe((project) => {
            this.currentProjectId = newProjectId;
            setTimeout(() => this.form.controls.projectId.updateValueAndValidity({emitEvent: true}));

            this.notificationService.info(`Switched to project »${project.name}«`);
        });
    }
}
