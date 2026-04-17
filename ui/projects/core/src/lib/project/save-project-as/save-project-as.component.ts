import {BehaviorSubject} from 'rxjs';
import {Component, ChangeDetectionStrategy, AfterViewInit, inject} from '@angular/core';
import {UntypedFormGroup, UntypedFormBuilder, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ProjectService} from '../project.service';
import {geoengineValidators, NotificationService, FxLayoutDirective, FxFlexDirective} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatFormField, MatInput, MatHint} from '@angular/material/input';
import {MatButton} from '@angular/material/button';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-save-project-as',
    templateUrl: './save-project-as.component.html',
    styleUrls: ['./save-project-as.component.scss'],
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
        MatButton,
        AsyncPipe,
    ],
})
export class SaveProjectAsComponent implements AfterViewInit {
    private formBuilder = inject(UntypedFormBuilder);
    private projectService = inject(ProjectService);
    private notificationService = inject(NotificationService);

    form: UntypedFormGroup;

    created$ = new BehaviorSubject(false);

    constructor() {
        this.form = this.formBuilder.group({
            name: [
                '',
                Validators.compose([Validators.required, geoengineValidators.notOnlyWhitespace]),
                // geoengineValidators.uniqueProjectName(this.storageService), // TODO: check for uniqueness
            ],
        });
        this.projectService.getProjectOnce().subscribe((project) => {
            this.form.controls['name'].setValue(project.name);
        });
    }

    ngAfterViewInit(): void {
        setTimeout(() => this.form.updateValueAndValidity());
    }

    /**
     * Save project under new name.
     *
     */
    save(): void {
        const projectName: string = this.form.controls['name'].value;

        this.projectService.cloneProject(projectName).subscribe((project) => {
            this.projectService.setProject(project);
            this.created$.next(true);
            this.notificationService.info(`Saved project to »${project.name}« and switched to it`);
        });
    }
}
