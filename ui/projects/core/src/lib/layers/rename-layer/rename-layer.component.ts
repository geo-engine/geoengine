import {Component, OnInit, ChangeDetectionStrategy, inject} from '@angular/core';
import {FormControl, FormGroup, NonNullableFormBuilder, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ProjectService} from '../../project/project.service';
import {MAT_DIALOG_DATA, MatDialogRef, MatDialogContent, MatDialogActions} from '@angular/material/dialog';
import {Layer} from '@geoengine/common';
import {DialogHeaderComponent} from '../../dialogs/dialog-header/dialog-header.component';
import {CdkScrollable} from '@angular/cdk/scrolling';
import {MatFormField, MatInput} from '@angular/material/input';
import {MatButton} from '@angular/material/button';

@Component({
    selector: 'geoengine-rename-layer',
    templateUrl: './rename-layer.component.html',
    styleUrls: ['./rename-layer.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        DialogHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        CdkScrollable,
        MatDialogContent,
        MatFormField,
        MatInput,
        MatDialogActions,
        MatButton,
    ],
})
export class RenameLayerComponent implements OnInit {
    private projectService = inject(ProjectService);
    private formBuilder = inject(NonNullableFormBuilder);
    private dialogRef = inject<MatDialogRef<RenameLayerComponent>>(MatDialogRef);
    private config = inject<{
        layer?: Layer;
    }>(MAT_DIALOG_DATA);

    form: FormGroup<{layerName: FormControl<string | undefined>}>;

    private layer?: Layer;

    constructor() {
        this.form = this.formBuilder.group({
            layerName: this.formBuilder.control<string | undefined>(undefined, Validators.required),
        });
    }

    ngOnInit(): void {
        this.layer = this.config.layer;
        this.form.controls.layerName.setValue(this.layer?.name);
    }

    /**
     * Save the layer name and close the dialog.
     */
    save(): void {
        if (!this.layer || !this.form) {
            return;
        }

        const layerName = this.form.controls.layerName.value;
        if (layerName === undefined || layerName === this.layer.name) {
            return;
        }

        this.projectService.changeLayer(this.layer, {name: layerName}).subscribe(
            () => this.dialogRef.close(),
            (_error) => {
                // TODO: handle error
            },
        );
    }
}
