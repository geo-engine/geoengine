import {Component, inject} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogRef, MatDialogTitle, MatDialogContent, MatDialogActions, MatDialogClose} from '@angular/material/dialog';
import {CdkScrollable} from '@angular/cdk/scrolling';
import {MatButton} from '@angular/material/button';

@Component({
    selector: 'geoengine-confirmation-dialog',
    templateUrl: './confirmation.component.html',
    styleUrl: './confirmation.component.css',
    imports: [MatDialogTitle, CdkScrollable, MatDialogContent, MatDialogActions, MatButton, MatDialogClose],
})
export class ConfirmationComponent {
    dialogRef = inject<MatDialogRef<ConfirmationComponent>>(MatDialogRef);
    readonly data = inject<{
        message: string;
    }>(MAT_DIALOG_DATA);
}
