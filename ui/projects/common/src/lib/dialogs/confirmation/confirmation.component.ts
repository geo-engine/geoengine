import {CdkScrollable} from '@angular/cdk/scrolling';
import {Component, inject, ChangeDetectionStrategy} from '@angular/core';
import {MatButton} from '@angular/material/button';
import {MAT_DIALOG_DATA, MatDialogRef, MatDialogTitle, MatDialogContent, MatDialogActions, MatDialogClose} from '@angular/material/dialog';

@Component({
    selector: 'geoengine-confirmation-dialog',
    templateUrl: './confirmation.component.html',
    styleUrl: './confirmation.component.css',
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatDialogTitle, CdkScrollable, MatDialogContent, MatDialogActions, MatButton, MatDialogClose],
})
export class ConfirmationComponent {
    dialogRef = inject<MatDialogRef<ConfirmationComponent>>(MatDialogRef);
    readonly data = inject<{
        message: string;
    }>(MAT_DIALOG_DATA);
}
