import {Component, ChangeDetectionStrategy} from '@angular/core';
import {MatButton} from '@angular/material/button';
import {MatDialogClose} from '@angular/material/dialog';
import {MatIcon} from '@angular/material/icon';
import {MatToolbar} from '@angular/material/toolbar';
import {FxLayoutDirective, FxLayoutGapDirective, FxFlexDirective} from '@geoengine/common';

@Component({
    selector: 'geoengine-dialog-header',
    templateUrl: './dialog-header.component.html',
    styleUrls: ['./dialog-header.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatToolbar, FxLayoutDirective, FxLayoutGapDirective, FxFlexDirective, MatButton, MatDialogClose, MatIcon],
})
export class DialogHeaderComponent {}
