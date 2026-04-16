import {Component} from '@angular/core';
import {MatToolbar} from '@angular/material/toolbar';
import {FxLayoutDirective, FxLayoutGapDirective, FxFlexDirective} from '@geoengine/common';
import {MatButton} from '@angular/material/button';
import {MatDialogClose} from '@angular/material/dialog';
import {MatIcon} from '@angular/material/icon';

@Component({
    selector: 'geoengine-dialog-header',
    templateUrl: './dialog-header.component.html',
    styleUrls: ['./dialog-header.component.scss'],
    imports: [MatToolbar, FxLayoutDirective, FxLayoutGapDirective, FxFlexDirective, MatButton, MatDialogClose, MatIcon],
})
export class DialogHeaderComponent {}
