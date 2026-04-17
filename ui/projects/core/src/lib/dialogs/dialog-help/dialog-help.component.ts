import {Component, ChangeDetectionStrategy} from '@angular/core';
import {MatExpansionPanel, MatExpansionPanelHeader, MatExpansionPanelTitle} from '@angular/material/expansion';
import {MatIcon} from '@angular/material/icon';

@Component({
    selector: 'geoengine-dialog-help',
    templateUrl: './dialog-help.component.html',
    styleUrls: ['./dialog-help.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatExpansionPanel, MatExpansionPanelHeader, MatExpansionPanelTitle, MatIcon],
})
export class DialogHelpComponent {}
