import {Component, ChangeDetectionStrategy} from '@angular/core';
import {MatDivider} from '@angular/material/list';

@Component({
    selector: 'geoengine-about',
    templateUrl: './about.component.html',
    styleUrls: ['./about.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatDivider],
})
export class AboutComponent {}
