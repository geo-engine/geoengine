import {Component, ChangeDetectionStrategy} from '@angular/core';
import {SidenavHeaderComponent} from '@geoengine/core';

@Component({
    selector: 'geoengine-gfbio-help',
    templateUrl: 'help.component.html',
    styleUrls: ['./help.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [SidenavHeaderComponent],
})
export class HelpComponent {}
