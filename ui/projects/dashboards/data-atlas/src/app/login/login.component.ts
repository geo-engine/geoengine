import {ChangeDetectionStrategy, Component} from '@angular/core';
import {CoreModule} from '@geoengine/core';
import {MatCardHeader, MatCardTitle, MatCardSubtitle} from '@angular/material/card';

@Component({
    selector: 'geoengine-login',
    templateUrl: './login.component.html',
    styleUrls: ['./login.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [CoreModule, MatCardHeader, MatCardTitle, MatCardSubtitle],
})
export class LoginComponent {}
