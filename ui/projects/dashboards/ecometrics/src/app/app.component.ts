import {Component, inject} from '@angular/core';
import {DashboardComponent} from './dashboard/dashboard.component';
import {CoreConfig, CoreModule} from '@geoengine/core';
import {DataSelectionService} from './data-selection.service';

@Component({
    selector: 'geoengine-root',
    imports: [CoreModule, DashboardComponent],
    providers: [DataSelectionService],
    templateUrl: './app.component.html',
    styleUrl: './app.component.scss',
})
export class AppComponent {
    title = 'ecometrics';

    config = inject(CoreConfig);
}
