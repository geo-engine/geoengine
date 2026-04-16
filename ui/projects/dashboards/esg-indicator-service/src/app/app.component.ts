import {Component, inject} from '@angular/core';
import {RouterOutlet} from '@angular/router';
import {CoreConfig, CoreModule} from '@geoengine/core';
import {DataSelectionService} from './data-selection.service';

@Component({
    selector: 'geoengine-root',
    imports: [RouterOutlet, CoreModule],
    providers: [DataSelectionService],
    templateUrl: './app.component.html',
    styleUrl: './app.component.scss',
})
export class AppComponent {
    title = 'esg-indicator-service';

    config = inject(CoreConfig);
}
