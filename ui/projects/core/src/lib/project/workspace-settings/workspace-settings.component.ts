import {Component, ChangeDetectionStrategy, inject} from '@angular/core';
import {LayoutService} from '../../layout.service';
import {ChangeSpatialReferenceComponent} from '../change-spatial-reference/change-spatial-reference.component';
import {NewProjectComponent} from '../new-project/new-project.component';
import {LoadProjectComponent} from '../load-project/load-project.component';
import {SaveProjectAsComponent} from '../save-project-as/save-project-as.component';
import {NotificationsComponent} from '../notifications/notifications.component';
import {BasemapSelectorComponent} from '../basemap-selector/basemap-selector.component';
import {MatListModule} from '@angular/material/list';
import {MatIconModule} from '@angular/material/icon';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {IfGuestDirective} from '../../util/directives/if-guest.directive';
import {IfLoggedInDirective} from '../../util/directives/if-logged-in.directive';

@Component({
    selector: 'geoengine-workspace-settings',
    templateUrl: './workspace-settings.component.html',
    styleUrls: ['./workspace-settings.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatListModule, MatIconModule, SidenavHeaderComponent, IfGuestDirective, IfLoggedInDirective],
})
export class WorkspaceSettingsComponent {
    protected layoutService = inject(LayoutService);

    loadSpatialReferenceDialog(): void {
        this.layoutService.setSidenavContentComponent({component: ChangeSpatialReferenceComponent, keepParent: true});
    }

    loadNewProjectDialog(): void {
        this.layoutService.setSidenavContentComponent({component: NewProjectComponent, keepParent: true});
    }

    loadChangeProjectDialog(): void {
        this.layoutService.setSidenavContentComponent({component: LoadProjectComponent, keepParent: true});
    }

    loadSaveAsDialog(): void {
        this.layoutService.setSidenavContentComponent({component: SaveProjectAsComponent, keepParent: true});
    }

    loadNotificationsDialog(): void {
        this.layoutService.setSidenavContentComponent({component: NotificationsComponent, keepParent: true});
    }

    loadBasemapDialog(): void {
        this.layoutService.setSidenavContentComponent({component: BasemapSelectorComponent, keepParent: true});
    }
}
