import {ChangeDetectionStrategy, Component, ViewContainerRef, inject} from '@angular/core';
import {MatIconRegistry} from '@angular/material/icon';
import {DomSanitizer} from '@angular/platform-browser';
import {Router, RouterOutlet} from '@angular/router';
import {UserService} from '@geoengine/common';

@Component({
    selector: 'geoengine-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [RouterOutlet],
})
export class AppComponent {
    private readonly iconRegistry = inject(MatIconRegistry);
    private readonly sanitizer = inject(DomSanitizer);
    private readonly userService = inject(UserService);
    private readonly router = inject(Router);
    private readonly vcRef = inject(ViewContainerRef);

    constructor() {
        this.registerIcons();

        this.setupLogoutCallback();
    }

    private registerIcons(): void {
        this.iconRegistry.addSvgIconInNamespace(
            'geoengine',
            'logo',
            this.sanitizer.bypassSecurityTrustResourceUrl('assets/geoengine-white.svg'),
        );

        // used for navigation
        this.iconRegistry.addSvgIcon('cogs', this.sanitizer.bypassSecurityTrustResourceUrl('assets/icons/cogs.svg'));
    }

    private setupLogoutCallback(): void {
        this.userService.setLogoutCallback(() => {
            this.router.navigate(['signin']);
        });
    }
}
