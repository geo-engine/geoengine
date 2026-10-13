import {ChangeDetectionStrategy, Component, OnInit, ViewContainerRef, inject} from '@angular/core';
import {MatIconRegistry} from '@angular/material/icon';
import {DomSanitizer, Title} from '@angular/platform-browser';
import {Router, RouterOutlet} from '@angular/router';
import {AppConfig} from './app-config.service';
import {Location} from '@angular/common';
import {UserService} from '@geoengine/common';

@Component({
    selector: 'geoengine-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [RouterOutlet],
})
export class AppComponent implements OnInit {
    private readonly iconRegistry = inject(MatIconRegistry);
    private readonly sanitizer = inject(DomSanitizer);
    private readonly userService = inject(UserService);
    private readonly router = inject(Router);
    readonly config = inject<AppConfig>(AppConfig);
    private readonly titleService = inject(Title);
    private readonly vcRef = inject(ViewContainerRef);
    private readonly location = inject(Location);

    constructor() {
        this.registerIcons();

        this.setupLogoutCallback();

        this.titleService.setTitle(this.config.BRANDING.PAGE_TITLE);
    }

    ngOnInit(): void {
        if (window.location.search.length > 0) {
            // remove the query parameters before the hash from the url because they are not part of the app and cannot be removed later on
            // services can get the original query parameters from the `URLSearchParams` in their constructor which is called before the routing is initialized.
            const search = window.location.search;

            let path = '/';
            if (window.location.hash.length > 0) {
                path = window.location.hash.substring(1); // remove the leading #
            }

            window.history.replaceState(null, '', window.location.pathname);
            this.location.go(path, search);
        }

        this.router.initialNavigation();
    }

    private registerIcons(): void {
        this.iconRegistry.addSvgIconInNamespace(
            'geoengine',
            'logo',
            this.sanitizer.bypassSecurityTrustResourceUrl(this.config.BRANDING.LOGO_URL),
        );

        this.iconRegistry.addSvgIconInNamespace(
            'geoengine',
            'favicon-white',
            this.sanitizer.bypassSecurityTrustResourceUrl(this.config.BRANDING.LOGO_ICON_URL),
        );

        this.iconRegistry.addSvgIconInNamespace(
            'geoengine',
            'logo-alt',
            this.sanitizer.bypassSecurityTrustResourceUrl(this.config.BRANDING.LOGO_ALT_URL),
        );

        // used for navigation
        this.iconRegistry.addSvgIcon('cogs', this.sanitizer.bypassSecurityTrustResourceUrl('assets/icons/cogs.svg'));
    }

    private setupLogoutCallback(): void {
        this.userService.setLogoutCallback(() => {
            void this.router.navigate(['signin']);
        });
    }
}
