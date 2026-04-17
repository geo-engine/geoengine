import {AfterViewInit, Component, computed, effect, inject, isDevMode, signal} from '@angular/core';
import {AppConfig} from '../app-config.service';
import {KeyValuePipe, Location} from '@angular/common';
import {ActivatedRoute, Router} from '@angular/router';

export interface OidcPopupMessage {
    oidcParams: [string, string][];
}

@Component({
    selector: 'geoengine-oidc-popup',
    templateUrl: './oidc-popup.component.html',
    styleUrls: ['./oidc-popup.component.scss'],
    imports: [KeyValuePipe],
})
export class OidcPopupComponent implements AfterViewInit {
    protected readonly config = inject<AppConfig>(AppConfig);

    readonly params = signal<Map<string, string>>(new Map());

    protected readonly location = inject(Location);
    protected readonly router = inject(Router);
    protected readonly route = inject(ActivatedRoute);

    readonly isError = computed(() => this.params().has('error'));
    readonly isDebugMode = isDevMode();

    constructor() {
        effect(() => {
            const params = this.params();
            if (params.size === 0) return;

            window.postMessage({oidcParams: Array.from(params.entries())} as OidcPopupMessage, '*');
        });
    }

    ngAfterViewInit(): void {
        const params = new Map<string, string>();
        Object.entries(this.route.snapshot.queryParams).forEach(([key, value]) => {
            params.set(key, value as string);
        });
        this.params.set(params);
    }
}
