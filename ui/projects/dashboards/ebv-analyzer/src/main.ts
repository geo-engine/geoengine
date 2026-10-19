import {LayoutModule} from '@angular/cdk/layout';
import {PortalModule} from '@angular/cdk/portal';
import {provideHttpClient, withInterceptorsFromDi, withXhr} from '@angular/common/http';
import {provideAppInitializer, inject, importProvidersFrom, provideZonelessChangeDetection} from '@angular/core';
import {BrowserModule, bootstrapApplication} from '@angular/platform-browser';
import {provideAnimations} from '@angular/platform-browser/animations';
import {provideRouter} from '@angular/router';
import {NgxMatSelectSearchModule} from 'ngx-mat-select-search';
import {CommonConfig, NotificationService, RandomColorService, UserService} from '@geoengine/common';
import {CoreConfig, LayoutService, MapService, ProjectService, SidenavRef, SpatialReferenceService, CoreModule} from '@geoengine/core';
import {AppConfig} from './app/app-config.service';
import {AppComponent} from './app/app.component';
import {DataSelectionService} from './app/data-selection.service';

bootstrapApplication(AppComponent, {
    providers: [
        provideZonelessChangeDetection(),
        importProvidersFrom(BrowserModule, CoreModule, PortalModule, NgxMatSelectSearchModule, LayoutModule),
        AppConfig,
        {
            provide: CoreConfig,
            useExisting: AppConfig,
        },
        {
            provide: CommonConfig,
            useExisting: AppConfig,
        },
        provideAppInitializer(() => {
            const initializerFn = (
                (config: AppConfig) => (): Promise<void> =>
                    config.load()
            )(inject(AppConfig));
            return initializerFn();
        }),
        LayoutService,
        MapService,
        NotificationService,
        ProjectService,
        RandomColorService,
        SidenavRef,
        SpatialReferenceService,
        DataSelectionService,
        UserService,
        provideHttpClient(withXhr(), withInterceptorsFromDi()),
        provideAnimations(),
        provideRouter([{path: '**', component: AppComponent}]),
    ],
}).catch((err) => console.error(err));
