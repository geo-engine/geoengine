import {AppConfig} from './app/app-config.service';
import {CoreConfig, LayoutService, MapService, ProjectService, SidenavRef, SpatialReferenceService, CoreModule} from '@geoengine/core';
import {CommonConfig, NotificationService, RandomColorService, UserService} from '@geoengine/common';
import {provideAppInitializer, inject, importProvidersFrom, provideZonelessChangeDetection} from '@angular/core';
import {DataSelectionService} from './app/data-selection.service';
import {provideHttpClient, withInterceptorsFromDi} from '@angular/common/http';
import {provideAnimations} from '@angular/platform-browser/animations';
import {BrowserModule, bootstrapApplication} from '@angular/platform-browser';
import {withHashLocation, provideRouter} from '@angular/router';
import {AppComponent} from './app/app.component';
import {PortalModule} from '@angular/cdk/portal';
import {NgxMatSelectSearchModule} from 'ngx-mat-select-search';
import {LayoutModule} from '@angular/cdk/layout';

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
        provideHttpClient(withInterceptorsFromDi()),
        provideAnimations(),
        provideRouter([{path: '**', component: AppComponent}], withHashLocation()),
    ],
}).catch((err) => console.error(err));
