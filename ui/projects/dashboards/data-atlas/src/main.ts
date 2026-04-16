import {AppConfig} from './app/app-config.service';
import {
    CoreConfig,
    DatasetService,
    LayoutService,
    MapService,
    ProjectService,
    SidenavRef,
    SpatialReferenceService,
    CoreModule,
} from '@geoengine/core';
import {CommonConfig, NotificationService, RandomColorService, UserService} from '@geoengine/common';
import {provideAppInitializer, inject, importProvidersFrom, provideZonelessChangeDetection} from '@angular/core';
import {AppDatasetService} from './app/app-dataset.service';
import {provideHttpClient, withInterceptorsFromDi} from '@angular/common/http';
import {provideAnimations} from '@angular/platform-browser/animations';
import {BrowserModule, bootstrapApplication} from '@angular/platform-browser';
import {withHashLocation, provideRouter} from '@angular/router';
import {AppComponent} from './app/app.component';
import {PortalModule} from '@angular/cdk/portal';
import {AppRoutingModule} from './app/app-routing.module';
import {NgxMatSelectSearchModule} from 'ngx-mat-select-search';

bootstrapApplication(AppComponent, {
    providers: [
        provideZonelessChangeDetection(),
        importProvidersFrom(BrowserModule, CoreModule, PortalModule, AppRoutingModule, NgxMatSelectSearchModule),
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
        {provide: DatasetService, useClass: AppDatasetService},
        LayoutService,
        MapService,
        NotificationService,
        ProjectService,
        RandomColorService,
        SidenavRef,
        SpatialReferenceService,
        UserService,
        provideHttpClient(withInterceptorsFromDi()),
        provideAnimations(),
        provideRouter([{path: '**', component: AppComponent}], withHashLocation()),
    ],
}).catch((err) => console.error(err));
