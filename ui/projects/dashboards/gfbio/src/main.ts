import {provideHttpClient, withInterceptorsFromDi} from '@angular/common/http';
import {provideAppInitializer, inject, importProvidersFrom, provideZonelessChangeDetection} from '@angular/core';
import {MatButtonModule} from '@angular/material/button';
import {MatTableModule} from '@angular/material/table';
import {BrowserModule, bootstrapApplication} from '@angular/platform-browser';
import {provideAnimations} from '@angular/platform-browser/animations';
import {provideRouter} from '@angular/router';
import {CommonConfig, NotificationService, RandomColorService, UserService} from '@geoengine/common';
import {
    CoreConfig,
    DatasetService,
    LayoutService,
    MapService,
    ProjectService,
    SidenavRef,
    SpatialReferenceService,
    TabsService,
    CoreModule,
} from '@geoengine/core';
import {AppConfig} from './app/app-config.service';
import {AppComponent} from './app/app.component';

bootstrapApplication(AppComponent, {
    providers: [
        provideZonelessChangeDetection(),
        importProvidersFrom(BrowserModule, MatTableModule, MatButtonModule, CoreModule),
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
        DatasetService,
        LayoutService,
        MapService,
        NotificationService,
        ProjectService,
        RandomColorService,
        SidenavRef,
        SpatialReferenceService,
        UserService,
        TabsService,
        provideHttpClient(withInterceptorsFromDi()),
        provideAnimations(),
        provideRouter([{path: '**', component: AppComponent}]),
    ],
}).catch((err) => console.error(err));
