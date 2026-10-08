import {AppConfig} from './app/app-config.service';
import {CoreConfig, MapService, ProjectService, SidenavRef, SpatialReferenceService, CoreModule} from '@geoengine/core';
import {CommonConfig, NotificationService, RandomColorService, UserService} from '@geoengine/common';
import {provideAppInitializer, inject, importProvidersFrom, provideZonelessChangeDetection} from '@angular/core';
import {provideHttpClient, withInterceptorsFromDi} from '@angular/common/http';
import {BrowserModule, bootstrapApplication} from '@angular/platform-browser';
import {AppRoutingModule} from './app/app-routing.module';
import {AppComponent} from './app/app.component';
import {provideNativeDateAdapter} from '@angular/material/core';

bootstrapApplication(AppComponent, {
    providers: [
        provideZonelessChangeDetection(),
        importProvidersFrom(BrowserModule, AppRoutingModule, CoreModule),
        AppConfig,
        {
            provide: CoreConfig,
            useExisting: AppConfig,
        },
        {
            provide: CommonConfig,
            useExisting: AppConfig,
        },
        provideAppInitializer(async () => {
            const config = inject(AppConfig);
            await config.load();
        }),
        MapService,
        NotificationService,
        ProjectService,
        RandomColorService,
        SidenavRef,
        SpatialReferenceService,
        UserService,
        provideNativeDateAdapter(),
        provideHttpClient(withInterceptorsFromDi()),
        // provideAnimations(), // TODO: remove because deprecated
    ],
}).catch((err) => console.error(err));
