import {provideHttpClient, withInterceptorsFromDi} from '@angular/common/http';
import {provideAppInitializer, inject, importProvidersFrom, provideZonelessChangeDetection} from '@angular/core';
import {BrowserModule, bootstrapApplication} from '@angular/platform-browser';
import {provideAnimations} from '@angular/platform-browser/animations';
import {CommonConfig, NotificationService, RandomColorService, UserService} from '@geoengine/common';
import {
    CoreConfig,
    LayoutService,
    MapService,
    ProjectService,
    SidenavRef,
    SpatialReferenceService,
    TabsService,
    CoreModule,
} from '@geoengine/core';
import {AppConfig as ManagerAppConfig} from '@geoengine/manager';
import {AppConfig} from './app/app-config.service';
import {AppRoutingModule} from './app/app-routing.module';
import {AppComponent} from './app/app.component';

bootstrapApplication(AppComponent, {
    providers: [
        provideZonelessChangeDetection(),
        importProvidersFrom(BrowserModule, AppRoutingModule, CoreModule),
        AppConfig,
        ManagerAppConfig,
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
                (config: AppConfig, managerConfig: ManagerAppConfig) => (): Promise<void> =>
                    Promise.all([config.load(), managerConfig.load()]).then(() => void 0)
            )(inject(AppConfig), inject(ManagerAppConfig));
            return initializerFn();
        }),
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
    ],
}).catch((err) => console.error(err));
