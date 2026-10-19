import {provideHttpClient} from '@angular/common/http';
import {ApplicationConfig, inject, provideAppInitializer} from '@angular/core';
import {provideAnimations} from '@angular/platform-browser/animations';
import {provideRouter} from '@angular/router';
import {CommonConfig, NotificationService, RandomColorService} from '@geoengine/common';
import {BackendService, CoreConfig, LayoutService, MapService, ProjectService, SidenavRef, SpatialReferenceService} from '@geoengine/core';
import {routes} from './app.routes';
import {DataSelectionService} from './data-selection.service';

export const appConfig: ApplicationConfig = {
    providers: [
        provideRouter(routes),
        provideAnimations(),
        provideHttpClient(),
        provideAppInitializer(() => {
            const initializerFn = (
                (config: CoreConfig) => (): Promise<void> =>
                    config.load()
            )(inject(CoreConfig));
            return initializerFn();
        }),
        {provide: CommonConfig, useExisting: CoreConfig},
        CoreConfig,
        BackendService,
        LayoutService,
        MapService,
        NotificationService,
        ProjectService,
        RandomColorService,
        SidenavRef,
        SpatialReferenceService,
        DataSelectionService,
    ],
};
