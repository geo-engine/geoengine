import {inject, NgModule} from '@angular/core';
import {ActivatedRouteSnapshot, CanActivateFn, RouterModule, RouterStateSnapshot, Routes} from '@angular/router';
import {BackendStatusPageComponent, NotFoundPageComponent} from '@geoengine/core';
import {MainComponent} from './main/main.component';
import {BackendAvailableGuard, CanRegisterGuard, LoginComponent, LogInGuard, RegisterComponent} from '@geoengine/common';
import {AppConfig} from './app-config.service';

export const routeToManager: CanActivateFn = (_route: ActivatedRouteSnapshot, _state: RouterStateSnapshot) => {
    const config = inject(AppConfig);
    return config.ROUTES.MANAGER;
};

const routes: Routes = [
    {path: '', redirectTo: 'map', pathMatch: 'full'},
    {path: 'map', component: MainComponent, canActivate: [BackendAvailableGuard, LogInGuard]},
    {path: 'signin', component: LoginComponent, canActivate: [BackendAvailableGuard]},
    {path: 'register', component: RegisterComponent, canActivate: [BackendAvailableGuard, CanRegisterGuard]},
    {path: '404', component: NotFoundPageComponent},
    {path: 'backend-status', component: BackendStatusPageComponent},
    // manager
    {
        path: 'manager',
        loadChildren: () => import('@geoengine/manager').then((m) => (m as {routes: (subdir?: string) => Routes}).routes('/manager')),
        canActivate: [routeToManager, BackendAvailableGuard],
    },
    // fallback to not found page
    {path: '**', redirectTo: '404', pathMatch: 'full'},
];

@NgModule({
    imports: [
        RouterModule.forRoot(routes, {
            initialNavigation: 'disabled', // navigation is enabled in app component after removing query params before the hash
            onSameUrlNavigation: 'reload', // for reload the page and checking if the user is logged in again
            bindToComponentInputs: true,
        }),
    ],
    providers: [BackendAvailableGuard, LogInGuard, CanRegisterGuard],
    exports: [RouterModule],
})
export class AppRoutingModule {}
