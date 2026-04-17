import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';
import {NavigationComponent} from './navigation/navigation.component';
import {LogInGuard} from './util/guards/log-in.guard';
import {BackendAvailableGuard, CanRegisterGuard, LoginComponent, RegisterComponent} from '@geoengine/common';
import {OidcPopupComponent} from './oidc-popup/oidc-popup.component';

export const routes: (subdir: string) => Routes = (subdir) => [
    {path: '', redirectTo: 'navigation', pathMatch: 'full'},
    {path: 'navigation', component: NavigationComponent, data: {logoutNavigation: '/signin'}, canActivate: [LogInGuard]},
    {path: 'signin', component: LoginComponent, data: {loginRedirect: subdir + '/navigation'}},
    {
        path: 'register',
        component: RegisterComponent,
        data: {loginRedirect: subdir + '/navigation'},
        canActivate: [BackendAvailableGuard, CanRegisterGuard],
    },
    {
        path: 'oidc-popup',
        component: OidcPopupComponent,
    },
];

@NgModule({
    imports: [
        RouterModule.forRoot(routes(''), {
            initialNavigation: 'disabled', // navigation is enabled in app component after removing query params before the hash
            onSameUrlNavigation: 'reload', // for reload the page and checking if the user is logged in again
            bindToComponentInputs: true,
            // enableTracing: true, // TODO: remove after debugging
        }),
    ],
    exports: [RouterModule],
})
export class AppRoutingModule {}
