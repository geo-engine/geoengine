import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';
import {BackendStatusPageComponent, NotFoundPageComponent} from '@geoengine/core';
import {LoginComponent} from './login/login.component';
import {MainComponent} from './main/main.component';
import {BackendAvailableGuard, LogInGuard} from '@geoengine/common';

const routes: Routes = [
    {path: '', redirectTo: 'map', pathMatch: 'full'},
    {path: 'map', component: MainComponent, canActivate: [BackendAvailableGuard, LogInGuard]},
    {path: 'signin', component: LoginComponent, canActivate: [BackendAvailableGuard]},
    {path: '404', component: NotFoundPageComponent},
    {path: 'backend-status', component: BackendStatusPageComponent},
    // fallback to not found page
    {path: '**', redirectTo: '404', pathMatch: 'full'},
];

@NgModule({
    imports: [RouterModule.forRoot(routes, {useHash: true})],
    providers: [BackendAvailableGuard, LogInGuard],
    exports: [RouterModule],
})
export class AppRoutingModule {}
