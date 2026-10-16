import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';
import {BackendStatusPageComponent, NotFoundPageComponent} from '@geoengine/core';
import {AboutComponent} from './about/about.component';
import {ComputeComponent} from './compute/compute.component';
import {LayersComponent} from './layers/layers.component';
import {MainComponent} from './main/main.component';
import {BackendAvailableGuard, CanRegisterGuard, LoginComponent, LogInGuard, RegisterComponent} from '@geoengine/common';

const routes: Routes = [
    {path: '', redirectTo: 'map', pathMatch: 'full'},
    {
        path: 'map',
        component: MainComponent,
        canActivate: [BackendAvailableGuard, LogInGuard],
        children: [
            {path: '', redirectTo: 'layers', pathMatch: 'full'},
            {path: 'layers', component: LayersComponent},
            {path: 'compute', component: ComputeComponent},
            {path: 'about', component: AboutComponent},
        ],
    },
    {path: 'signin', component: LoginComponent, canActivate: [BackendAvailableGuard]},
    {path: 'register', component: RegisterComponent, canActivate: [BackendAvailableGuard, CanRegisterGuard]},
    {path: '404', component: NotFoundPageComponent},
    {path: 'backend-status', component: BackendStatusPageComponent},
    // fallback to not found page
    {path: '**', redirectTo: '404', pathMatch: 'full'},
];

@NgModule({
    imports: [
        RouterModule.forRoot(routes, {
            bindToComponentInputs: true,
        }),
    ],
    providers: [BackendAvailableGuard, LogInGuard, CanRegisterGuard],
    exports: [RouterModule],
})
export class AppRoutingModule {}
