import {Routes} from '@angular/router';
import {LogInGuard} from '@geoengine/common';
import {DashboardComponent} from './dashboard/dashboard.component';
import {LoginComponent} from './login/login.component';

export const routes: Routes = [
    {path: '', redirectTo: 'dashboard', pathMatch: 'full'},
    {path: 'dashboard', component: DashboardComponent, canActivate: [LogInGuard]},
    {path: 'signin', component: LoginComponent},
];
