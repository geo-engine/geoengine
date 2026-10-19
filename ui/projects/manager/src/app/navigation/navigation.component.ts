import {BreakpointObserver, Breakpoints} from '@angular/cdk/layout';
import {AsyncPipe} from '@angular/common';
import {Component, inject, input, ChangeDetectionStrategy} from '@angular/core';
import {MatNavList, MatListItem} from '@angular/material/list';
import {MatSidenavContainer, MatSidenav, MatSidenavContent} from '@angular/material/sidenav';
import {MatToolbar} from '@angular/material/toolbar';
import {Router} from '@angular/router';
import {Observable} from 'rxjs';
import {map, shareReplay} from 'rxjs/operators';
import {UserService} from '@geoengine/common';
import {AppConfig} from '../app-config.service';
import {DatasetsComponent} from '../datasets/datasets.component';
import {LayersComponent} from '../layers/layers.component';
import {ProvidersComponent} from '../providers/providers.component';

export enum NavigationType {
    Datasets = 'datasets',
    Layers = 'layers',
    Providers = 'providers',
}

@Component({
    selector: 'geoengine-manager-navigation',
    templateUrl: './navigation.component.html',
    styleUrls: ['./navigation.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatSidenavContainer,
        MatSidenav,
        MatToolbar,
        MatNavList,
        MatListItem,
        MatSidenavContent,
        DatasetsComponent,
        LayersComponent,
        AsyncPipe,
        ProvidersComponent,
    ],
})
export class NavigationComponent {
    private breakpointObserver = inject(BreakpointObserver);
    private userService = inject(UserService);
    private router = inject(Router);
    readonly config = inject<AppConfig>(AppConfig);

    readonly logoutNavigation = input('/signin');

    isHandset$: Observable<boolean> = this.breakpointObserver.observe(Breakpoints.Handset).pipe(
        map((result) => result.matches),
        shareReplay(),
    );

    NavigationType = NavigationType;

    selectedType: NavigationType = NavigationType.Datasets;

    async logout(): Promise<void> {
        this.userService.logout();
        const current = this.router.url ?? '';
        const returnUrl = current.split('?')[0];
        await this.router.navigate([this.logoutNavigation()], {queryParams: {returnUrl}});
    }

    toggleSelection(selection: NavigationType): void {
        this.selectedType = selection;
    }
}
