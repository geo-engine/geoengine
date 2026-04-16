import {Subscription, Observable, combineLatest, of as observableOf} from 'rxjs';
import {Component, OnInit, ChangeDetectionStrategy, OnDestroy, ChangeDetectorRef, inject, input} from '@angular/core';
import {LayoutService, SidenavConfig} from '../../layout.service';
import {ThemePalette} from '@angular/material/core';
import {distinctUntilChanged, map, mergeScan} from 'rxjs/operators';
import {CoreConfig} from '../../config.service';
import {SidenavRef} from '../sidenav-ref.service';
import {OidcComponent} from '../../users/oidc/oidc.component';
import {UserService, AsyncStringSanitizer} from '@geoengine/common';
import {MatIconButton} from '@angular/material/button';
import {MatTooltip} from '@angular/material/tooltip';
import {MatIcon} from '@angular/material/icon';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {AsyncPipe} from '@angular/common';

/**
 * Button config for the sidenav navigation
 *
 * The icon can be a name or an svg image.
 * Furthermore, there is the option to define observables that specify icon as well as color
 * upon user-defined events.
 */
export interface NavigationButton {
    sidenavConfig?: SidenavConfig;
    icon: NavigationButtonIconName | NavigationButtonIconNameObservable | NavigationButtonIconSvg | NavigationButtonIconLoading;
    tooltip: string;
    colorObservable?: Observable<ThemePalette>;
    tooltipObservable?: Observable<string>;
}

export interface NavigationButtonIcon {
    type: string;
}

export interface NavigationButtonIconName extends NavigationButtonIcon {
    type: 'icon';
    name: string;
}

export interface NavigationButtonIconSvg extends NavigationButtonIcon {
    type: 'svg';
    name: string;
}

export interface NavigationButtonIconNameObservable extends NavigationButtonIcon {
    type: 'observableIcon';
    name: Observable<string>;
}

export interface NavigationButtonIconLoading extends NavigationButtonIcon {
    type: 'loading';
}

/**
 * This component lists all buttons for the sidenav navigation.
 */
@Component({
    selector: 'geoengine-navigation',
    templateUrl: './navigation.component.html',
    styleUrls: ['./navigation.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatIconButton, MatTooltip, MatIcon, MatProgressSpinner, AsyncPipe, AsyncStringSanitizer],
})
export class NavigationComponent implements OnInit, OnDestroy {
    private layoutService = inject(LayoutService);
    private sidenavRef = inject(SidenavRef);
    private changeDetectorRef = inject(ChangeDetectorRef);

    /**
     * The navigation shows this array of buttons.
     */
    readonly buttons = input.required<Array<NavigationButton>>();

    private sidenavConfig?: SidenavConfig;
    private sidenavConfigSubscription?: Subscription;

    ngOnInit(): void {
        this.sidenavConfigSubscription = this.layoutService.getSidenavContentComponentStream().subscribe((sidenavConfig) => {
            this.sidenavConfig = sidenavConfig;
            this.changeDetectorRef.markForCheck();
        });
    }

    ngOnDestroy(): void {
        if (this.sidenavConfigSubscription) {
            this.sidenavConfigSubscription.unsubscribe();
        }
    }

    /**
     * Load a component into the sidenav
     */
    setComponent(sidenavConfig: SidenavConfig): void {
        this.layoutService.setSidenavContentComponent(sidenavConfig);
    }

    /**
     * Map the sidenavConfig to a theme palette color for the button
     */
    buttonColor(sidenavConfig?: SidenavConfig): ThemePalette {
        if (!sidenavConfig || !this.sidenavConfig) {
            return undefined;
        }

        const currentComponent = this.sidenavConfig.component;
        const parentComponent = this.sidenavRef.getBackButtonComponent()?.component;
        if (currentComponent === sidenavConfig.component || parentComponent === sidenavConfig.component) {
            return 'primary';
        } else {
            return undefined;
        }
    }

    /**
     * Default constructor for a login button in the navigation.
     */
    static createLoginButton(
        userService: UserService,
        layoutService: LayoutService,
        config: CoreConfig,
        loginSidenavConfig?: SidenavConfig,
    ): NavigationButton {
        loginSidenavConfig = loginSidenavConfig ?? {component: OidcComponent};
        return {
            sidenavConfig: loginSidenavConfig,
            icon: {
                type: 'observableIcon',
                name: userService.isGuestUserStream().pipe(map((isGuest) => (isGuest ? 'person_outline' : 'person'))),
            },
            tooltip: '',
            tooltipObservable: userService.isGuestUserStream().pipe(map((isGuest) => (isGuest ? 'Login' : 'User Account'))),
            colorObservable: combineLatest([userService.isGuestUserStream(), layoutService.getSidenavContentComponentStream()]).pipe(
                distinctUntilChanged(),
                mergeScan<[boolean, SidenavConfig | undefined], [boolean, string | undefined]>(
                    // abort inner observable when new source event arises
                    ([wasGuest, _state], [isGuest, sidenavConfig], _index) => {
                        if (sidenavConfig?.component === loginSidenavConfig?.component) {
                            return observableOf([isGuest, 'primary']);
                        } else if (!wasGuest && isGuest) {
                            // show 'accent' color for some time
                            return new Observable((observer) => {
                                observer.next([isGuest, 'accent']);
                                setTimeout(() => {
                                    observer.next([isGuest, undefined]);
                                    observer.complete();
                                }, config.DELAYS.GUEST_LOGIN_HINT);
                            });
                        } else {
                            return observableOf([isGuest, undefined]);
                        }
                    },
                    [true, 'accent' as ThemePalette],
                ),
                map(([_wasGuest, state]) => state as ThemePalette),
            ),
        };
    }

    static createLoadingButton(tooltip: string): NavigationButton {
        return {
            sidenavConfig: undefined,
            icon: {
                type: 'loading',
            },
            tooltip,
        };
    }

    static createAddDataButton(addDataConfig: SidenavConfig): NavigationButton {
        return {
            sidenavConfig: addDataConfig,
            icon: {
                type: 'icon',
                name: 'add',
            },
            tooltip: 'Add Data',
        };
    }
}
