import {Directive, TemplateRef, Component, ViewContainerRef, OnDestroy, inject} from '@angular/core';
import {UserService} from '@geoengine/common';
import {Subscription} from 'rxjs';

@Directive({
    selector: '[geoengineIfLoggedIn]',
})
export class IfLoggedInDirective implements OnDestroy {
    private userService = inject(UserService);
    private templateRef = inject<TemplateRef<Component>>(TemplateRef);
    private viewContainer = inject(ViewContainerRef);

    private subscription: Subscription;

    constructor() {
        this.subscription = this.userService.isGuestUserStream().subscribe((isGuest) => {
            this.viewContainer.clear();
            if (!isGuest) {
                this.viewContainer.createEmbeddedView(this.templateRef).markForCheck();
            }
        });
    }

    ngOnDestroy(): void {
        this.subscription.unsubscribe();
    }
}
