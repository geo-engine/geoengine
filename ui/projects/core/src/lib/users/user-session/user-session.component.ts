import {Component, OnDestroy, inject} from '@angular/core';
import {Subscription} from 'rxjs';
import {Session} from '../session.model';
import {Clipboard} from '@angular/cdk/clipboard';
import {NotificationService, UserService} from '@geoengine/common';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';

@Component({
    selector: 'geoengine-user-session',
    templateUrl: './user-session.component.html',
    styleUrls: ['./user-session.component.scss'],
    imports: [MatIconButton, MatIcon],
})
export class UserSessionComponent implements OnDestroy {
    protected readonly userService = inject(UserService);
    protected clipboard = inject(Clipboard);
    protected readonly notificationService = inject(NotificationService);

    protected session: Session | undefined;
    private sessionStreamSubscription: Subscription;

    constructor() {
        const subs = this.userService.getSessionStream().subscribe((session) => {
            this.session = session;
        });

        this.sessionStreamSubscription = subs;
    }

    ngOnDestroy(): void {
        this.sessionStreamSubscription.unsubscribe();
    }

    copySessionTokenToClipboard(): void {
        if (this.session) {
            this.clipboard.copy(this.session.sessionToken);
            this.notificationService.info('Session token copied to clipboard: ' + this.session.sessionToken);
        }
    }
}
