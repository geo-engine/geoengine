import {Component, OnInit, ChangeDetectionStrategy, inject} from '@angular/core';
import {NotificationService, Notification} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatCard} from '@angular/material/card';
import {MatList, MatListItem, MatListItemIcon, MatListItemMeta, MatDivider} from '@angular/material/list';
import {MatIcon} from '@angular/material/icon';
import {MatIconButton, MatButton} from '@angular/material/button';

@Component({
    selector: 'geoengine-notifications',
    templateUrl: './notifications.component.html',
    styleUrls: ['./notifications.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        MatCard,
        MatList,
        MatListItem,
        MatIcon,
        MatListItemIcon,
        MatIconButton,
        MatListItemMeta,
        MatDivider,
        MatButton,
    ],
})
export class NotificationsComponent implements OnInit {
    private notificationService = inject(NotificationService);

    notifications: Array<Notification>;

    constructor() {
        this.notifications = [];
    }

    ngOnInit(): void {
        this.notifications = this.notificationService.notifications;
    }

    removeAllNotifications(): void {
        this.notifications.splice(0, this.notifications.length);
    }

    removeCurrentNotification(value: Notification): void {
        const index: number = this.notifications.indexOf(value);
        this.notifications.splice(index, 1);
    }
}
