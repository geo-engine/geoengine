import {Injectable, inject} from '@angular/core';
import {Subject, Observable} from 'rxjs';
import {MatSnackBar} from '@angular/material/snack-bar';

export enum NotificationType {
    Info,
    Error,
}

export interface Notification {
    type: NotificationType;
    message: string;
}

@Injectable()
export class NotificationService {
    private snackBar = inject(MatSnackBar);

    public notifications: Array<Notification> = [];
    private notification$ = new Subject<Notification>();

    getNotificationStream(): Observable<Notification> {
        return this.notification$;
    }

    info(message: string): void {
        const notification: Notification = {type: NotificationType.Info, message};
        this.notification$.next({
            type: NotificationType.Info,
            message,
        });
        this.snackBar.open(message, undefined, {
            duration: 3000,
        });
        this.notifications.unshift(notification);
    }

    error(message: string): void {
        const notification: Notification = {type: NotificationType.Error, message};
        this.notification$.next({
            type: NotificationType.Error,
            message,
        });
        this.snackBar.open(message, undefined, {
            duration: 5000,
        });
        this.notifications.unshift(notification);
    }
}
