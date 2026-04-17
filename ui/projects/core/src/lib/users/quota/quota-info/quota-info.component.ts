import {ChangeDetectorRef, Component, OnDestroy, OnInit, inject} from '@angular/core';
import {Quota, UserService} from '@geoengine/common';
import {Subscription, timer} from 'rxjs';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';

@Component({
    selector: 'geoengine-quota-info',
    templateUrl: './quota-info.component.html',
    styleUrls: ['./quota-info.component.scss'],
    imports: [MatIconButton, MatIcon],
})
export class QuotaInfoComponent implements OnDestroy, OnInit {
    protected readonly userService = inject(UserService);
    protected readonly changeDetectorRef = inject(ChangeDetectorRef);

    sessionQuota: Quota | undefined;
    sessionQuotaSubscription: Subscription | undefined;
    timerSubscription: Subscription | undefined;

    static readonly refreshTime: number = 30000;

    ngOnInit(): void {
        this.sessionQuotaSubscription = this.userService.getSessionQuotaStream().subscribe((quota) => {
            this.sessionQuota = quota;
            this.changeDetectorRef.detectChanges();
        });
        this.timerSubscription = timer(0, QuotaInfoComponent.refreshTime).subscribe(() => {
            this.refreshQuota();
        });
    }

    ngOnDestroy(): void {
        this.timerSubscription?.unsubscribe();
        this.sessionQuotaSubscription?.unsubscribe();
    }

    protected hasQuota(): boolean {
        return !!this.sessionQuota;
    }

    protected refreshQuota(): void {
        this.userService.refreshSessionQuota();
    }
}
