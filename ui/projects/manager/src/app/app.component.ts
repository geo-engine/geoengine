import {OnInit, ViewContainerRef, inject, ChangeDetectionStrategy} from '@angular/core';
import {Component} from '@angular/core';
import {Router, RouterOutlet} from '@angular/router';
import {UserService} from '@geoengine/common';
import {firstValueFrom} from 'rxjs';

@Component({
    selector: 'geoengine-root',
    templateUrl: './app.component.html',
    styleUrl: './app.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [RouterOutlet],
})
export class AppComponent implements OnInit {
    private readonly vcRef = inject(ViewContainerRef);
    private readonly router = inject(Router);
    private readonly userService = inject(UserService);

    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    async ngOnInit(): Promise<void> {
        // wait for login to be completed before initializing the router
        await firstValueFrom(this.userService.getSessionOrUndefinedStream());

        this.router.initialNavigation();
    }
}
