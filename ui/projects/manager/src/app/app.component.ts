import {OnInit, ViewContainerRef, inject} from '@angular/core';
import {Component} from '@angular/core';
import {Router, RouterOutlet} from '@angular/router';
import {Location} from '@angular/common';
import {UserService} from '@geoengine/common';
import {firstValueFrom} from 'rxjs';

@Component({
    selector: 'geoengine-root',
    templateUrl: './app.component.html',
    styleUrl: './app.component.scss',
    imports: [RouterOutlet],
})
export class AppComponent implements OnInit {
    private readonly vcRef = inject(ViewContainerRef);
    private readonly router = inject(Router);
    private readonly location = inject(Location);
    private readonly userService = inject(UserService);

    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    async ngOnInit(): Promise<void> {
        // wait for login to be completed before initializing the router
        await firstValueFrom(this.userService.getSessionOrUndefinedStream());

        if (window.location.search.length > 0) {
            const hashLocation = window.location.hash.slice(1); // remove the leading '#'

            // remove the query parameters before the hash from the url because they are not part of the app and cannot be removed later on
            // services can get the original query parameters from the `URLSearchParams` in their constructor which is called before the routing is initialized.
            const search = window.location.search;
            window.history.replaceState(null, '', window.location.pathname);
            this.location.go(hashLocation, search);
        }

        this.router.initialNavigation();
    }
}
