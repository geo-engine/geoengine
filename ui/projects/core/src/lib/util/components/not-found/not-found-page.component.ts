import {Component, inject} from '@angular/core';
import {Router} from '@angular/router';
import {FxLayoutDirective, FxLayoutAlignDirective} from '@geoengine/common';
import {MatCard, MatCardTitle, MatCardMdImage, MatCardContent, MatCardActions} from '@angular/material/card';
import {MatIcon} from '@angular/material/icon';
import {MatButton} from '@angular/material/button';

@Component({
    selector: 'geoengine-not-found-page',
    templateUrl: './not-found-page.component.html',
    styleUrls: ['./not-found-page.component.scss'],
    imports: [
        FxLayoutDirective,
        FxLayoutAlignDirective,
        MatCard,
        MatCardTitle,
        MatIcon,
        MatCardMdImage,
        MatCardContent,
        MatCardActions,
        MatButton,
    ],
})
export class NotFoundPageComponent {
    private router = inject(Router);

    async goBack(): Promise<void> {
        await this.router.navigate(['/']);
    }
}
