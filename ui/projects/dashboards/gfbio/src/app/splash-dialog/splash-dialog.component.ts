import {Component, ChangeDetectionStrategy} from '@angular/core';
import {CoreModule} from '@geoengine/core';
import {CdkScrollable} from '@angular/cdk/scrolling';
import {MatDialogContent} from '@angular/material/dialog';

@Component({
    selector: 'geoengine-gfbio-splash-dialog',
    templateUrl: './splash-dialog.component.html',
    styleUrls: ['./splash-dialog.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [CoreModule, CdkScrollable, MatDialogContent],
})
export class SplashDialogComponent {
    static SPLASH_DIALOG_NAME = 'showStartupSplashScreen';

    get splashName(): string {
        return SplashDialogComponent.SPLASH_DIALOG_NAME;
    }
}
