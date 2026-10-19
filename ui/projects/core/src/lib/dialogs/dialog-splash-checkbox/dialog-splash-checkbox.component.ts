import {Component, inject, input, ChangeDetectionStrategy} from '@angular/core';
import {MatCheckboxChange, MatCheckbox} from '@angular/material/checkbox';
import {MatDialogActions} from '@angular/material/dialog';
import {UserService} from '@geoengine/common';

@Component({
    selector: 'geoengine-dialog-splash-checkbox',
    templateUrl: './dialog-splash-checkbox.component.html',
    styleUrls: ['./dialog-splash-checkbox.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatDialogActions, MatCheckbox],
})
export class DialogSplashCheckboxComponent {
    private readonly userService = inject(UserService);

    /**
     * The name is used as the key in LocalStorage.
     */
    readonly splashScreenName = input.required<string>();

    changeCheckbox(event: MatCheckboxChange): void {
        this.userService.saveSettingInLocalStorage(this.splashScreenName(), JSON.stringify(!event.checked));
    }
}
