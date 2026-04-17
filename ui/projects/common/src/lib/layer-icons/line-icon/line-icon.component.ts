import {Component, ChangeDetectionStrategy, input} from '@angular/core';
import {IconStyle} from '../../symbology/symbology.model';
import {BLACK, Color} from '../../colors/color';

/**
 * A simple interface to specify the style of a line icon.
 */
export interface LineIconStyle extends IconStyle {
    strokeWidth: number;
    // strokeDashStyle: StrokeDashStyle;
    strokeRGBA: Color;
}

/**
 * The line icon component
 */
@Component({
    selector: 'geoengine-line-icon',
    templateUrl: './line-icon.component.svg',
    styleUrls: ['./line-icon.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class LineIconComponent {
    // the style to use for the icon
    readonly iconStyle = input<LineIconStyle>({
        strokeWidth: 2,
        // strokeDashArray: Array<number> = [];
        strokeRGBA: BLACK,
    });
}
