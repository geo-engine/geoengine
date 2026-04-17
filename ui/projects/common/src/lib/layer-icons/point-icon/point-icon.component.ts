import {Component, ChangeDetectionStrategy, input} from '@angular/core';
import {IconStyle} from '../../symbology/symbology.model';
import {BLACK, Color, WHITE} from '../../colors/color';

/**
 * A simple interface to specify the style of a point icon
 */
export interface PointIconStyle extends IconStyle {
    strokeWidth: number;
    // strokeDashStyle: StrokeDashStyle;
    strokeRGBA: Color;
    fillRGBA: Color;
}

/**
 * The point icon component
 */
@Component({
    selector: 'geoengine-point-icon',
    templateUrl: './point-icon.component.svg',
    styleUrls: ['./point-icon.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PointIconComponent {
    // the style to use for the icon
    readonly iconStyle = input<PointIconStyle>({
        strokeWidth: 2,
        // strokeDashArray: Array<number> = [];
        strokeRGBA: BLACK,
        fillRGBA: WHITE,
    });
}
