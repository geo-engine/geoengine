import {Component, ChangeDetectionStrategy, input} from '@angular/core';
import {BLACK, Color, WHITE} from '../../colors/color';
import {IconStyle} from '../../symbology/symbology.model';

/**
 * A simple interface to specify the style of a polygon icon
 */
export interface PolygonIconStyle extends IconStyle {
    strokeWidth: number;
    // strokeDashStyle: StrokeDashStyle;
    strokeRGBA: Color;
    fillRGBA: Color;
}

/**
 * The polygon icon component
 */
@Component({
    selector: 'geoengine-polygon-icon',
    templateUrl: './polygon-icon.component.svg',
    styleUrls: ['./polygon-icon.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PolygonIconComponent {
    // the style to use for the icon
    readonly iconStyle = input<PolygonIconStyle>({
        strokeWidth: 2,
        // strokeDashArray: Array<number> = [];
        strokeRGBA: BLACK,
        fillRGBA: WHITE,
    });
}
