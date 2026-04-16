import {Injectable} from '@angular/core';
import {Color, RgbTuple} from '../../colors/color';

@Injectable()
export class RandomColorService {
    protected static colorWheel: Array<RgbTuple> = [
        [166, 206, 227], // #a6cee3 / light blue
        [31, 120, 180], // #1f78b4 / blue
        [178, 223, 138], // #b2df8a / light green
        [51, 160, 44], // #33a02c / green
        [251, 154, 153], // #fb9a99 / light red
        [227, 26, 28], // #e31a1c / red
        [253, 191, 111], // #fdbf6f / light orange
        [255, 127, 0], // #ff7f00 / orange
        [202, 178, 214], // #cab2d6 / light purple
        [106, 61, 154], // #6a3d9a / purple
        [255, 255, 153], // #ffff99 / yellow-ish
        [177, 89, 40], // #b15928 / brown
    ];

    protected colorIndex = Math.trunc(Math.random() * RandomColorService.colorWheel.length);

    getRandomColorRgba(alpha = 0.8): Color {
        const color = RandomColorService.colorWheel[this.colorIndex];
        this.colorIndex = (this.colorIndex + 1) % RandomColorService.colorWheel.length;
        return Color.fromRgbaLike([color[0], color[1], color[2], alpha]);
    }
}
