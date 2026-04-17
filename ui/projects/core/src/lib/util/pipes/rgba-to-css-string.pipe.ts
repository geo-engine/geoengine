import {Pipe, PipeTransform} from '@angular/core';

@Pipe({name: 'rgbaToCssStringPipe'})
export class RgbaToCssStringPipe implements PipeTransform {
    transform(rgba: Array<number>): string {
        return 'rgba(' + rgba[0] + ',' + rgba[1] + ',' + rgba[2] + ',' + rgba[3] + ')';
    }
}
