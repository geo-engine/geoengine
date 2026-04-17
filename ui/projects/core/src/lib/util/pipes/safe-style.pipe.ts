import {DomSanitizer, SafeHtml} from '@angular/platform-browser';
import {Pipe, PipeTransform, inject} from '@angular/core';

/**
 * This pipe is a workaround for to strict css sanitazion:
 * see: https://github.com/angular/angular/issues/8491
 */
@Pipe({name: 'geoengineSafeStyle'})
export class SafeStylePipe implements PipeTransform {
    private sanitizer = inject(DomSanitizer);

    transform(style: string): SafeHtml {
        return this.sanitizer.bypassSecurityTrustStyle(style);
    }
}
