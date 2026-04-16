import {DomSanitizer, SafeHtml} from '@angular/platform-browser';
import {Pipe, PipeTransform, inject} from '@angular/core';

/**
 * This pipe is a workaround for to strict html sanitazion:
 * see: https://github.com/angular/angular/issues/8491
 */
@Pipe({name: 'geoengineSafeHtml'})
export class SafeHtmlPipe implements PipeTransform {
    private sanitizer = inject(DomSanitizer);

    transform(value: string): SafeHtml {
        return this.sanitizer.bypassSecurityTrustHtml(value);
    }
}
