import {Pipe, PipeTransform} from '@angular/core';

@Pipe({name: 'geoengineHighlightPipe'})
export class HighlightPipe implements PipeTransform {
    // TODO: replace <span> with a <higlight> component?
    transform(text: string, term: string | null): string {
        let rexp;
        if (term) {
            try {
                rexp = new RegExp('(' + term + ')', 'gi');
            } catch (_e) {
                // TODO: what to do?
            }
            if (rexp) {
                text = text.replace(rexp, '<span class="highlight">$1</span>');
            }
        }
        return text;
    }
}
