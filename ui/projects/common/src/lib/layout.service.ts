import {Injectable} from '@angular/core';

/**
 * A service that keeps track of app layouting options.
 */
@Injectable()
export class LayoutService {
    static readonly remInPx: number = parseFloat(getComputedStyle(document.documentElement).fontSize);
}
