import {describe, expect, it} from 'vitest';
import {Subject} from 'rxjs';
import {tileLoadingIndicator} from './main.component';

describe('tileLoadingIndicator', () => {
    it('shows the indicator immediately and hides it after the delay', async () => {
        const tileLoading$ = new Subject<boolean>();
        const values: Array<boolean> = [];
        const subscription = tileLoadingIndicator(tileLoading$).subscribe((loading) => values.push(loading));

        expect(values).toEqual([false]);

        tileLoading$.next(true);
        expect(values).toEqual([false, true]);

        tileLoading$.next(false);
        expect(values).toEqual([false, true]);

        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(values).toEqual([false, true, false]);

        subscription.unsubscribe();
    });
});
