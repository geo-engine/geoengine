import {describe, expect, it} from 'vitest';
import {AsyncSequencer} from './sequencer';

describe('AsyncSequencer', () => {
    it('runs queued tasks in sequence', async () => {
        const sequencer = new AsyncSequencer();
        const events: string[] = [];

        const first = sequencer.enqueue(async () => {
            events.push('first:start');
            await new Promise((resolve) => setTimeout(resolve, 20));
            events.push('first:end');
            return 'first';
        });

        // eslint-disable-next-line @typescript-eslint/require-await
        const second = sequencer.enqueue(async () => {
            events.push('second:start');
            return 'second';
        });

        await expect(first).resolves.toBe('first');
        await expect(second).resolves.toBe('second');
        expect(events).toEqual(['first:start', 'first:end', 'second:start']);
    });
});
