import {Signal, WritableSignal, signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {afterEach, beforeEach, describe, expect, it, vi, type Mock} from 'vitest';
import {ProcessingGraphReadStatus} from '@geoengine/api-client';
import {UserService} from '@geoengine/common';
import {CoreConfig} from '../config.service';
import {ReadStatusService} from './read-status.service';

const GRAPH = '6b1e4f3a-2c5d-5e7f-8a9b-0c1d2e3f4a5b';

const rateLimited = (processingGraphId: string): ProcessingGraphReadStatus => ({
    processingGraphId,
    throttledReads: 1,
    throttledForSeconds: 4,
    httpStatus: 429,
    retryInSeconds: 1,
});

describe('ReadStatusService', () => {
    let readStatusHandler: Mock<() => Promise<Array<ProcessingGraphReadStatus>>>;
    let service: ReadStatusService;

    beforeEach(() => {
        vi.useFakeTimers();
        readStatusHandler = vi.fn(() => Promise.resolve([rateLimited(GRAPH)]));
        TestBed.configureTestingModule({
            providers: [
                ReadStatusService,
                {provide: UserService, useValue: {sessionApi: (): {readStatusHandler: typeof readStatusHandler} => ({readStatusHandler})}},
                {provide: CoreConfig, useValue: {DELAYS: {READ_STATUS: {AFTER_LOADING: 3000, POLL_INTERVAL: 1000}}}},
            ],
        });
        service = TestBed.inject(ReadStatusService);
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    /** Whether `throttled` says that the layer is rate limited, for a layer whose loading state is `loading`. */
    const watch = (loading: WritableSignal<boolean>, processingGraphId: Signal<string | undefined> = signal(GRAPH)): Signal<boolean> => {
        const throttled = TestBed.runInInjectionContext(() => service.throttled(loading, processingGraphId));
        TestBed.tick();
        return throttled;
    };

    /** Lets `ms` pass and runs the effects that hand signal changes to the poll. */
    const advance = async (ms: number): Promise<void> => {
        TestBed.tick();
        await vi.advanceTimersByTimeAsync(ms);
        TestBed.tick();
    };

    it('asks only after a layer has been loading for a while', async () => {
        const throttled = watch(signal(true));

        await advance(2999);
        expect(readStatusHandler).not.toHaveBeenCalled();
        expect(throttled()).toBe(false);

        // the poll starts right after the delay
        await advance(10);
        expect(readStatusHandler).toHaveBeenCalledTimes(1);
        expect(throttled()).toBe(true);
    });

    it('never asks for a layer that loads fast', async () => {
        const loading = signal(true);
        const throttled = watch(loading);

        await advance(1000);
        loading.set(false);
        await advance(10000);

        expect(readStatusHandler).not.toHaveBeenCalled();
        expect(throttled()).toBe(false);
    });

    it('stops polling and clears the hint when the layer is done', async () => {
        const loading = signal(true);
        const throttled = watch(loading);

        // polls at 3 s, 4 s and 5 s
        await advance(5010);
        expect(readStatusHandler).toHaveBeenCalledTimes(3);
        expect(throttled()).toBe(true);

        loading.set(false);
        await advance(10000);

        expect(readStatusHandler).toHaveBeenCalledTimes(3);
        expect(throttled()).toBe(false);
    });

    it('waits for the processing graph to be known', async () => {
        const processingGraphId = signal<string | undefined>(undefined);
        const throttled = watch(signal(true), processingGraphId);

        await advance(5000);
        expect(readStatusHandler).not.toHaveBeenCalled();

        processingGraphId.set(GRAPH);
        await advance(10);
        expect(throttled()).toBe(true);
    });

    it('ignores the rate limits of other processing graphs', async () => {
        readStatusHandler.mockResolvedValue([rateLimited('0f9e8d7c-6b5a-5f4e-9d3c-2b1a0f9e8d7c')]);
        const throttled = watch(signal(true));

        await advance(5000);

        expect(readStatusHandler).toHaveBeenCalled();
        expect(throttled()).toBe(false);
    });

    it('shares one poll between layers', async () => {
        watch(signal(true));
        watch(signal(true));

        await advance(3010);

        expect(readStatusHandler).toHaveBeenCalledTimes(1);
    });

    it('shows no hint if the request fails', async () => {
        readStatusHandler.mockRejectedValue(new Error('offline'));
        const throttled = watch(signal(true));

        await advance(5000);

        expect(throttled()).toBe(false);
    });
});
