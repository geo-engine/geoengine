/**
 * Ensures asynchronous tasks execute strictly one after another in sequence.
 */
export class AsyncSequencer {
    private queue: Promise<unknown> = Promise.resolve();

    /**
     * Enqueues an async operation to run sequentially after previous operations finish.
     * Returns a promise that resolves with the result of the passed operation.
     */
    enqueue<T>(task: () => Promise<T>): Promise<T> {
        const result = this.queue.then(() => task());

        // Catch errors on the internal queue tracker so a failure in task A
        // doesn't break execution of task B.
        this.queue = result.catch(() => ({}));

        return result;
    }
}
