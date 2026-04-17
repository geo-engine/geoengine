import {ErrorResponse} from '@geoengine/api-client';

/**
 * Error when getting an unexpected result type.
 */
export class UnexpectedResultType extends Error {
    constructor() {
        super('Unexpected Result Type');
    }
}

/**
 * Wrapper error type for server errors
 */
export class GeoEngineError extends Error {
    constructor(error: string, message: string) {
        super(message);
        this.name = error;
    }

    static fromDict(dict: ErrorResponse): GeoEngineError {
        return new GeoEngineError(dict.error, dict.message);
    }
}
