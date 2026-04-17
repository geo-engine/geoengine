import {HttpErrorResponse} from '@angular/common/http';

/**
 * This class represents a geo engine user.
 */
export class User {
    id: string;

    realName?: string;
    email?: string;

    isGuest: boolean;

    constructor(config: {id: string; realName?: string; email?: string}) {
        this.id = config.id;
        this.realName = config.realName;
        this.email = config.email;

        this.isGuest = !config.email || !config.realName;
    }
}

/**
 * This interface represents the status of the used geo engine backend.
 */
export interface BackendStatus {
    available: boolean;
    httpError?: HttpErrorResponse;
    initial?: boolean;
}
