import {HttpErrorResponse} from '@angular/common/http';
import {Quota as QuotaDict} from '@geoengine/api-client';

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

export class Quota {
    protected _used: number;
    protected _available: number;

    constructor(used: number, available: number) {
        this._used = used;
        this._available = available;
    }

    get used(): number {
        return this._used;
    }

    get available(): number {
        return this._available;
    }

    static fromDict(dict: QuotaDict): Quota {
        return new Quota(dict.used, dict.available);
    }
}
