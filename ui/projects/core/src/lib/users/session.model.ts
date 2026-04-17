import {Moment} from 'moment';
import {STRectangleDict, UUID} from '../backend/backend.model';
import {User} from './user.model';
import {Configuration} from '@geoengine/api-client';

/**
 * A user session after login
 */
export interface Session {
    sessionToken: string;
    user?: User; // TODO: split for pro
    apiConfiguration: Configuration;
    validUntil: Moment; // TODO: custom time point?
    lastProjectId?: UUID;
    lastView?: STRectangleDict;
}
