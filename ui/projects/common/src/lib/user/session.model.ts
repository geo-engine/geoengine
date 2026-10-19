import {Moment} from 'moment';
import {Configuration, STRectangle} from '@geoengine/api-client';
import {User} from './user.model';
import {UUID} from '../datasets/dataset.model';

/**
 * A user session after login
 */
export interface Session {
    sessionToken: string;
    user?: User; // TODO: split for pro
    apiConfiguration: Configuration;
    validUntil: Moment; // TODO: custom time point?
    lastProjectId?: UUID;
    lastView?: STRectangle;
}
