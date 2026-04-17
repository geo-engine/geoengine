import moment from 'moment';
import {estimateTimeFormat, NON_BREAKING_HYPHEN} from './conversions';
import {Time} from '../time/time.model';

describe('Conversions', () => {
    it('estimates time formats correctly', () => {
        expect(estimateTimeFormat([new Time(moment.utc('2000-01-01T00:00:00Z'))])).toEqual('YYYY');
        expect(
            estimateTimeFormat([
                new Time(moment.utc('2000-01-01T00:00:00Z')),
                new Time(moment.utc('2001-01-01T00:00:00Z')),
                new Time(moment.utc('2002-01-01T00:00:00Z')),
            ]),
        ).toEqual('YYYY');
        expect(
            estimateTimeFormat([
                new Time(moment.utc('2000-01-01T00:00:00Z')),
                new Time(moment.utc('2001-01-01T00:00:00Z')),
                new Time(moment.utc('2002-02-01T00:00:00Z')),
            ]),
        ).toEqual('YYYY-MM'.replaceAll('-', NON_BREAKING_HYPHEN));
        expect(
            estimateTimeFormat([
                new Time(moment.utc('2000-01-01T00:00:00Z')),
                new Time(moment.utc('2000-01-02T00:00:00Z')),
                new Time(moment.utc('2000-01-03T00:00:00Z')),
            ]),
        ).toEqual('YYYY-MM-DD'.replaceAll('-', NON_BREAKING_HYPHEN));
    });
});
