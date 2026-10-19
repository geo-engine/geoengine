import {describe, expect, it} from 'vitest';
import moment from 'moment';
import {errorMessage, estimateTimeFormat, NON_BREAKING_HYPHEN} from './conversions';
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

describe('errorMessage', () => {
    it('uses string errors as they are', () => {
        expect(errorMessage('something failed')).toBe('something failed');
    });

    it('prefers the message of an error response body', () => {
        expect(errorMessage({error: {message: 'body message'}, message: 'outer message'})).toBe('body message');
    });

    it('uses the message of an error', () => {
        expect(errorMessage(new Error('error message'))).toBe('error message');
        expect(errorMessage({error: 'not an object', message: 'outer message'})).toBe('outer message');
    });

    it('falls back to the default message', () => {
        expect(errorMessage(undefined)).toBe('Unknown error');
        expect(errorMessage(42, 'fallback')).toBe('fallback');
        expect(errorMessage({message: 42}, 'fallback')).toBe('fallback');
    });
});
