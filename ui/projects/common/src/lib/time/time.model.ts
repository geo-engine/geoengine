import {DurationInputArg1, DurationInputArg2, Moment, MomentFormatSpecification, MomentInput, utc} from 'moment';
import {
    TimeStep as TimeStepDict,
    TimeGranularity as TimeStepGranularityDict,
    TimeInterval as TimeIntervalDict,
} from '@geoengine/api-client';

export type TimeType = 'TimePoint' | 'TimeInterval';

export interface ToDict<T> {
    toDict(): T;
}

type TimeStepDurationType =
    | 'millisecond'
    | 'milliseconds'
    | 'second'
    | 'seconds'
    | 'minute'
    | 'minutes'
    | 'hour'
    | 'hours'
    | 'day'
    | 'days'
    | 'month'
    | 'months'
    | 'year'
    | 'years';

export interface TimeStepDuration {
    durationAmount: number;
    durationUnit: TimeStepDurationType;
}

export const timeStepDurationToTimeStepDict = (duration: TimeStepDuration): TimeStepDict => {
    const mapGranularity = (durationUnit: TimeStepDurationType): TimeStepGranularityDict => {
        switch (durationUnit) {
            case 'millisecond':
            case 'milliseconds':
                return 'millis';
            case 'second':
            case 'seconds':
                return 'seconds';
            case 'minute':
            case 'minutes':
                return 'minutes';
            case 'hour':
            case 'hours':
                return 'hours';
            case 'day':
            case 'days':
                return 'days';
            case 'month':
            case 'months':
                return 'months';
            case 'year':
            case 'years':
                return 'years';
        }
    };

    return {
        step: duration.durationAmount,
        granularity: mapGranularity(duration.durationUnit),
    };
};

export const timeStepDurationToDurationInputArg2 = (durationUnit: TimeStepDurationType): DurationInputArg2 => {
    switch (durationUnit) {
        case 'millisecond':
        case 'milliseconds':
            return 'millisecond';
        case 'second':
        case 'seconds':
            return 'second';
        case 'minute':
        case 'minutes':
            return 'minute';
        case 'hour':
        case 'hours':
            return 'hour';
        case 'day':
        case 'days':
            return 'day';
        case 'month':
        case 'months':
            return 'month';
        case 'year':
        case 'years':
            return 'year';
    }
};

export const timeStepDictTotimeStepDuration = (timeStepDict: TimeStepDict): TimeStepDuration => {
    const mapGranularity = (granularity: TimeStepGranularityDict, plural: boolean): TimeStepDurationType => {
        switch (granularity) {
            case 'millis':
                return plural ? 'milliseconds' : 'millisecond';
            case 'seconds':
                return plural ? 'seconds' : 'second';
            case 'minutes':
                return plural ? 'minutes' : 'minute';
            case 'hours':
                return plural ? 'hours' : 'hour';
            case 'days':
                return plural ? 'days' : 'day';
            case 'months':
                return plural ? 'months' : 'month';
            case 'years':
                return plural ? 'years' : 'year';
        }
    };

    return {
        durationAmount: timeStepDict.step,
        durationUnit: mapGranularity(timeStepDict.granularity, timeStepDict.step > 1),
    };
};

const MIN_TIME_MOMENT = utc(-8_334_632_851_200_001 + 1, true);
const MAX_TIME_MOMENT = utc(8_210_298_412_800_000 - 1, true);
const DEFAULT_TIMEFORMAT = 'YYYY-MM-DDTHH:mm:ssZ';

export const timeStepGranularityOptions: Array<TimeStepGranularityDict> = [
    'millis',
    'seconds',
    'minutes',
    'hours',
    'days',
    'months',
    'years',
];

export class Time implements ToDict<TimeIntervalDict> {
    readonly start: Moment;
    readonly end: Moment;

    constructor(start: MomentInput, end?: MomentInput, format?: MomentFormatSpecification) {
        this.start = utc(start, format, true);
        if (end) {
            this.end = utc(end, format, true);
        } else {
            this.end = this.start.clone();
        }
    }

    static fromDict(dict: TimeIntervalDict): Time {
        return new Time(dict.start, dict.end);
    }

    toDict(): TimeIntervalDict {
        return {
            start: this.start.valueOf(),
            end: this.end.valueOf(),
        };
    }

    get type(): TimeType {
        if (this.start.isSame(this.end)) {
            return 'TimePoint';
        } else {
            return 'TimeInterval';
        }
    }

    add(durationAmount: DurationInputArg1, durationUnit?: DurationInputArg2): Time {
        return new Time(this.start.clone().add(durationAmount, durationUnit), this.end.clone().add(durationAmount, durationUnit));
    }

    addDuration(timeStepDuration: TimeStepDuration): Time {
        return this.add(timeStepDuration.durationAmount, timeStepDurationToDurationInputArg2(timeStepDuration.durationUnit));
    }

    subtract(durationAmount: DurationInputArg1, durationUnit?: DurationInputArg2): Time {
        return new Time(this.start.clone().subtract(durationAmount, durationUnit), this.end.clone().subtract(durationAmount, durationUnit));
    }

    clone(): Time {
        return new Time(this.start.clone(), this.end.clone());
    }

    isSame(other: Time): boolean {
        return !!other && this.type === other.type && this.start.isSame(other.start) && this.end.isSame(other.end);
    }

    isValid(): boolean {
        return !!this.start && !!this.end && this.start.isValid() && this.end.isValid() && this.start <= this.end;
    }

    /**
     * Checks if `this` value is before the `other` value on both `start` and `end` values.
     */
    isBefore(other: Time): boolean {
        return this.start < other.start && this.end < other.end;
    }

    isStartMin(): boolean {
        return this.start.isSame(MIN_TIME_MOMENT);
    }

    isEndMax(): boolean {
        return this.end.isSame(MAX_TIME_MOMENT);
    }

    asRequestString(): string {
        switch (this.type) {
            case 'TimePoint':
                return this.start.toISOString();
            case 'TimeInterval':
                return `${this.start.toISOString()}/${this.end.toISOString()}`;
        }
    }

    toString(format = DEFAULT_TIMEFORMAT): string {
        switch (this.type) {
            case 'TimePoint':
                return this.startString(format);
            case 'TimeInterval': {
                const start = this.startString(format);
                const end = this.endString(format);
                return `${start} - ${end}`;
            }
        }
    }

    startString(format = DEFAULT_TIMEFORMAT): string {
        return this.start.format(format);
    }

    endString(format = DEFAULT_TIMEFORMAT): string {
        return this.end.format(format);
    }

    public startStringOrNegInf(format = DEFAULT_TIMEFORMAT): string {
        if (this.isStartMin()) {
            return '-∞';
        }
        return this.startString(format);
    }

    public endStringOrPosInf(format = DEFAULT_TIMEFORMAT): string {
        if (this.isEndMax()) {
            return '∞';
        }
        return this.endString(format);
    }

    timeBounds(): Time {
        return new Time(MIN_TIME_MOMENT, MAX_TIME_MOMENT);
    }
}
