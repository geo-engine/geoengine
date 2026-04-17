import * as Immutable from 'immutable';
import {ColorAttributeInputHinter} from '../colors/color-attribute-input/color-attribute-input.component';
import {ToDict} from '../time/time.model';
import {Measurement as MeasurementDict} from '@geoengine/api-client';

export abstract class Measurement implements ToDict<MeasurementDict> {
    abstract readonly measurement: string;

    static fromDict(dict: MeasurementDict): Measurement {
        if (dict.type === 'unitless') {
            return new UnitlessMeasurement();
        }

        if (dict.type === 'continuous') {
            return new ContinuousMeasurement(dict.measurement, dict.unit ?? undefined);
        }

        if (dict.type === 'classification') {
            return new ClassificationMeasurement(dict.measurement, dict.classes);
        }

        throw new Error('unable to deserialize `Measurement`');
    }

    abstract toDict(): MeasurementDict;
}

export class UnitlessMeasurement extends Measurement {
    readonly measurement = '';

    constructor() {
        super();
    }

    toDict(): MeasurementDict {
        return {
            type: 'unitless',
        };
    }
}

export class ContinuousMeasurement extends Measurement {
    readonly measurement: string;
    readonly unit?: string;

    constructor(measurement: string, unit?: string) {
        super();

        this.measurement = measurement;
        this.unit = unit;
    }

    toDict(): MeasurementDict {
        return {
            type: 'continuous',
            measurement: this.measurement,
            unit: this.unit,
        };
    }
}

export class ClassificationMeasurement extends Measurement implements ColorAttributeInputHinter {
    readonly measurement: string;
    readonly classes: Immutable.Map<number, string>;

    constructor(measurement: string, classes: Record<number, string>) {
        super();

        this.measurement = measurement;

        let classMap = Immutable.Map<number, string>();
        for (const classesKey of Object.keys(classes)) {
            const classesKeyNumber = parseInt(classesKey, 10);
            classMap = classMap.set(classesKeyNumber, classes[classesKeyNumber]);
        }

        this.classes = Immutable.Map(classMap);
    }

    toDict(): MeasurementDict {
        return {
            type: 'classification',
            measurement: this.measurement,
            classes: this.classes.toObject(),
        };
    }

    colorHint(key: string): string | undefined {
        const keyNumber = parseInt(key, 10);
        return this.classes.get(keyNumber);
    }
}
