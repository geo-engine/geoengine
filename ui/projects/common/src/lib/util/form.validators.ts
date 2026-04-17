import {Observable, Observer} from 'rxjs';
import {map} from 'rxjs/operators';
import {AbstractControl, AsyncValidatorFn, FormArray, FormGroup, ValidationErrors, ValidatorFn, Validators} from '@angular/forms';
import {Moment} from 'moment';
import {PathKind, SchemaPath, SchemaPathRules, validate} from '@angular/forms/signals';

const isFiniteNumber = (value: null | undefined | number): boolean =>
    value !== null && value !== undefined && !isNaN(value) && isFinite(value);

/**
 * A validator that validates a form group that contains min/max number fields.
 */
const minAndMax = (
    controlMinName: string,
    controlMaxName: string,
    options: {
        checkBothExist?: boolean;
        checkOneExists?: boolean;
        mustNotEqual?: boolean;
    } = {},
): ((_: AbstractControl) => ValidationErrors | null) => {
    if (typeof options.checkBothExist !== 'boolean') {
        // default
        options.checkBothExist = false;
    }
    if (typeof options.checkOneExists !== 'boolean') {
        // default
        options.checkOneExists = true;
    }

    return (control: AbstractControl): ValidationErrors | null => {
        const min = control.get(controlMinName)?.value;
        const max = control.get(controlMaxName)?.value;

        const errors: {
            minOverMax?: boolean;
            minEqualsMax?: boolean;
            noFilter?: boolean;
            noFiniteNumber?: boolean;
        } = {};

        const validMin = isFiniteNumber(min);
        const validMax = isFiniteNumber(max);

        if (validMin && validMax && max < min) {
            errors.minOverMax = true;
        }

        if (options.mustNotEqual && min === max) {
            errors.minEqualsMax = true;
        }

        if (options.checkOneExists && !validMin && !validMax) {
            errors.noFilter = true;
        }

        if (options.checkBothExist && (!validMin || !validMax)) {
            errors.noFilter = true;
        }

        if ((!validMin && min !== undefined && min !== null) || (!validMax && max !== undefined && max !== null)) {
            errors.noFiniteNumber = true;
        }

        return Object.keys(errors).length > 0 ? errors : null;
    };
};

/**
 * A validator that validates a form group that contains min/max number or string fields.
 */
const minAndMaxNumOrStr = (
    controlMinName: string,
    controlMaxName: string,
    options: {
        checkBothExist?: boolean;
        checkIsNumber?: boolean;
    } = {},
): ((_: AbstractControl) => ValidationErrors | null) => {
    if (typeof options.checkBothExist !== 'boolean') {
        // default
        options.checkBothExist = false;
    }
    if (typeof options.checkIsNumber !== 'boolean') {
        // default
        options.checkIsNumber = false;
    }

    return (control: AbstractControl): ValidationErrors | null => {
        let min = control.get(controlMinName)?.value;
        let max = control.get(controlMaxName)?.value;

        const errors: {
            minOverMax?: boolean;
            minOverMaxStr?: boolean;
            minEqualsMax?: boolean;
            noFilter?: boolean;
            noFiniteNumber?: boolean;
        } = {};
        if (options.checkBothExist && (min === '' || max === '')) {
            errors.noFilter = true;
        }

        if (options.checkIsNumber) {
            min = Number(min);
            max = Number(max);

            const validMin = isFiniteNumber(min);
            const validMax = isFiniteNumber(max);

            if ((!validMin && min !== undefined && min !== null) || (!validMax && max !== undefined && max !== null)) {
                errors.noFiniteNumber = true;
            }
            if (max < min) {
                errors.minOverMax = true;
            }
        } else {
            if (max < min) {
                errors.minOverMaxStr = true;
            }
        }

        return Object.keys(errors).length > 0 ? errors : null;
    };
};

/**
 * A validator that invokes the underlying one only if the condition holds.
 */
const conditionalValidator =
    (validator: (control: AbstractControl) => ValidationErrors | null, condition: () => boolean) =>
    (control: AbstractControl): Record<string, boolean> | null => {
        if (condition()) {
            return validator(control);
        } else {
            return null;
        }
    };

/**
 * A validator that invokes the underlying one only if the condition holds.
 */
const conditionalAsyncValidator =
    (validator: AsyncValidatorFn, condition: () => Promise<boolean>): AsyncValidatorFn =>
    async (control: AbstractControl): Promise<ValidationErrors | null> => {
        if (!(await condition())) {
            return null;
        }

        return validator(control);
    };

/**
 * Checks if keyword is a reserved keyword.
 */
const keywordValidator =
    (keywords: Array<string>) =>
    (control: AbstractControl): {keyword: true} | null =>
        keywords.includes(control.value) ? {keyword: true} : null;

function keywordSignalValidator<TPathKind extends PathKind = PathKind.Root>(
    path: SchemaPath<string, SchemaPathRules.Supported, TPathKind>,
    keywords: Array<string>,
): void {
    validate(path, ({value}) => {
        if (keywords.includes(value())) {
            return {
                kind: 'keyword',
                message: `${value()} is a reserved keyword.`,
            };
        }
        return null;
    });
}

/**
 * Checks if the project name is unique.
 */
const uniqueProjectNameValidator =
    (storageService: {projectExists(_: string): Observable<boolean>}): AsyncValidatorFn =>
    (control: AbstractControl): Observable<ValidationErrors | null> =>
        new Observable((observer: Observer<ValidationErrors | null>) => {
            storageService
                .projectExists(control.value as string)
                .pipe(
                    map((projectExists) => {
                        const errors: {
                            nameInUsage?: boolean;
                        } = {};

                        if (projectExists) {
                            errors.nameInUsage = true;
                        }

                        return Object.keys(errors).length > 0 ? errors : null;
                    }),
                )
                .subscribe(
                    (errors) => {
                        observer.next(errors);
                        observer.complete();
                    },
                    (error) => {
                        observer.error(error);
                    },
                );
        });

const notOnlyWhitespace = (control: AbstractControl): {onlyWhitespace: true} | null => {
    const text = control.value as string;
    if (!text) {
        return null;
    }
    return text.trim().length <= 0 ? {onlyWhitespace: true} : null;
};

const isNumber = (control: AbstractControl): {isNoNumber: true} | null => {
    const value = control.value;
    return isFiniteNumber(value) ? null : {isNoNumber: true};
};

/**
 * checks if a value is undefined or null
 */
const nullOrUndefined = (value: unknown): boolean => value === undefined || value === null;

/**
 * This Validator checks the relation of a value compared to another value.
 *
 * @param controlValueProvider: function deriving the value of the control
 * @param compareValueProvider: function deriving the min value
 * @param options: {checkEqual: true} enables checking for equal, {checkAbove: true} for above and {checkBelow: true] for below.
 */
// eslint-disable-next-line prefer-arrow/prefer-arrow-functions
export function valueRelation(
    controlValueProvider: (control: AbstractControl) => number,
    compareValueProvider: (control: AbstractControl) => number,
    options: {
        checkEqual?: boolean;
        checkAbove?: boolean;
        checkBelow?: boolean;
    } = {
        checkEqual: true,
        checkAbove: true,
        checkBelow: true,
    },
): (_: AbstractControl) => ValidationErrors | null {
    return (control: AbstractControl): ValidationErrors | null => {
        const value = controlValueProvider(control);
        const compareValue = compareValueProvider(control);

        const errors: {
            valueAbove?: boolean;
            valueAboveOrEqual?: boolean;
            valueBelow?: boolean;
            valueBelowOrEqual?: boolean;
            valueEquals?: boolean;
            noFilter?: boolean;
        } = {};

        if (options.checkEqual && !nullOrUndefined(value) && !nullOrUndefined(compareValue) && value === compareValue) {
            errors.valueEquals = true;
            if (options.checkBelow) {
                errors.valueBelowOrEqual = true;
            }
            if (options.checkAbove) {
                errors.valueAboveOrEqual = true;
            }
        }

        if (options.checkBelow && !nullOrUndefined(value) && !nullOrUndefined(compareValue) && value < compareValue) {
            errors.valueBelow = true;
            errors.valueBelowOrEqual = true;
        }
        if (options.checkAbove && !nullOrUndefined(value) && !nullOrUndefined(compareValue) && value > compareValue) {
            errors.valueAbove = true;
            errors.valueAboveOrEqual = true;
        }
        if (nullOrUndefined(value)) {
            errors.noFilter = true;
        }
        return Object.keys(errors).length > 0 ? errors : null;
    };
}

export const isValidUuid = Validators.pattern(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$/i);

const validRasterMetadataKey = Validators.pattern(/^([a-zA-Z0-9]+\.)?[a-zA-Z0-9]+$/);

/**
 * A vaidator that checks if at least on of the provided validators is valid.
 */
const oneOrBoth =
    (first: ValidatorFn, second: ValidatorFn) =>
    (control: AbstractControl): ValidationErrors | null => {
        const firstResult = first(control);
        const secondResult = second(control);
        if (firstResult && secondResult) {
            return {...firstResult, ...secondResult, noneOfBoth: true};
        }
        return null;
    };

/**
 * A validator that checks if values are larger than the given value.
 */
const largerThan =
    (lowerBound: number): ((_: AbstractControl) => ValidationErrors | null) =>
    (control: AbstractControl): ValidationErrors | null => {
        const value = control.value;

        const errors: {
            valueNotLarger?: boolean;
        } = {};

        if (isFiniteNumber(value) && value <= lowerBound) {
            errors.valueNotLarger = true;
        }

        return Object.keys(errors).length > 0 ? errors : null;
    };

/**
 * A validator that checks if values are in the given value range.
 */
const inRange =
    (
        lowerBound: number,
        upperBound: number,
        lowerInclusive: boolean,
        upperInclusive: boolean,
    ): ((_: AbstractControl) => ValidationErrors | null) =>
    (control: AbstractControl): ValidationErrors | null => {
        const value = control.value;

        const errors: {
            valueNotInRange?: boolean;
        } = {};

        if (
            isFiniteNumber(value) &&
            (value < lowerBound ||
                (value === lowerBound && !lowerInclusive) ||
                value > upperBound ||
                (value === upperBound && !upperInclusive))
        ) {
            errors.valueNotInRange = true;
        }

        return Object.keys(errors).length > 0 ? errors : null;
    };

/**
 * A validator that checks that a value is not zero.
 */
const notZero = (control: AbstractControl): ValidationErrors | null => {
    const value = control.value;

    const errors: {
        valueIsZero?: boolean;
    } = {};

    if (value === 0) {
        errors.valueIsZero = true;
    }

    return Object.keys(errors).length > 0 ? errors : null;
};

/**
 * A validator that checks that the start value is before the end value.
 *
 * The field must be a `FormGroup` with the fields `start` and `end` of type `Moment`.
 */
const startBeforeEndValidator = (control: AbstractControl): ValidationErrors | null => {
    if (!(control instanceof FormGroup)) {
        throw Error('The field must be a FormGroup with the fields `start` and `end` of type Moment and `timeAsPoint` of type boolean.');
    }
    if (!control.controls.start || !control.controls.end) {
        throw Error('The field must be a FormGroup with the fields `start` and `end` of type Moment and `timeAsPoint` of type boolean.');
    }

    const start = control.controls.start.value as Moment;
    const end = control.controls.end.value as Moment;
    const timeAsPoint = control.controls.timeAsPoint.value as boolean;

    if (timeAsPoint || start.isBefore(end)) {
        return null;
    } else {
        return {valid: false};
    }
};

export const duplicateInFormArrayValidator =
    (): ValidatorFn =>
    (control: AbstractControl): ValidationErrors | null => {
        if (!(control instanceof FormArray)) {
            return null;
        }

        const formArray = control;

        const controls = formArray.controls;
        const values = controls.map((c) => c.value);

        const duplicates = values.some((value, index) => values.indexOf(value) !== index);

        return duplicates ? {duplicate: true} : null;
    };

const duplicateValidator =
    (): ValidatorFn =>
    (control: AbstractControl): ValidationErrors | null => {
        const values = control.value as string[];

        if (!values) {
            return null;
        }

        const duplicates = values.some((value, index) => values.indexOf(value) !== index);

        return duplicates ? {duplicate: true} : null;
    };

/**
 * A validator that checks if a value is valid JSON.
 */
export const validJson: ValidatorFn = (control: AbstractControl): ValidationErrors | null => {
    if (control.value === null || control.value === undefined || control.value === '') {
        return null; // Don't validate empty values, use Validators.required if needed
    }

    try {
        JSON.parse(control.value);
        return null; // Valid JSON
    } catch (_e) {
        return {invalidJson: true}; // Invalid JSON
    }
};

export const geoengineValidators = {
    conditionalValidator,
    conditionalAsyncValidator,
    isNumber,
    keyword: keywordValidator,
    keyword2: keywordSignalValidator,
    minAndMax,
    notOnlyWhitespace,
    uniqueProjectName: uniqueProjectNameValidator,
    largerThan,
    inRange,
    validRasterMetadataKey,
    oneOrBoth,
    minAndMaxNumOrStr,
    notZero,
    startBeforeEndValidator,
    duplicateInFormArrayValidator,
    duplicateValidator,
    validJson,
};
