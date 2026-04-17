import {AbstractControl, ValidationErrors, ValidatorFn} from '@angular/forms';

export const priorityValidator =
    (): ValidatorFn =>
    (control: AbstractControl): ValidationErrors | null => {
        const value = Number(control.value);

        if (!Number.isInteger(value)) {
            return {notInteger: true};
        }

        if (value < -32768 || value > 32767) {
            return {outOfRange: true};
        }

        return null;
    };
