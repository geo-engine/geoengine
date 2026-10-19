import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {UntypedFormControl} from '@angular/forms';
import {form} from '@angular/forms/signals';
import {describe, expect, it} from 'vitest';
import {geoengineValidators, isValidUuid, validJson} from './form.validators';

describe('Form Validators', () => {
    it('validates uuids', () => {
        expect(isValidUuid(new UntypedFormControl('504ed8a4-e0a4-5cef-9f91-b2ffd4a2b56b'))).toBeNull();
        expect(isValidUuid(new UntypedFormControl('foo'))).toEqual({
            pattern: {
                requiredPattern: '/^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$/i',
                actualValue: 'foo',
            },
        });
    });

    it('validates JSON strings', () => {
        const validJsonControl = new UntypedFormControl('{"key": "value"}');
        const invalidJsonControl = new UntypedFormControl('{"key": "value"');
        const emptyJsonControl = new UntypedFormControl('');

        expect(validJson(validJsonControl)).toBeNull();
        expect(validJson(invalidJsonControl)).toEqual({invalidJson: true});
        expect(validJson(emptyJsonControl)).toBeNull();
    });

    it('validates that signal form strings are not only whitespace', () => {
        const model = signal({name: ''});
        const nameForm = TestBed.runInInjectionContext(() =>
            form(model, (schemaPath) => geoengineValidators.notOnlyWhitespace2(schemaPath.name)),
        );

        expect(nameForm.name().valid()).toBe(true);

        model.set({name: '   '});
        expect(
            nameForm
                .name()
                .errors()
                .map((error) => error.kind),
        ).toEqual(['onlyWhitespace']);

        model.set({name: ' foo '});
        expect(nameForm.name().valid()).toBe(true);
    });
});
