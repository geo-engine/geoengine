import {UntypedFormControl} from '@angular/forms';
import {isValidUuid, validJson} from './form.validators';

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
});
