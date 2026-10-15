import {ChangeDetectionStrategy, Component, signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {form, FormField} from '@angular/forms/signals';
import {describe, expect, it} from 'vitest';
import {OperatorOutputNameComponent} from './operator-output-name.component';

@Component({
    selector: 'geoengine-test-host',
    template: `<geoengine-operator-output-name [formField]="nameForm.name" [suggestion]="suggestion()"></geoengine-operator-output-name>`,
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [OperatorOutputNameComponent, FormField],
})
class TestHostComponent {
    readonly suggestion = signal('Suggested name');
    readonly model = signal({name: 'Initial name'});
    readonly nameForm = form(this.model);
}

describe('OperatorOutputNameComponent', () => {
    it('can be bound to a signal form field and emits a changed suggestion', async () => {
        const fixture = TestBed.createComponent(TestHostComponent);
        await fixture.whenStable();

        expect(fixture.componentInstance.model().name).toBe('Initial name');

        fixture.componentInstance.suggestion.set('Other name');
        await fixture.whenStable();

        expect(fixture.componentInstance.model().name).toBe('Other name');
    });
});
