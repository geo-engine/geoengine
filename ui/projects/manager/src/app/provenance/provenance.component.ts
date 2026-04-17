import {Component, OnChanges, SimpleChanges, input, output} from '@angular/core';
import {FormArray, FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {geoengineValidators} from '@geoengine/common';
import {Provenance} from '@geoengine/api-client';
import {MatDivider} from '@angular/material/list';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';

interface ProvenanceListForm {
    provenance: FormArray<FormGroup<ProvenanceForm>>;
}

interface ProvenanceForm {
    citation: FormControl<string>;
    license: FormControl<string>;
    uri: FormControl<string>;
}

export interface ProvenanceChange {
    provenance: Array<Provenance>;
    valid: boolean;
}

@Component({
    selector: 'geoengine-manager-provenance',
    templateUrl: './provenance.component.html',
    styleUrl: './provenance.component.scss',
    imports: [FormsModule, ReactiveFormsModule, MatDivider, MatFormField, MatLabel, MatInput, MatIconButton, MatIcon, MatButton],
})
export class ProvenanceComponent implements OnChanges {
    readonly provenance = input<Array<Provenance>>();

    readonly provenanceChange = output<ProvenanceChange>();

    form: FormGroup<ProvenanceListForm> = this.setUpForm();

    ngOnChanges(changes: SimpleChanges): void {
        if (changes.provenance) {
            this.form = this.setUpForm();
        }
    }

    addEntry(): void {
        this.form.controls.provenance.push(this.createProvenanceForm());
    }

    removeEntry(i: number): void {
        this.form.controls.provenance.removeAt(i);

        this.form.markAsDirty();
    }

    getProvenance(): Array<Provenance> {
        return this.form.controls.provenance.value.map((p) => ({
            citation: p.citation ?? '',
            license: p.license ?? '',
            uri: p.uri ?? '',
        }));
    }

    private setUpForm(): FormGroup<ProvenanceListForm> {
        const provenance = this.provenance();
        if (!provenance) {
            return new FormGroup<ProvenanceListForm>({
                provenance: new FormArray<FormGroup<ProvenanceForm>>([]),
            });
        }

        this.form.valueChanges.subscribe(() => {
            this.provenanceChange.emit({provenance: this.getProvenance(), valid: this.form.valid});
        });

        return new FormGroup<ProvenanceListForm>({
            provenance: new FormArray<FormGroup<ProvenanceForm>>(provenance.map((p) => this.createProvenanceForm(p))),
        });
    }

    private createProvenanceForm(p?: Provenance): FormGroup<ProvenanceForm> {
        return new FormGroup<ProvenanceForm>({
            citation: new FormControl(p?.citation ?? '', {
                nonNullable: true,
                validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
            }),
            license: new FormControl(p?.license ?? '', {
                nonNullable: true,
                validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
            }),
            uri: new FormControl(p?.uri ?? '', {
                nonNullable: true,
                validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
            }),
        });
    }
}
