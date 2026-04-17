import {Component, Input, output} from '@angular/core';
import {FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ClassificationMeasurement, ContinuousMeasurement, Measurement, UnitlessMeasurement} from '@geoengine/api-client';
import {MatButtonToggleGroup, MatButtonToggle} from '@angular/material/button-toggle';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {KeyValuePipe} from '@angular/common';

enum MeasurementType {
    Classification = 'classification',
    Continuous = 'continuous',
    Unitless = 'unitless',
}

interface AddClassForm {
    key: FormControl<string>;
    value: FormControl<string>;
}

@Component({
    selector: 'geoengine-measurement',
    templateUrl: './measurement.component.html',
    styleUrl: './measurement.component.css',
    imports: [
        MatButtonToggleGroup,
        FormsModule,
        MatButtonToggle,
        MatFormField,
        MatLabel,
        MatInput,
        MatIconButton,
        MatIcon,
        ReactiveFormsModule,
        KeyValuePipe,
    ],
})
export class MeasurementComponent {
    @Input() measurement!: Measurement;

    readonly measurementChange = output<Measurement>();

    MeasurementType = MeasurementType;

    classificationMeasurement?: ClassificationMeasurement;
    continousMeasurement?: ContinuousMeasurement;
    unitlessMeasurement?: UnitlessMeasurement;

    addClassForm: FormGroup<AddClassForm> = new FormGroup<AddClassForm>({
        key: new FormControl('', {
            nonNullable: true,
            validators: [Validators.required, Validators.minLength(1)],
        }),
        value: new FormControl('', {
            nonNullable: true,
            validators: [Validators.required, Validators.minLength(1)],
        }),
    });

    constructor() {
        if (!this.measurement) {
            this.measurement = {
                type: 'unitless',
            };
        }
    }

    getMeasurementType(): MeasurementType {
        switch (this.measurement.type) {
            case 'classification':
                return MeasurementType.Classification;
            case 'continuous':
                return MeasurementType.Continuous;
            case 'unitless':
                return MeasurementType.Unitless;
        }
    }

    updateMeasurementType(type: MeasurementType): void {
        switch (type) {
            case MeasurementType.Classification:
                this.classificationMeasurement ??= {
                    type: 'classification',
                    measurement: 'classification',
                    classes: {},
                };

                this.measurement = this.classificationMeasurement;
                break;
            case MeasurementType.Continuous:
                this.continousMeasurement ??= {
                    type: 'continuous',
                    measurement: 'continuous',
                };

                this.measurement = this.continousMeasurement;
                break;
            case MeasurementType.Unitless:
                this.unitlessMeasurement ??= {
                    type: 'unitless',
                };

                this.measurement = {
                    type: 'unitless',
                };
                break;
        }
    }

    removeClass(key: string): void {
        if (!this.classificationMeasurement) {
            return;
        }

        delete this.classificationMeasurement.classes[key];
    }

    addClass(): void {
        if (!this.classificationMeasurement) {
            return;
        }

        const key = this.addClassForm.controls.key.value;
        const value = this.addClassForm.controls.value.value;

        this.classificationMeasurement.classes[key] = value;

        this.addClassForm.reset();
        this.addClassForm.markAsPristine();
    }
}
