import {Component, ChangeDetectionStrategy, OnChanges, SimpleChanges, OnInit, inject, input, output} from '@angular/core';
import {
    ClusteredPointSymbology,
    ColorParam,
    LineSymbology,
    NumberParam,
    PointSymbology,
    PolygonSymbology,
    StaticColor,
    StaticNumber,
    Stroke,
    SymbologyType,
    SymbologyWorkflow,
    TextSymbology,
    VectorSymbology,
} from '../symbology.model';
import {ReplaySubject} from 'rxjs';
import {BLACK, WHITE} from '../../colors/color';
import {FeatureDataType} from '@geoengine/api-client';
import {WorkflowsService} from '../../workflows/workflows.service';
import {MatCard, MatCardHeader, MatCardAvatar, MatCardTitle, MatCardContent} from '@angular/material/card';
import {MatIcon} from '@angular/material/icon';
import {MatSlideToggle} from '@angular/material/slide-toggle';
import {ColorParamEditorComponent} from '../color-param-editor/color-param-editor.component';
import {FormsModule} from '@angular/forms';
import {NumberParamEditorComponent} from '../number-param-editor/number-param-editor.component';
import {MatFormField, MatLabel} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {AsyncPipe} from '@angular/common';
import {AsyncValueDefault} from '../../util/pipes/async-converters.pipe';

/**
 * An editor for generating raster symbologies.
 */
@Component({
    selector: 'geoengine-vector-symbology-editor',
    templateUrl: 'vector-symbology-editor.component.html',
    styleUrls: ['vector-symbology-editor.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatCard,
        MatCardHeader,
        MatCardAvatar,
        MatIcon,
        MatCardTitle,
        MatCardContent,
        MatSlideToggle,
        ColorParamEditorComponent,
        FormsModule,
        NumberParamEditorComponent,
        MatFormField,
        MatLabel,
        MatSelect,
        MatOption,
        AsyncPipe,
        AsyncValueDefault,
    ],
})
export class VectorSymbologyEditorComponent implements OnChanges, OnInit {
    private readonly workflowsService = inject(WorkflowsService);

    readonly symbologyWorkflow = input.required<SymbologyWorkflow<VectorSymbology>>();

    readonly changedSymbology = output<VectorSymbology>();

    currentSymbology!: VectorSymbology;

    showFillColorEditor = false;
    showRadiusEditor = false;

    numericAttributes = new ReplaySubject<Array<string>>(1);
    allAttributes = new ReplaySubject<Array<string>>(1);

    ngOnChanges(changes: SimpleChanges): void {
        const symbologyWorkflow = this.symbologyWorkflow();
        if (changes.symbologyWorkflow && symbologyWorkflow) {
            this.currentSymbology = symbologyWorkflow.symbology.clone();
            this.showFillColorEditor =
                this.currentSymbology instanceof PointSymbology ||
                this.currentSymbology instanceof ClusteredPointSymbology ||
                this.currentSymbology instanceof PolygonSymbology;
            this.showRadiusEditor = this.currentSymbology instanceof PointSymbology;
            this.initializeAttributes();
        }
    }

    ngOnInit(): void {
        this.currentSymbology = this.symbologyWorkflow().symbology.clone();
        this.showFillColorEditor =
            this.currentSymbology instanceof PointSymbology ||
            this.currentSymbology instanceof ClusteredPointSymbology ||
            this.currentSymbology instanceof PolygonSymbology;
        this.showRadiusEditor = this.currentSymbology instanceof PointSymbology;
        this.initializeAttributes();
    }

    get isPointLayer(): boolean {
        return this.currentSymbology.symbologyType === SymbologyType.POINT;
    }

    get isClustered(): boolean {
        return this.currentSymbology instanceof ClusteredPointSymbology;
    }

    get isLineLayer(): boolean {
        return this.currentSymbology.symbologyType === SymbologyType.LINE;
    }

    get isPolygonLayer(): boolean {
        return this.currentSymbology.symbologyType === SymbologyType.POLYGON;
    }

    setClusterSymbology(clustered: boolean): void {
        if (clustered && this.currentSymbology instanceof PointSymbology) {
            this.currentSymbology = new ClusteredPointSymbology(this.currentSymbology.fillColor, this.currentSymbology.stroke);
            this.showRadiusEditor = false;
            this.updateClusteredPointSymbology({});
        } else if (!clustered && this.currentSymbology instanceof ClusteredPointSymbology) {
            this.currentSymbology = new PointSymbology(
                new StaticNumber(PointSymbology.DEFAULT_POINT_RADIUS),
                this.currentSymbology.fillColor,
                this.currentSymbology.stroke,
            );
            this.showRadiusEditor = true;
            this.updatePointSymbology({});
        } else {
            // should not be called
        }
    }

    get fillColor(): ColorParam {
        if (
            this.currentSymbology instanceof PointSymbology ||
            this.currentSymbology instanceof ClusteredPointSymbology ||
            this.currentSymbology instanceof PolygonSymbology
        ) {
            return this.currentSymbology.fillColor;
        } else {
            throw Error('This symbology has no fill color');
        }
    }

    set fillColor(fillColor: ColorParam) {
        if (this.currentSymbology instanceof PointSymbology) {
            this.updatePointSymbology({fillColor});
        } else if (this.currentSymbology instanceof ClusteredPointSymbology) {
            this.updateClusteredPointSymbology({fillColor});
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            this.updatePolygonSymbology({fillColor});
        } else {
            throw Error('This symbology has no fill color');
        }
    }

    get strokeColor(): ColorParam {
        if (
            this.currentSymbology instanceof PointSymbology ||
            this.currentSymbology instanceof ClusteredPointSymbology ||
            this.currentSymbology instanceof PolygonSymbology ||
            this.currentSymbology instanceof LineSymbology
        ) {
            return this.currentSymbology.stroke.color;
        } else {
            throw Error('This symbology has no stroke');
        }
    }

    set strokeColor(strokeColor: ColorParam) {
        if (this.currentSymbology instanceof PointSymbology) {
            this.updatePointSymbology({stroke: new Stroke(this.currentSymbology.stroke.width, strokeColor)});
        } else if (this.currentSymbology instanceof ClusteredPointSymbology) {
            this.updateClusteredPointSymbology({stroke: new Stroke(this.currentSymbology.stroke.width, strokeColor)});
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            this.updatePolygonSymbology({stroke: new Stroke(this.currentSymbology.stroke.width, strokeColor)});
        } else if (this.currentSymbology instanceof LineSymbology) {
            this.updateLineSymbology({stroke: new Stroke(this.currentSymbology.stroke.width, strokeColor)});
        } else {
            throw Error('This symbology has no stroke');
        }
    }

    get strokeWidth(): NumberParam {
        if (
            this.currentSymbology instanceof PointSymbology ||
            this.currentSymbology instanceof ClusteredPointSymbology ||
            this.currentSymbology instanceof PolygonSymbology ||
            this.currentSymbology instanceof LineSymbology
        ) {
            return this.currentSymbology.stroke.width;
        } else {
            throw Error('This symbology has no stroke');
        }
    }

    set strokeWidth(strokeWidth: NumberParam) {
        if (this.currentSymbology instanceof PointSymbology) {
            this.updatePointSymbology({stroke: new Stroke(strokeWidth, this.currentSymbology.stroke.color)});
        } else if (this.currentSymbology instanceof ClusteredPointSymbology) {
            this.updateClusteredPointSymbology({stroke: new Stroke(strokeWidth, this.currentSymbology.stroke.color)});
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            this.updatePolygonSymbology({stroke: new Stroke(strokeWidth, this.currentSymbology.stroke.color)});
        } else if (this.currentSymbology instanceof LineSymbology) {
            this.updateLineSymbology({stroke: new Stroke(strokeWidth, this.currentSymbology.stroke.color)});
        } else {
            throw Error('This symbology has no stroke');
        }
    }

    get radius(): NumberParam {
        if (this.currentSymbology instanceof PointSymbology || this.currentSymbology instanceof ClusteredPointSymbology) {
            return this.currentSymbology.radius;
        } else {
            throw Error('This symbology has no radius');
        }
    }

    set radius(radius: NumberParam) {
        if (this.currentSymbology instanceof PointSymbology || this.currentSymbology instanceof ClusteredPointSymbology) {
            this.updatePointSymbology({radius});
        } else {
            throw Error('This symbology has no radius');
        }
    }

    get supportsText(): boolean {
        return (
            this.currentSymbology instanceof PointSymbology ||
            this.currentSymbology instanceof LineSymbology ||
            this.currentSymbology instanceof PolygonSymbology
        );
    }

    get hasText(): boolean {
        if (
            this.currentSymbology instanceof PointSymbology ||
            this.currentSymbology instanceof LineSymbology ||
            this.currentSymbology instanceof PolygonSymbology
        ) {
            return !!this.currentSymbology.text;
        } else {
            // This symbology has no text
            return false;
        }
    }

    get textFillColor(): ColorParam {
        let textSymbology: TextSymbology | undefined;

        if (
            this.currentSymbology instanceof PointSymbology ||
            this.currentSymbology instanceof ClusteredPointSymbology ||
            this.currentSymbology instanceof LineSymbology ||
            this.currentSymbology instanceof PolygonSymbology
        ) {
            textSymbology = this.currentSymbology.text;
        } else {
            throw Error('This symbology has no text');
        }

        if (!textSymbology) {
            throw Error('TextSymbology is undefined');
        }

        return textSymbology.fillColor;
    }

    set textFillColor(fillColor: ColorParam) {
        const generateTextSymbology = (textSymbology?: TextSymbology): TextSymbology | undefined => {
            if (!textSymbology) {
                return undefined;
            }

            return new TextSymbology(textSymbology.attribute, fillColor, textSymbology.stroke);
        };

        if (this.currentSymbology instanceof PointSymbology) {
            this.updatePointSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            this.updatePolygonSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else if (this.currentSymbology instanceof LineSymbology) {
            this.updateLineSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else {
            throw Error('This symbology has no text');
        }
    }

    get textStrokeColor(): ColorParam {
        let textSymbology: TextSymbology | undefined;

        if (this.currentSymbology instanceof PointSymbology) {
            textSymbology = this.currentSymbology.text;
        } else if (this.currentSymbology instanceof LineSymbology) {
            textSymbology = this.currentSymbology.text;
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            textSymbology = this.currentSymbology.text;
        } else {
            throw Error('This symbology has no text');
        }

        if (!textSymbology) {
            throw Error('TextSymbology is undefined');
        }

        return textSymbology.stroke.color;
    }

    set textStrokeColor(strokeColor: ColorParam) {
        const generateTextSymbology = (textSymbology?: TextSymbology): TextSymbology | undefined => {
            if (!textSymbology) {
                return undefined;
            }

            return new TextSymbology(textSymbology.attribute, textSymbology.fillColor, new Stroke(textSymbology.stroke.width, strokeColor));
        };

        if (this.currentSymbology instanceof PointSymbology) {
            this.updatePointSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            this.updatePolygonSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else if (this.currentSymbology instanceof LineSymbology) {
            this.updateLineSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else {
            throw Error('This symbology has no text');
        }
    }

    get textStrokeWidth(): NumberParam {
        let textSymbology: TextSymbology | undefined;

        if (this.currentSymbology instanceof PointSymbology) {
            textSymbology = this.currentSymbology.text;
        } else if (this.currentSymbology instanceof LineSymbology) {
            textSymbology = this.currentSymbology.text;
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            textSymbology = this.currentSymbology.text;
        } else {
            throw Error('This symbology has no text');
        }

        if (!textSymbology) {
            throw Error('TextSymbology is undefined');
        }

        return textSymbology.stroke.width;
    }

    set textStrokeWidth(strokeWidth: NumberParam) {
        const generateTextSymbology = (textSymbology?: TextSymbology): TextSymbology | undefined => {
            if (!textSymbology) {
                return undefined;
            }

            return new TextSymbology(textSymbology.attribute, textSymbology.fillColor, new Stroke(strokeWidth, textSymbology.stroke.color));
        };

        if (this.currentSymbology instanceof PointSymbology) {
            this.updatePointSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            this.updatePolygonSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else if (this.currentSymbology instanceof LineSymbology) {
            this.updateLineSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else {
            throw Error('This symbology has no text');
        }
    }

    get textAttribute(): string | undefined {
        let textSymbology: TextSymbology | undefined;

        if (this.currentSymbology instanceof PointSymbology) {
            textSymbology = this.currentSymbology.text;
        } else if (this.currentSymbology instanceof LineSymbology) {
            textSymbology = this.currentSymbology.text;
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            textSymbology = this.currentSymbology.text;
        } else {
            throw Error('This symbology has no text');
        }

        if (!textSymbology) {
            return undefined;
        }

        return textSymbology.attribute;
    }

    set textAttribute(attribute: string | undefined) {
        const generateTextSymbology = (textSymbology?: TextSymbology): TextSymbology | null => {
            if (!attribute) {
                return null;
            }

            if (!textSymbology) {
                // generate default
                return new TextSymbology(attribute, new StaticColor(WHITE), new Stroke(new StaticNumber(1), new StaticColor(BLACK)));
            }

            return new TextSymbology(attribute, textSymbology.fillColor, textSymbology.stroke);
        };

        if (this.currentSymbology instanceof PointSymbology) {
            this.updatePointSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else if (this.currentSymbology instanceof PolygonSymbology) {
            this.updatePolygonSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else if (this.currentSymbology instanceof LineSymbology) {
            this.updateLineSymbology({text: generateTextSymbology(this.currentSymbology.text)});
        } else {
            throw Error('This symbology has no text');
        }
    }

    get isAutoSimplified(): boolean {
        if (!(this.currentSymbology instanceof LineSymbology || this.currentSymbology instanceof PolygonSymbology)) {
            return false;
        }

        return this.currentSymbology.autoSimplified;
    }

    updatePointSymbology(params: {radius?: NumberParam; fillColor?: ColorParam; stroke?: Stroke; text?: TextSymbology | null}): void {
        if (!(this.currentSymbology instanceof PointSymbology)) {
            return;
        }

        // unsetting with null
        const text = params.text === null ? undefined : (params.text ?? this.currentSymbology.text);

        this.currentSymbology = new PointSymbology(
            params.radius ?? this.currentSymbology.radius,
            params.fillColor ?? this.currentSymbology.fillColor,
            params.stroke ?? this.currentSymbology.stroke,
            text,
        );

        this.changedSymbology.emit(this.currentSymbology);
    }

    updateClusteredPointSymbology(params: {fillColor?: ColorParam; stroke?: Stroke}): void {
        if (!(this.currentSymbology instanceof ClusteredPointSymbology)) {
            return;
        }

        this.currentSymbology = new ClusteredPointSymbology(
            params.fillColor ?? this.currentSymbology.fillColor,
            params.stroke ?? this.currentSymbology.stroke,
        );

        this.changedSymbology.emit(this.currentSymbology);
    }

    updateLineSymbology(params: {stroke?: Stroke; text?: TextSymbology | null; autoSimplified?: boolean}): void {
        if (!(this.currentSymbology instanceof LineSymbology)) {
            return;
        }

        // unsetting with null
        const text = params.text === null ? undefined : (params.text ?? this.currentSymbology.text);

        this.currentSymbology = new LineSymbology(
            params.stroke ?? this.currentSymbology.stroke,
            text,
            params.autoSimplified ?? this.currentSymbology.autoSimplified,
        );

        this.changedSymbology.emit(this.currentSymbology);
    }

    updatePolygonSymbology(params: {fillColor?: ColorParam; stroke?: Stroke; text?: TextSymbology | null; autoSimplified?: boolean}): void {
        if (!(this.currentSymbology instanceof PolygonSymbology)) {
            return;
        }

        // unsetting with null
        const text = params.text === null ? undefined : (params.text ?? this.currentSymbology.text);

        this.currentSymbology = new PolygonSymbology(
            params.fillColor ?? this.currentSymbology.fillColor,
            params.stroke ?? this.currentSymbology.stroke,
            text,
            params.autoSimplified ?? this.currentSymbology.autoSimplified,
        );

        this.changedSymbology.emit(this.currentSymbology);
    }

    protected initializeAttributes(): void {
        this.workflowsService.getMetadata(this.symbologyWorkflow().workflowId).then((metadata) => {
            if (!(metadata.type === 'vector')) {
                return;
            }
            const allColumnNames: Array<string> = Object.keys(metadata.columns);
            const numericColumnNames: Array<string> = allColumnNames.filter(
                (column) =>
                    metadata.columns[column].dataType === FeatureDataType.Int ||
                    metadata.columns[column].dataType === FeatureDataType.Float,
            );
            this.numericAttributes.next(numericColumnNames);
            this.allAttributes.next(allColumnNames);
        });
    }
}
