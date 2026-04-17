import {HttpEventType} from '@angular/common/http';
import {ChangeDetectionStrategy, Component, effect, inject, input, OnInit} from '@angular/core';
import {FormControl, FormGroup, UntypedFormBuilder, ValidatorFn, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import moment from 'moment';
import {combineLatest, mergeMap} from 'rxjs';
import {RasterResultDescriptorDict, WcsParamsDict, WfsParamsDict} from '../backend/backend.model';
import {BackendService} from '../backend/backend.service';
import {MapService} from '../map/map.service';
import {ProjectService} from '../project/project.service';
import {SpatialReferenceService} from '../spatial-references/spatial-reference.service';
import {bboxAsOgcString, gridOffsetsAsOgcString, gridOriginAsOgcString} from '../util/spatial_reference';
import {
    Layer,
    SpatialReference,
    Time,
    olExtentToTuple,
    extentToBboxDict,
    geoengineValidators,
    TimeInterval,
    UserService,
    NotificationService,
    FxLayoutDirective,
    FxFlexDirective,
    FxLayoutGapDirective,
    CommonModule,
} from '@geoengine/common';
import {TypedResultDescriptor} from '@geoengine/api-client';
import {CoreConfig} from '../config.service';
import {toSignal} from '@angular/core/rxjs-interop';
import {SidenavHeaderComponent} from '../sidenav/sidenav-header/sidenav-header.component';
import {MatCard, MatCardHeader, MatCardTitle, MatCardSubtitle, MatCardContent} from '@angular/material/card';
import {MatFormField, MatLabel, MatInput, MatHint} from '@angular/material/input';
import {MatButton} from '@angular/material/button';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';

export interface DownloadLayerForm {
    bboxMinX: FormControl<number>;
    bboxMaxX: FormControl<number>;
    bboxMinY: FormControl<number>;
    bboxMaxY: FormControl<number>;

    timeInterval: FormControl<TimeInterval>;

    interpolationMethod: FormControl<string>;
    inputResolution: FormControl<string>;
    inputResolutionX: FormControl<number>;
    inputResolutionY: FormControl<number>;
}

@Component({
    selector: 'geoengine-download-layer',
    templateUrl: './download-layer.component.html',
    styleUrls: ['./download-layer.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        FxLayoutDirective,
        ReactiveFormsModule,
        FxFlexDirective,
        MatCard,
        MatCardHeader,
        MatCardTitle,
        MatCardSubtitle,
        MatCardContent,
        FxLayoutGapDirective,
        MatFormField,
        MatLabel,
        MatInput,
        MatButton,
        CommonModule,
        MatSelect,
        MatOption,
        MatHint,
    ],
})
export class DownloadLayerComponent implements OnInit {
    protected readonly backend = inject(BackendService);
    protected readonly projectService = inject(ProjectService);
    protected readonly userService = inject(UserService);
    protected readonly mapService = inject(MapService);
    protected readonly notificationService = inject(NotificationService);
    protected readonly spatialReferenceService = inject(SpatialReferenceService);
    private readonly formBuilder = inject(UntypedFormBuilder);
    public readonly config = inject(CoreConfig);

    readonly layer = input.required<Layer>();

    readonly rasterInterpolationMethods = [
        ['Nearest Neighbor', 'nearestNeighbor'],
        ['Bilinear', 'biLinear'],
    ];

    form: FormGroup<DownloadLayerForm>;

    isSelectingBox = false;

    private editedExtent = false;
    private editedResolution = false;
    private editedTime = false;

    private readonly projectTime = toSignal(this.projectService.getTimeStream());
    private readonly viewportSize = toSignal(this.mapService.getViewportSizeStream());

    constructor() {
        // initialize with the current time to have a defined value
        const time = new Time(moment.utc(), moment.utc());

        this.form = this.formBuilder.group({
            bboxMinX: new FormControl(-180.0, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            bboxMaxX: new FormControl(180.0, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            bboxMinY: new FormControl(-90.0, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            bboxMaxY: new FormControl(90.0, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            timeInterval: new FormControl(
                {start: time.start, timeAsPoint: true, end: time.end},
                {nonNullable: true, validators: [Validators.required]},
            ),
            interpolationMethod: new FormControl(this.rasterInterpolationMethods[0][1], {
                nonNullable: true,
                validators: [Validators.required],
            }),
            inputResolution: new FormControl('source', {
                nonNullable: true,
                validators: [Validators.required],
            }),
            inputResolutionX: new FormControl(1.0, {
                nonNullable: true,
                validators: [this.resolutionValidator()],
            }),
            inputResolutionY: new FormControl(1.0, {
                nonNullable: true,
                validators: [this.resolutionValidator()],
            }),
        });

        this.updateRegionByExtent(olExtentToTuple(this.mapService.getViewportSize().extent));

        effect(() => {
            const t = this.projectTime();

            if (!t) return;

            if (this.editedTime) return;

            const newTime = t.clone();

            this.form.controls['timeInterval'].setValue({
                start: newTime.start,
                end: newTime.end,
                timeAsPoint: newTime.start.isSame(newTime.end),
            });
        });

        effect(() => {
            const viewport = this.viewportSize();

            if (!viewport) return;

            if (this.editedExtent && this.editedResolution) return;

            const extent = olExtentToTuple(viewport.extent);

            if (!this.editedExtent) this.updateRegionByExtent(extent);
            if (!this.editedResolution) this.updateResolution(viewport.resolution);
        });
    }

    ngOnInit(): void {
        if (this.layer().layerType === 'vector') {
            this.form.controls['inputResolution'].setValue('value');
        }
    }

    setEditedExtent(): void {
        this.editedExtent = true;
    }

    setEditedResolution(): void {
        this.editedResolution = true;
    }

    setEditedTime(): void {
        this.editedTime = true;
    }

    selectBox(): void {
        this.isSelectingBox = true;
        this.notificationService.info('Select region on the map');
        this.mapService.startBoxDrawInteraction((feature) => {
            const b = feature.getGeometry()?.getExtent();
            if (b) {
                this.form.controls['bboxMinX'].setValue(b[0]);
                this.form.controls['bboxMaxX'].setValue(b[2]);
                this.form.controls['bboxMinY'].setValue(b[1]);
                this.form.controls['bboxMaxY'].setValue(b[3]);
            }
            this.isSelectingBox = false;

            this.setEditedExtent();
        });
    }

    download(): void {
        if (this.form.invalid) {
            return;
        }

        if (this.layer().layerType === 'raster') {
            this.rasterDownload();
        } else if (this.layer().layerType === 'vector') {
            this.vectorDownload();
        }
    }

    private rasterDownload(): void {
        combineLatest([
            this.projectService.getSpatialReferenceOnce(),
            this.userService.getSessionTokenForRequest(),
            this.projectService.getWorkflowMetaData(this.layer().workflowId),
        ])
            .pipe(
                mergeMap(([sref, sessionToken, resultDescriptor]) => {
                    const params: WcsParamsDict = this.makeWcsParams(sref, resultDescriptor);

                    return this.backend.downloadRasterLayer(this.layer().workflowId, sessionToken, params);
                }),
            )
            .subscribe({
                next: (event) => {
                    if (event.type !== HttpEventType.Response || event.body === null) {
                        return;
                    }

                    const tiffFile = new File([event.body], `${this.layer.name}.tiff`);
                    const url = window.URL.createObjectURL(tiffFile);

                    // trigger download
                    const anchor = document.createElement('a');
                    anchor.href = url;
                    anchor.download = tiffFile.name;
                    anchor.click();
                },
                error: (error) => {
                    this.notificationService.error(`File download failed: ${error.message}`);
                },
            });
    }

    private vectorDownload(): void {
        combineLatest([
            this.projectService.getSpatialReferenceOnce(),
            this.userService.getSessionTokenForRequest(),
            this.projectService.getWorkflowMetaData(this.layer().workflowId),
        ])
            .pipe(
                mergeMap(([sref, sessionToken, resultDescriptor]) => {
                    const params: WfsParamsDict = this.makeWfsParams(sref, resultDescriptor);

                    return this.backend.wfsGetFeature(params, sessionToken);
                }),
            )
            .subscribe({
                next: (json) => {
                    const jsonFile = new File([JSON.stringify(json)], `${this.layer.name}.json`);
                    const url = window.URL.createObjectURL(jsonFile);

                    // trigger download
                    const anchor = document.createElement('a');
                    anchor.href = url;
                    anchor.download = jsonFile.name;
                    anchor.click();
                },
                error: (error) => {
                    this.notificationService.error(`File download failed: ${error.message}`);
                },
            });
    }

    private updateRegionByExtent(extent: [number, number, number, number]): void {
        this.form.controls['bboxMinX'].setValue(extent[0]);
        this.form.controls['bboxMinY'].setValue(extent[1]);
        this.form.controls['bboxMaxX'].setValue(extent[2]);
        this.form.controls['bboxMaxY'].setValue(extent[3]);
    }

    private updateResolution(resolution: number): void {
        this.form.controls['inputResolutionX'].setValue(resolution);
        this.form.controls['inputResolutionY'].setValue(resolution);
    }

    private makeWcsParams(sref: SpatialReference, resultDescriptor: TypedResultDescriptor): WcsParamsDict {
        const wcsUrn = sref.wcsUrn();
        if (!wcsUrn) {
            this.notificationService.error('Could not determine WCS URN for spatial reference');
            throw new Error('Could not determine WCS URN for spatial reference');
        }

        if (resultDescriptor.type !== 'raster') {
            throw new Error('Result descriptor is not of type raster');
        }

        const rasterRd = resultDescriptor as RasterResultDescriptorDict;

        const [minX, maxX, minY, maxY] = [
            this.form.controls['bboxMinX'].value,
            this.form.controls['bboxMaxX'].value,
            this.form.controls['bboxMinY'].value,
            this.form.controls['bboxMaxY'].value,
        ];

        const bboxOgc = bboxAsOgcString(minX, maxX, minY, maxY, sref.srsString);

        const time = this.formToTime();

        const resolution = this.formToResolution(rasterRd);

        const params: WcsParamsDict = {
            service: 'WCS',
            request: 'GetCoverage',
            version: '1.1.1',
            identifier: this.layer().workflowId,
            boundingbox: `${bboxOgc},${wcsUrn}`,
            format: 'image/tiff',
            gridbasecrs: wcsUrn,
            gridcs: 'urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS',
            gridtype: 'urn:ogc:def:method:WCS:1.1:2dSimpleGrid',
            gridorigin: gridOriginAsOgcString(minX, maxY, sref.srsString),
            gridoffsets: gridOffsetsAsOgcString(resolution.x, resolution.y, sref.srsString),
            time: time.asRequestString(),
        };
        return params;
    }

    private makeWfsParams(sref: SpatialReference, resultDescriptor: TypedResultDescriptor): WfsParamsDict {
        if (resultDescriptor.type !== 'vector') {
            throw new Error('Result descriptor is not of type vector');
        }

        const extent: [number, number, number, number] = [
            this.form.controls['bboxMinX'].value,
            this.form.controls['bboxMinY'].value,
            this.form.controls['bboxMaxX'].value,
            this.form.controls['bboxMaxY'].value,
        ];

        const bbox = extentToBboxDict(extent);

        const time = this.formToTime().toDict();

        return {
            bbox: bbox,
            time: time,
            srsName: sref.srsString,
            workflowId: this.layer().workflowId,
        };
    }

    private formToResolution(rasterRd?: RasterResultDescriptorDict): {x: number; y: number} {
        const inputResolution = this.form.controls['inputResolution'].value;

        let resolution = {x: 1.0, y: 1.0};

        if (inputResolution === 'source' && rasterRd) {
            if (!rasterRd.resolution) {
                // TODO: do not allow selecting this in the first place, if no resolution is defined
                throw new Error('Source resolution is not defined');
            }
            resolution = rasterRd.resolution;
        } else if (inputResolution === 'value') {
            resolution = {
                x: this.form.controls['inputResolutionX'].value,
                y: this.form.controls['inputResolutionY'].value,
            };
        }
        return resolution;
    }

    private formToTime(): Time {
        const timeInterval = this.form.get('timeInterval')!.value;

        const start = timeInterval.start;
        const timeAsPoint = timeInterval.timeAsPoint;
        let end = timeInterval.end;

        if (timeAsPoint) {
            end = start;
        }

        return new Time(start, end);
    }

    private resolutionValidator(): ValidatorFn {
        const validator = Validators.compose([Validators.required, geoengineValidators.largerThan(0.0)]);

        if (!validator) {
            throw Error('Invalid validator');
        }

        return geoengineValidators.conditionalValidator(validator, () => this.form?.get('inputResolution')?.value === 'value');
    }
}
