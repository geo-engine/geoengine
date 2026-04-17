import {
    Component,
    OnInit,
    ChangeDetectionStrategy,
    AfterViewInit,
    OnDestroy,
    ElementRef,
    ChangeDetectorRef,
    inject,
    input,
    viewChild,
    effect,
} from '@angular/core';
import {MatPaginator} from '@angular/material/paginator';
import {combineLatest, Observable, Subject, Subscription} from 'rxjs';
import {Feature as OlFeature} from 'ol';
import {FeatureSelection, ProjectService} from '../../project/project.service';
import {DataSource} from '@angular/cdk/collections';
import OlGeometry from 'ol/geom/Geometry';
import OlPolygon from 'ol/geom/Polygon';
import {MatDialog} from '@angular/material/dialog';
import {FullDisplayComponent} from './full-display/full-display.component';
import {MediaviewComponent} from '../mediaview/mediaview.component';
import {
    Layer,
    RasterLayer,
    RasterLayerMetadata,
    ResultTypes,
    Time,
    VectorColumnDataType,
    VectorColumnDataTypes,
    VectorData,
    VectorLayer,
    VectorLayerMetadata,
} from '@geoengine/common';
import {Measurement, ClassificationMeasurement} from '@geoengine/common';
import {Map} from 'immutable';
import {
    MatTable,
    MatColumnDef,
    MatHeaderCellDef,
    MatHeaderCell,
    MatCellDef,
    MatCell,
    MatHeaderRowDef,
    MatHeaderRow,
    MatRowDef,
    MatRow,
    MatNoDataRow,
} from '@angular/material/table';
import {MatCheckbox} from '@angular/material/checkbox';
import {MatIconButton} from '@angular/material/button';
import {MatTooltip} from '@angular/material/tooltip';
import {MatIcon} from '@angular/material/icon';
import {MatFormField, MatLabel} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {FormsModule} from '@angular/forms';
import {MatOption} from '@angular/material/autocomplete';

@Component({
    selector: 'geoengine-datatable',
    templateUrl: './table.component.html',
    styleUrls: ['./table.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatTable,
        MatColumnDef,
        MatHeaderCellDef,
        MatHeaderCell,
        MatCellDef,
        MatCell,
        MatCheckbox,
        MatIconButton,
        MatTooltip,
        MatIcon,
        MediaviewComponent,
        MatHeaderRowDef,
        MatHeaderRow,
        MatRowDef,
        MatRow,
        MatNoDataRow,
        MatFormField,
        MatLabel,
        MatSelect,
        FormsModule,
        MatOption,
        MatPaginator,
    ],
})
export class DataTableComponent implements OnInit, AfterViewInit, OnDestroy {
    protected readonly dialog = inject(MatDialog);
    protected readonly projectService = inject(ProjectService);
    protected readonly hostElement = inject<ElementRef<HTMLElement>>(ElementRef);
    protected readonly changeDetectorRef = inject(ChangeDetectorRef);

    readonly paginator = viewChild.required(MatPaginator);

    readonly layer = input<Layer>();

    readonly layerTypes = ResultTypes.VECTOR_TYPES;
    readonly columnDataTypes = VectorColumnDataTypes;

    // selectedFeature$ = new BehaviorSubject<FeatureSelection>({feature: undefined});

    dataSource = new FeatureDataSource();
    displayedColumns: Array<string> = [];
    featureColumns: Array<string> = [];
    featureColumnDataTypes: Array<VectorColumnDataType> = [];
    checkboxLabels: Array<string> = [];
    measurements!: Map<string, Measurement>;

    protected layerDataSubscription?: Subscription = undefined;
    protected selectedFeatureSubscription?: Subscription = undefined;

    constructor() {
        effect(() => {
            const layer = this.layer();
            if (layer) {
                this.selectLayer(layer);
            } else {
                this.emptyTable();
            }
        });
    }

    ngOnInit(): void {
        this.selectedFeatureSubscription = this.projectService.getSelectedFeatureStream().subscribe((selection) => {
            this.changeDetectorRef.markForCheck();
            this.navigatePage(selection);
        });
    }

    ngAfterViewInit(): void {
        this.dataSource.paginator = this.paginator();
    }

    ngOnDestroy(): void {
        if (this.layerDataSubscription) {
            this.layerDataSubscription.unsubscribe();
        }

        if (this.selectedFeatureSubscription) {
            this.selectedFeatureSubscription.unsubscribe();
        }
    }

    selectLayer(layer: Layer): void {
        if (this.layerDataSubscription) {
            this.layerDataSubscription.unsubscribe();
        }

        const dataStream = this.projectService.getLayerDataStream(layer);
        const metadataStream = this.projectService.getLayerMetadata(layer);

        if (!dataStream || !metadataStream) {
            // layer was removed
            this.emptyTable();
            return;
        }

        this.layerDataSubscription = combineLatest([dataStream, metadataStream]).subscribe(
            ([data, metadata]) => {
                if (layer instanceof VectorLayer && data instanceof VectorData) {
                    this.processVectorLayer(layer, metadata as VectorLayerMetadata, data);
                } else if (layer instanceof RasterLayer) {
                    this.processRasterLayer(layer, metadata as RasterLayerMetadata, data);
                }
            },
            (_error) => {
                // TODO: cope with error
            },
        );
    }

    processVectorLayer(_layer: VectorLayer, metadata: VectorLayerMetadata, data?: VectorData): void {
        if (!data?.data || data.data.length === 0) {
            this.emptyTable();
            return;
        }
        this.dataSource.data = data.data;
        this.featureColumns = metadata.dataTypes.keySeq().toArray();
        this.measurements = metadata.measurements;
        this.featureColumnDataTypes = metadata.dataTypes.valueSeq().toArray();
        if (this.displayedColumns.length === 0) {
            // Only true when the table is first created. Prevents "forgetting" selected columns when zooming/scrolling the map
            this.displayedColumns = ['_____select', '_____coordinates', '_____table__start', '_____table__end'].concat(this.featureColumns);
        }
        this.featureColumnDataTypes = this.getColumnProperties();
        this.checkboxLabels = ['_____coordinates', '_____table__start', '_____table__end'].concat(this.featureColumns);
        setTimeout(() => this.navigatePage(this.projectService.getSelectedFeature()));
    }

    /**
     * Used by HTML template when (de)selecting a column to make sure the leftmost checkbox column stays visible
     */
    prependCheckboxColumn(): void {
        // Necessary because dropdown-menu is bound to displayedColumns. Since the menu doesn't contain an option for the
        // select-checkbox, that column gets removed on each change as well. It is added back in here.
        if (!this.displayedColumns.find((x) => x === '_____select')) {
            this.displayedColumns = ['_____select'].concat(this.displayedColumns);
        }
    }

    /**
     * Used only by HTML template to display prettier names for default columns inside dropdown menu
     */
    fixedColumnDescriptors(columnName: string): string {
        return columnName === '_____coordinates'
            ? 'Coordinates'
            : columnName === '_____table__start'
              ? 'Start '
              : columnName === '_____table__end'
                ? 'End'
                : columnName === '_____select' // TODO delete this if hiding select box is not necessary
                  ? 'Select'
                  : columnName;
    }

    processRasterLayer(_layer: RasterLayer, _metadata: RasterLayerMetadata, _data: unknown): void {
        // TODO: implement

        this.emptyTable();
    }

    /**
     * Show an empty table when there is no data to display
     */
    emptyTable(): void {
        this.displayedColumns = [];
        this.dataSource.data = [];

        // TODO: implement some default message
    }

    isSelected(feature: OlFeature<OlGeometry>): boolean {
        return feature.getId() === this.projectService.getSelectedFeature().feature;
    }

    coordinateFromGeometry(geometry: OlFeature): string {
        // For truncated coordinate view in table
        const coords: string[][] = this.readCoordinates(geometry);
        const contd: string = coords[0].length > 1 ? '...' : '';
        const displayLength = 5;
        const output = ` ${this.sliceColumnContent(coords[0][0], displayLength)}, ${this.sliceColumnContent(
            coords[1][0],
            displayLength,
        )} ${contd}`;
        return output;
    }

    /**
     * Extracts the coordinates of an open layers feature as strings
     *
     * @param geometry The feature to extract coordinates from
     * @returns A nested string[][] where index 0 of the outer array are x-Coordinates, index 1 are y-Coordinates
     */
    readCoordinates(geometry: OlFeature): string[][] {
        const featureType: string | undefined = geometry.getGeometry()?.getType();
        const xCoords: string[] = [];
        const yCoords: string[] = [];

        if (
            !(
                featureType === 'Polygon' ||
                featureType === 'MultiPolygon' ||
                featureType === 'LineString' ||
                featureType === 'MultiLineString' ||
                featureType === 'Point'
            )
        ) {
            xCoords.push('N/A');
            yCoords.push('N/A');
            return [xCoords, yCoords];
        }

        const poly: OlPolygon = geometry.getGeometry() as OlPolygon;
        const l = poly.getCoordinates().length;
        let allCoords: string[] = [];
        for (let i = 0; i < l; i++) {
            const coord = poly.getCoordinates()[i].toString().split(',');
            allCoords = allCoords.concat(coord);
        }
        for (let i = 0; i < allCoords.length - 1; i += 2) {
            xCoords.push(allCoords[i]);
            yCoords.push(allCoords[i + 1]);
        }
        return [xCoords, yCoords];
    }

    onFullDisplayClick(output: OlFeature): void {
        const coords: string[][] = this.readCoordinates(output);
        this.dialog.open(FullDisplayComponent, {data: {xStrings: coords[0], yStrings: coords[1], geometry: output.getGeometry()}});
    }

    readTimePropertyStart(geometry: OlFeature): string | undefined {
        const result = geometry['values_']['_____table__start'];
        if (!result) {
            return undefined;
        }

        const timeInstant = new Time(result);
        return timeInstant.startStringOrNegInf();
    }

    readTimePropertyEnd(geometry: OlFeature): string | undefined {
        const result: string = geometry['values_']['_____table__end'];
        if (!result) {
            return undefined;
        }

        const timeInstant = new Time(result);
        return timeInstant.endStringOrPosInf();
    }

    select(feature: OlFeature<OlGeometry>, select: boolean): void {
        if (select) {
            this.projectService.setSelectedFeature(feature);
        } else {
            this.projectService.setSelectedFeature(undefined);
        }
    }

    /**
     * Truncates column text.
     */
    sliceColumnContent(columnText: string, colmaxlen: number): string {
        const colText = columnText?.length > colmaxlen ? columnText.slice(0, colmaxlen) + '...' : columnText;
        return colText;
    }

    protected resolveClassification(columnName: string, value: number): string {
        const measurement = this.measurements.get(columnName)!;
        if (measurement instanceof ClassificationMeasurement) {
            const mapping = measurement.classes.get(value);
            if (mapping) {
                return mapping;
            }
        }
        return value.toString();
    }

    protected navigatePage(selection: FeatureSelection): void {
        const paginator = this.paginator();
        if (!paginator) {
            return;
        }

        for (let i = 0; i < this.dataSource.data.length; i++) {
            const feature = this.dataSource.data[i];
            if (feature.getId() === selection.feature) {
                const page = Math.floor(i / paginator.pageSize);
                paginator.pageIndex = page;
                paginator.page.next({
                    pageIndex: page,
                    pageSize: paginator.pageSize,
                    length: paginator.length,
                });
                break;
            }
        }
    }

    /**
     * Tests and gets the content type of each column of the data source.
     */
    private getColumnProperties(): Array<VectorColumnDataType> {
        const types: Array<VectorColumnDataType> = [];

        for (let column = 0; column < this.featureColumns.length; column++) {
            let columnType = this.featureColumnDataTypes[column];
            for (const rowData of this.dataSource.data) {
                const tmp = rowData.get(this.featureColumns[column]);
                if (typeof tmp === 'string' && tmp !== '') {
                    const tmpUrls = tmp.split(/(,)/g);
                    for (const tmpUrl of tmpUrls) {
                        const mediaType = MediaviewComponent.getType(tmpUrl);
                        if (mediaType !== '' && mediaType !== 'text') {
                            columnType = VectorColumnDataTypes.Media;
                            break;
                        }
                    }
                }
            }
            types[column] = columnType;
        }
        return types;
    }
}

/**
 * A custom data source that reacts on pagination and input data changes.
 *
 * It was necessary to implement it because it seems to be much faster than `MatTableDataSource`.
 */
class FeatureDataSource extends DataSource<OlFeature<OlGeometry>> {
    protected _data: Array<OlFeature<OlGeometry>> = [];
    protected data$ = new Subject<Array<OlFeature<OlGeometry>>>();

    protected _paginator?: MatPaginator;
    protected paginatorSubscription?: Subscription;

    constructor() {
        super();
    }

    set data(data: Array<OlFeature<OlGeometry>>) {
        this._data = data;

        if (this.paginator) {
            this.paginator.length = data.length;
            this.processPage();
        }
    }

    get data(): Array<OlFeature<OlGeometry>> {
        return this._data;
    }

    set paginator(paginator: MatPaginator | undefined) {
        if (this.paginatorSubscription) {
            this.paginatorSubscription.unsubscribe();
        }

        this._paginator = paginator;

        if (!this.paginator) {
            return;
        }

        // update length wrt. data
        this.paginator.length = this.data.length;

        // subscribe to page events…
        this.paginatorSubscription = this.paginator.page.subscribe(() => this.processPage());

        // …but fire initial event
        this.processPage();
    }

    get paginator(): MatPaginator | undefined {
        return this._paginator;
    }

    connect(): Observable<Array<OlFeature<OlGeometry>>> {
        return this.data$;
    }

    /**
     * Clean up resources
     */
    disconnect(): void {
        if (this.paginatorSubscription) {
            this.paginatorSubscription.unsubscribe();
        }
    }

    /**
     * display a portion of the data
     */
    protected processPage(): void {
        if (!this.paginator) {
            return;
        }

        const start = this.paginator.pageIndex * this.paginator.pageSize;
        const end = start + this.paginator.pageSize;

        if (start > this.data.length) {
            // reset paginator
            this.paginator.pageIndex = 0;

            this.processPage();
            return;
        }

        this.data$.next(this.data.slice(start, end));
    }
}
