import {Component, OnInit, ChangeDetectionStrategy, inject} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogContent, MatDialogActions, MatDialogClose} from '@angular/material/dialog';
import {Clipboard} from '@angular/cdk/clipboard';
import {Geometry} from 'ol/geom';
import OlFormatGeoJson from 'ol/format/GeoJSON';
import OlFormatWKT from 'ol/format/WKT';
import {DialogHeaderComponent} from '../../../dialogs/dialog-header/dialog-header.component';
import {CdkScrollable, CdkVirtualScrollViewport, CdkFixedSizeVirtualScroll, CdkVirtualForOf} from '@angular/cdk/scrolling';
import {MatButton} from '@angular/material/button';

/**
 * Opened as modal dialog to display a full set of coordinates and allow copying to clipboard
 */
@Component({
    selector: 'geoengine-full-display',
    templateUrl: './full-display.component.html',
    styleUrls: ['./full-display.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        DialogHeaderComponent,
        CdkScrollable,
        MatDialogContent,
        CdkVirtualScrollViewport,
        CdkFixedSizeVirtualScroll,
        CdkVirtualForOf,
        MatDialogActions,
        MatButton,
        MatDialogClose,
    ],
})
export class FullDisplayComponent implements OnInit {
    data = inject<{
        xStrings: string[];
        yStrings: string[];
        geometry: Geometry;
    }>(MAT_DIALOG_DATA);
    private clipboard = inject(Clipboard);

    xCoords: string[] = [];
    yCoords: string[] = [];

    ngOnInit(): void {
        this.xCoords = this.data.xStrings;
        this.yCoords = this.data.yStrings;
    }

    copyAsGeoJson(): void {
        const geometryJson = JSON.stringify(new OlFormatGeoJson().writeGeometryObject(this.data.geometry), null, 2);
        this.stringToClipboard(geometryJson);
    }

    copyAsWkt(): void {
        const geometryWkT = new OlFormatWKT().writeGeometry(this.data.geometry);
        this.stringToClipboard(geometryWkT);
    }

    stringToClipboard(toCopy: string): void {
        const pending = this.clipboard.beginCopy(toCopy);
        let remainingAttempts = 3;
        function attempt(): void {
            const copied = pending.copy();
            if (!copied && --remainingAttempts) {
                setTimeout(attempt);
            } else {
                pending.destroy();
            }
        }
        attempt();
    }
}
