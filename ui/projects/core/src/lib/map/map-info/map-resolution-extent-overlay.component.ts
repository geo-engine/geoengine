import {ChangeDetectionStrategy, Component, inject, input} from '@angular/core';
import {ProjectService} from '../../project/project.service';
import {MapService} from '../map.service';
import {BehaviorSubject, combineLatestWith, Observable} from 'rxjs';
import {map} from 'rxjs/operators';
import {SpatialReferenceService} from '../../spatial-references/spatial-reference.service';
import {AsyncPipe} from '@angular/common';

/**
 * The `geoengine-map-resolution-extent-overlay` displays information about the resolution and extent of the visible map(s).
 */
@Component({
    selector: 'geoengine-map-resolution-extent-overlay',
    templateUrl: 'map-resolution-extent-overlay.component.html',
    styleUrls: ['map-resolution-extent-overlay.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [AsyncPipe],
})
export class MapResolutionExtentOverlayComponent {
    private mapService = inject(MapService);
    private spatialReferenceService = inject(SpatialReferenceService);
    private projectService = inject(ProjectService);

    public readonly bottom = input.required<number>();

    highPrecision = new BehaviorSubject<boolean>(false);
    private static readonly lowNumFractions = 2;
    private static readonly highNumFractions = 12;

    togglePrecision(): void {
        this.highPrecision.next(!this.highPrecision.getValue());
    }

    info(): Observable<string[]> {
        return this.projectService.getSpatialReferenceStream().pipe(
            map((srs) => this.spatialReferenceService.getOlProjection(srs).getUnits()),
            combineLatestWith(this.mapService.getViewportSizeStream()),
            map(([unit, viewport]) => {
                const numFractions = this.highPrecision.getValue()
                    ? MapResolutionExtentOverlayComponent.highNumFractions
                    : MapResolutionExtentOverlayComponent.lowNumFractions;

                const resolution = this.trimFraction(viewport.resolution.toFixed(numFractions));
                const xMin = viewport.extent[0].toFixed(numFractions);
                const xMax = viewport.extent[2].toFixed(numFractions);
                const yMin = viewport.extent[1].toFixed(numFractions);
                const yMax = viewport.extent[3].toFixed(numFractions);

                const toTrim = Math.min(
                    this.countTrailingFractionZeros(xMin),
                    this.countTrailingFractionZeros(xMax),
                    this.countTrailingFractionZeros(yMin),
                    this.countTrailingFractionZeros(yMax),
                );

                return [
                    resolution,
                    unit,
                    this.trimFraction(xMin, toTrim),
                    this.trimFraction(yMin, toTrim),
                    this.trimFraction(xMax, toTrim),
                    this.trimFraction(yMax, toTrim),
                ];
            }),
        );
    }

    private trimFraction(number: string, trim?: number): string {
        trim ??= this.countTrailingFractionZeros(number);
        if (trim == this.getFraction(number).length) {
            trim += 1;
        }
        return number.substring(0, number.length - trim);
    }

    private getFraction(number: string): string {
        const split = number.split('.');
        if (split.length < 2) {
            return '';
        }
        return split[1];
    }

    private countTrailingFractionZeros(number: string): number {
        const frac = this.getFraction(number);
        const trailingZeros = frac
            .split('')
            .reverse()
            .findIndex((c) => c != '0');
        if (trailingZeros == -1) {
            return frac.length;
        }
        return trailingZeros;
    }
}
