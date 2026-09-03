import {computed, Directive, inject, input, signal} from '@angular/core';
import OlOverlay from 'ol/Overlay';
import OlGeomLineString from 'ol/geom/LineString';
import OlGeomPolygon from 'ol/geom/Polygon';
import {GeometryFunction as OlGeometryFunction, createBox} from 'ol/interaction/Draw';
import {Type as OlGeometryType} from 'ol/geom/Geometry';
import {drawContrastStrokeStyle, drawStyle, MapContainerComponent, MapService} from '@geoengine/core';
import {getArea, getLength} from 'ol/sphere.js';
import {assertNever} from '../../../../common/src/lib/util/assertions';
import {getCenter} from 'ol/extent';
import OlFeature from 'ol/Feature';
import OlGeometry from 'ol/geom/Geometry';
import OlLayerVector from 'ol/layer/Vector';
import OlSourceVector from 'ol/source/Vector';
import {AppConfig} from '../app-config.service';

export enum MeasurementType {
    Line = 'line',
    Box = 'box',
}

enum State {
    None,
    Drawing,
    Done,
}

const TOOLTIP_COLORS = {
    background: 'var(--geoengine-background-color)',
    border: 'var(--geoengine-divider-color)',
    color: 'var(--geoengine-foreground-text-color)',
} as const;

@Directive({
    selector: '[geoengineMeasure]',
    standalone: true,
    exportAs: 'geoengineMeasure',
    host: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        '(click)': 'toggle()',
        // eslint-disable-next-line @typescript-eslint/naming-convention
        '[attr.aria-pressed]': 'ariaPressed()',
    },
})
export class MeasureDirective {
    private readonly mapService = inject(MapService);
    private readonly config = inject(AppConfig);

    /** The OpenLayers map instance that receives the measurement overlay and draw interaction. */
    readonly mapComponent = input.required<MapContainerComponent>({alias: 'geoengineMeasure'});
    /** The type of measurement to perform (line or box). */
    readonly measurementType = input.required<MeasurementType>();

    /** Whether the measure tool is currently active. */
    readonly isActive = computed(() => this.state() !== State.None);
    /** Exposes the pressed state for the host button. */
    readonly ariaPressed = computed(() => (this.state() === State.Drawing ? 'true' : 'false'));

    private readonly state = signal(State.None);

    private readonly tooltip = new OlOverlay({
        offset: [0, -7],
        positioning: 'bottom-center',
        element: this.createTooltipElement(),
    });

    /** Toggles the measure interaction on or off. */
    toggle(): void {
        if (this.isActive()) {
            this.state.set(State.None);
            if (this.mapService.isDrawInteractionAttached()) this.mapService.endDrawInteraction();
            this.hideTooltip();
            this.mapComponent().overlayLayer.set(undefined);
            return;
        }

        this.state.set(State.Drawing);
        this.reattachTooltip();
        this.hideTooltip();

        const measurementType = this.measurementType();
        let drawType: OlGeometryType;
        let geometryFn: OlGeometryFunction | undefined;
        switch (measurementType) {
            case MeasurementType.Line:
                drawType = 'LineString';
                geometryFn = undefined;
                break;
            case MeasurementType.Box:
                drawType = 'Circle';
                geometryFn = createBox();
                break;
            default:
                assertNever(measurementType);
        }

        this.mapService.startDrawInteraction(drawType, true, geometryFn, (feature) => {
            const mapProjectionCode = this.mapComponent().getMap().getView().getProjection().getCode();
            const geometry = feature.getGeometry();

            if (geometry instanceof OlGeomLineString) {
                const length = measureLengthInMeters(geometry, mapProjectionCode);
                this.showTooltip(length, geometry.getLastCoordinate());
            } else if (geometry instanceof OlGeomPolygon) {
                const area = measureAreaInSquareMeters(geometry, mapProjectionCode);
                this.showTooltip(area, getCenter(geometry.getExtent()));
            }

            this.mapComponent().overlayLayer.set(this.layerFromFeature(feature));

            this.state.set(State.Done);
        });
    }

    private layerFromFeature(feature: OlFeature<OlGeometry>): OlLayerVector<OlSourceVector<OlFeature>> {
        const source = new OlSourceVector({wrapX: false});
        source.addFeatures([feature]);

        return new OlLayerVector({
            source: source,
            style: [drawContrastStrokeStyle(this.config.MAP.DRAWING.AFTER_DRAW_STYLE), drawStyle(this.config.MAP.DRAWING.AFTER_DRAW_STYLE)],
        });
    }

    private createTooltipElement(): HTMLElement {
        const tooltipElement = document.createElement('div');

        // 1. Create a Shadow Root inside the div
        const shadow = tooltipElement.attachShadow({mode: 'open'});

        // 2. Create the CSSStyleSheet
        const sheet = new CSSStyleSheet();
        sheet.replaceSync(`
            :host {
                position: relative;
                
                opacity: 1;
                font-weight: 600;
                background-color: ${TOOLTIP_COLORS.background};
                color: ${TOOLTIP_COLORS.color};
                border: 1px solid ${TOOLTIP_COLORS.border};
                border-radius: 0.25rem;

                padding: 0.25rem 0.5rem;
                white-space: nowrap;
                font-size: 0.75rem;

                cursor: default;
                user-select: none;
            }

            :host:before {
                border-top: 6px solid ${TOOLTIP_COLORS.background};
                border-right: 6px solid transparent;
                border-left: 6px solid transparent;
                content: "";
                position: absolute;
                bottom: -6px;
                margin-left: -7px;
                left: 50%;
            }
        `);

        // 3. Adopt the stylesheet into the shadow root
        shadow.adoptedStyleSheets = [sheet];

        return tooltipElement;
    }

    private reattachTooltip(): void {
        const map = this.mapComponent().getMap();

        const overlays = map.getOverlays();
        if (overlays.getArray().includes(this.tooltip)) {
            overlays.remove(this.tooltip);
        }

        map.addOverlay(this.tooltip);
    }

    private showTooltip(lengthOrArea: number, coordinate: number[]): void {
        const activeTooltipElement = this.tooltip.getElement();
        if (!activeTooltipElement) return;

        const shadow = activeTooltipElement.shadowRoot;
        if (!shadow) return;

        const measurementType = this.measurementType();
        switch (measurementType) {
            case MeasurementType.Line:
                shadow.textContent = formatLength(lengthOrArea);
                break;
            case MeasurementType.Box:
                shadow.textContent = formatArea(lengthOrArea);
                break;
            default:
                assertNever(measurementType);
        }

        this.tooltip.setPosition(coordinate);
    }

    private hideTooltip(): void {
        this.tooltip.setPosition(undefined);
    }
}

/** Computes a line length in meters in the projected map space used for display. */
export const measureLengthInMeters = (geometry: OlGeomLineString, projection: string): number => getLength(geometry, {projection});

/** Computes a polygon area in square meters in the projected map space used for display. */
export const measureAreaInSquareMeters = (geometry: OlGeomPolygon, projection: string): number => getArea(geometry, {projection});

const METERS_IN_KILOMETER = 1000;
const SQUARE_METERS_IN_SQUARE_KILOMETER = METERS_IN_KILOMETER * METERS_IN_KILOMETER;

/** Formats a line length in meters or kilometers for the measurement tooltip. */
export function formatLength(lengthInMeters: number, maxDecimalPlaces: number = 2): string {
    if (lengthInMeters >= METERS_IN_KILOMETER) {
        return `${(lengthInMeters / METERS_IN_KILOMETER).toFixed(maxDecimalPlaces)} km`;
    }

    return `${lengthInMeters.toFixed(0)} m`;
}

/** Formats a polygon area in square meters or square kilometers for the measurement tooltip. */
export function formatArea(areaInSquareMeters: number, maxDecimalPlaces: number = 2): string {
    if (areaInSquareMeters >= SQUARE_METERS_IN_SQUARE_KILOMETER) {
        return `${(areaInSquareMeters / SQUARE_METERS_IN_SQUARE_KILOMETER).toFixed(maxDecimalPlaces)} km²`;
    }

    return `${areaInSquareMeters.toFixed(0)} m²`;
}
